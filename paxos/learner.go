// Copyright © 2017 sosozhuang <sosozhuang@163.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package paxos

import (
	"errors"
	"github.com/gogo/protobuf/proto"
	"github.com/sosozhuang/paxos/checkpoint"
	"github.com/sosozhuang/paxos/comm"
	"github.com/sosozhuang/paxos/logger"
	"github.com/sosozhuang/paxos/store"
	"github.com/sosozhuang/paxos/util"
	"hash/crc32"
	"math"
	"sync"
	"sync/atomic"
	"time"
	"fmt"
)

var (
	llog = logger.GetLogger("learner")
)

type Learner interface {
	start(<-chan struct{})
	stop()
	getInstanceID() uint64
	setInstanceID(uint64)
	newInstance()
	onAskForLearn(*comm.PaxosMsg)
	onConfirmAskForLearn(*comm.PaxosMsg)
	onProposalFinished(*comm.PaxosMsg)
	onSendValue(*comm.PaxosMsg)
	onAckSendValue(*comm.PaxosMsg)
	onSendInstanceID(*comm.PaxosMsg)
	onAskForCheckpoint(*comm.PaxosMsg)
	proposalFinished(uint64, uint64)
	isLearned() bool
	isReadyForNewValue() bool
	getChecksum() uint32
	onSendCheckpoint(*comm.CheckpointMsg)
	onAckSendCheckpoint(*comm.CheckpointMsg)
	sendCheckpointBegin(uint64, uint64, uint64, uint64) error
	sendCheckpoint(uint64, uint64, uint64, uint64, uint32, string, uint32, int64, []byte) error
	sendCheckpointEnd(uint64, uint64, uint64, uint64) error
	sendValue(uint64, uint64, ballot, []byte, uint32, bool)
	getValue() []byte
	getLastSeenInstanceID() uint64
}

type learner struct {
	state              learnerState
	acceptor           Acceptor
	learning           bool
	instanceID         uint64
	lastAckInstanceID  uint64
	lastSeenInstanceID uint64
	lastSeenNodeID     uint64
	done               chan struct{}
	stopped            <-chan struct{}
	groupCfg           comm.GroupConfig
	instance           Instance
	cpm                checkpoint.CheckpointManager
	cps                Sender
	cpr                checkpoint.Receiver
	tp                 Transporter
	st                 store.Storage
	sender             sender
	token chan struct{}
}

func newLearner(groupCfg comm.GroupConfig, instance Instance, tp Transporter, st store.Storage,
	acceptor Acceptor, cpm checkpoint.CheckpointManager) Learner {
	learner := &learner{
		acceptor: acceptor,
		groupCfg: groupCfg,
		instance: instance,
		cpm:      cpm,
		tp:       tp,
		st:       st,
		done: make(chan struct{}),
		token: make(chan struct{}, 1),
	}
	learner.state = learnerState{
		st: st,
	}
	learner.sender = sender{
		Learner: learner,
		st:      st,
	}

	learner.cps = newCheckpointSender(learner, instance, cpm)
	learner.cpr = checkpoint.NewReceiver(st)
	return learner
}

func (l *learner) start(stopped <-chan struct{}) {
	l.stopped = stopped
	l.askForLearn(time.Second * 3)
	llog.Debugf("Learner of group %d started.", l.groupCfg.GetGroupID())
}

func (l *learner) stop() {
	close(l.done)
	l.sender.stop()
	l.cps.stop()
	llog.Debugf("Learner of group %d stopped.", l.groupCfg.GetGroupID())
}

func (l *learner) newInstance() {
	atomic.AddUint64(&l.instanceID, 1)
	l.state.reset()
}

func (l *learner) getInstanceID() uint64 {
	return atomic.LoadUint64(&l.instanceID)
	//return l.instanceID
}

func (l *learner) setInstanceID(id uint64) {
	l.instanceID = id
}

func (l *learner) getLastSeenInstanceID() uint64 {
	return l.lastSeenInstanceID
}

func (l *learner) setLastSeenInstanceID(instanceID, nodeID uint64) {
	if instanceID > l.lastSeenInstanceID {
		l.lastSeenInstanceID = instanceID
		l.lastSeenNodeID = nodeID
	}
}

func (l *learner) getValue() []byte {
	return l.state.value
}

func (l *learner) proposalFinished(instanceID, proposalID uint64) {
	msg := &comm.PaxosMsg{
		Type:       comm.PaxosMsgType_ProposalFinished.Enum(),
		InstanceID: proto.Uint64(instanceID),
		NodeID:     proto.Uint64(l.instance.getNodeID()),
		ProposalID: proto.Uint64(proposalID),
		Checksum:   proto.Uint32(l.instance.getChecksum()),
	}

	l.instance.ReceivePaxosMessage(msg)
	if err := l.tp.broadcast(comm.MsgType_Paxos, msg); err != nil {
		llog.Errorf("Learner broadcast message error: %v.", err)
	}
}

func (l *learner) onProposalFinished(msg *comm.PaxosMsg) {
	if l.getInstanceID() != msg.GetInstanceID() {
		llog.Warningf("Learner receive proposal finished message instance id %d, current instance id %d.", l.getInstanceID(), msg.GetInstanceID())
		return
	}
	//if !l.acceptor.getAcceptorState().acceptedBallot.valid() {
	//	llog.Warning("Learner check acceptor's accepted ballot invalid.")
	//	return
	//}
	//
	b := &ballot{msg.GetProposalID(), msg.GetNodeID()}
	value, checksum, err := l.acceptor.getAcceptorState(b)
	if err != nil {
		llog.Warningf("Learner receive proposal finished, validate acceptor state: %v.", err)
		return
	}
	//if l.acceptor.getAcceptorState().acceptedBallot.ne(b) {
	//	llog.Warning("Learner receive message ballot not equals to acceptor's accepted ballot.")
	//	return
	//}
	l.state.learn(value, checksum)
	l.broadcastToFollowers(b, value)
}

func (l *learner) broadcastToFollowers(b *ballot, value []byte) {
	msg := &comm.PaxosMsg{
		Type:           comm.PaxosMsgType_SendValue.Enum(),
		InstanceID:     proto.Uint64(l.getInstanceID()),
		NodeID:         proto.Uint64(l.instance.getNodeID()),
		ProposalNodeID: proto.Uint64(b.nodeID),
		ProposalID:     proto.Uint64(b.proposalID),
		Value:          value,
		Checksum:       proto.Uint32(l.instance.getChecksum()),
	}

	if err := l.tp.broadcastToFollowers(comm.MsgType_Paxos, msg); err != nil {
		llog.Errorf("Learner broadcast message to followers error: %v.", err)
	}
}

func (l *learner) sendValue(nodeID, instanceID uint64, b ballot, value []byte, checksum uint32, ack bool) {
	msg := &comm.PaxosMsg{
		Type:           comm.PaxosMsgType_SendValue.Enum(),
		InstanceID:     proto.Uint64(instanceID),
		NodeID:         proto.Uint64(l.instance.getNodeID()),
		ProposalID:     proto.Uint64(b.proposalID),
		ProposalNodeID: proto.Uint64(b.nodeID),
		Value:          value,
		Checksum:       proto.Uint32(checksum),
		AckFlag:  proto.Bool(ack),
	}

	if err := l.tp.send(nodeID, comm.MsgType_Paxos, msg); err != nil {
		llog.Errorf("Learner send message error: %v.", err)
	}
}

func (l *learner) onSendValue(msg *comm.PaxosMsg) {
	if msg.GetInstanceID() > l.getInstanceID() {
		llog.Warningf("Learner can't learn send value message, instance id %d, current instance id %d.",
			msg.GetInstanceID(), l.getInstanceID())
		return
	}
	if msg.GetInstanceID() < l.getInstanceID() {
		llog.Warningf("Learner no need to learn send value message, instance id %d, current instance id %d.",
			msg.GetInstanceID(), l.getInstanceID())
	} else {
		b := ballot{msg.GetProposalID(), msg.GetProposalNodeID()}
		if err := l.state.learnAndSave(msg.GetInstanceID(), b, msg.GetValue(), 0); err != nil {
			llog.Errorf("Learner save state error: %v.", err)
			return
		}
	}
	if msg.GetAckFlag() {
		l.askForLearn(time.Second * 3)
		l.ackSendValue(msg.GetNodeID())
	}
}

func (l *learner) ackSendValue(nodeID uint64) {
	if l.getInstanceID() < l.lastAckInstanceID+25 {
		return
	}
	l.lastAckInstanceID = l.getInstanceID()

	msg := &comm.PaxosMsg{
		Type:       comm.PaxosMsgType_AckSendValue.Enum(),
		InstanceID: proto.Uint64(l.getInstanceID()),
		NodeID:     proto.Uint64(l.instance.getNodeID()),
	}
	if err := l.tp.send(nodeID, comm.MsgType_Paxos, msg); err != nil {
		llog.Errorf("Learner send message error: %v.", err)
	}
}

func (l *learner) onAckSendValue(msg *comm.PaxosMsg) {
	l.sender.ack(msg.GetInstanceID(), msg.GetNodeID())
}

func (l *learner) askForLearn(d time.Duration) {
	l.token <- struct{}{}
	defer func() {
		<- l.token
	}()
	close(l.done)
	go func() {
		ticker := time.NewTicker(d)
		defer ticker.Stop()
		l.done = make(chan struct{})
		for {
			select {
			case <-l.done:
				return
			case <-l.stopped:
				return
			case <-ticker.C:
				go l.doAskForLearn()
			}
		}
	}()
}

func (l *learner) doAskForLearn() {
	l.learning = false
	l.cpm.ExitAskForCheckpoint()
	msg := &comm.PaxosMsg{
		Type:       comm.PaxosMsgType_AskForLearn.Enum(),
		NodeID:     proto.Uint64(l.instance.getNodeID()),
		InstanceID: proto.Uint64(l.getInstanceID()),
	}
	if l.groupCfg.FollowerMode() {
		msg.ProposalNodeID = proto.Uint64(l.groupCfg.GetFollowNodeID())
	}
	if err := l.tp.broadcast(comm.MsgType_Paxos, msg); err != nil {
		llog.Errorf("Learner broadcast message error: %v.", err)
	}
	if err := l.tp.broadcastToLearnNodes(comm.MsgType_Paxos, msg); err != nil {
		llog.Errorf("Learner broadcast message to learn nodes error: %v.", err)
	}
}

func (l *learner) onAskForLearn(msg *comm.PaxosMsg) {
	l.setLastSeenInstanceID(msg.GetInstanceID(), msg.GetNodeID())
	if msg.GetProposalNodeID() == l.instance.getNodeID() {
		l.groupCfg.AddFollower(msg.GetNodeID())
	}
	if msg.GetInstanceID() >= l.getInstanceID() {
		return
	}
	if msg.GetInstanceID() >= l.cpm.GetMinChosenInstanceID() {
		if !l.sender.prepare(msg.GetInstanceID(), msg.GetNodeID()) {
			if msg.GetInstanceID() == l.getInstanceID()-1 {
				value, err := l.st.Get(msg.GetInstanceID())
				if err != nil {
					llog.Errorf("Learner get instance id %d value error: %v.", msg.GetInstanceID(), err)
					return
				}
				var state comm.AcceptorStateData
				if err = proto.Unmarshal(value, &state); err != nil {
					llog.Errorf("Learner unmarshal instance id %d value error: %v.", msg.GetInstanceID(), err)
					return
				}
				b := ballot{state.GetAcceptedID(), state.GetAcceptedNodeID()}
				l.sendValue(msg.GetNodeID(), msg.GetInstanceID(), b, state.GetAcceptedValue(), 0, false)
			}
			return
		}
	}
	l.sendInstanceID(msg.GetInstanceID(), msg.GetNodeID())
}

func (l *learner) confirmAskForLearn(nodeID uint64) {
	msg := &comm.PaxosMsg{
		Type:       comm.PaxosMsgType_ConfirmAskForLearn.Enum(),
		InstanceID: proto.Uint64(l.getInstanceID()),
		NodeID:     proto.Uint64(l.instance.getNodeID()),
	}
	if err := l.tp.send(nodeID, comm.MsgType_Paxos, msg); err != nil {
		llog.Errorf("Learner send message error: %v.", err)
	}
	l.learning = true
}

func (l *learner) onConfirmAskForLearn(msg *comm.PaxosMsg) {
	if !l.sender.confirm(msg.GetInstanceID(), msg.GetNodeID()) {
		llog.Error("Learner confirm message failed.")
	}
}

func (l *learner) sendInstanceID(instanceID, nodeID uint64) {
	msg := &comm.PaxosMsg{
		Type:                comm.PaxosMsgType_SendInstanceID.Enum(),
		InstanceID:          proto.Uint64(instanceID),
		NodeID:              proto.Uint64(l.instance.getNodeID()),
		CurInstanceID:       proto.Uint64(l.getInstanceID()),
		MinChosenInstanceID: proto.Uint64(l.cpm.GetMinChosenInstanceID()),
	}
	if l.getInstanceID()-instanceID > 50 {
		cp, err := l.groupCfg.GetSystemCheckpoint()
		if err == nil {
			msg.SystemVar = cp

		}
		cp, err = l.groupCfg.GetMasterCheckpoint()
		if err == nil {
			msg.MasterVar = cp
		}
	}
	if err := l.tp.send(nodeID, comm.MsgType_Paxos, msg); err != nil {
		llog.Errorf("Learner send message error: %v.", err)
	}
}

func (l *learner) onSendInstanceID(msg *comm.PaxosMsg) {
	l.setLastSeenInstanceID(msg.GetCurInstanceID(), msg.GetNodeID())

	if len(msg.GetSystemVar()) > 0 {
		l.groupCfg.UpdateSystemByCheckpoint(msg.GetSystemVar())
	}
	if len(msg.GetMasterVar()) > 0 {
		l.groupCfg.UpdateMasterByCheckpoint(msg.GetMasterVar())
	}

	if msg.GetInstanceID() != l.getInstanceID() {
		llog.Debugf("Learner receive instance lagging behind.")
		return
	}
	if msg.GetCurInstanceID() <= l.getInstanceID() {
		llog.Debugf("Learner receive instance lagging behind.")
		return
	}
	if msg.GetMinChosenInstanceID() > l.getInstanceID() {
		l.askForCheckpoint(msg.GetNodeID())
	} else if !l.learning {
		l.confirmAskForLearn(msg.GetNodeID())
	}
}

func (l *learner) isLearned() bool {
	return l.state.learned
}

func (l *learner) isReadyForNewValue() bool {
	return l.getInstanceID()+1 >= l.lastSeenInstanceID
}

func (l *learner) getChecksum() uint32 {
	return l.state.checksum
}

func (l *learner) askForCheckpoint(nodeID uint64) {
	if err := l.cpm.PrepareAskForCheckpoint(nodeID); err != nil {
		llog.Errorf("Learner prepare ask for checkpoint error: %v.", err)
		return
	}
	msg := &comm.PaxosMsg{
		Type:       comm.PaxosMsgType_AskForCheckpoint.Enum(),
		NodeID:     proto.Uint64(l.instance.getNodeID()),
		InstanceID: proto.Uint64(l.getInstanceID()),
	}
	if err := l.tp.send(nodeID, comm.MsgType_Paxos, msg); err != nil {
		llog.Errorf("Learner send message error: %v.", err)
	}
}

func (l *learner) onAskForCheckpoint(msg *comm.PaxosMsg) {
	if l.cps.isFinished() {
		l.cps.start(msg.GetNodeID())
	} else {
		llog.Error("Learner's checkpoint sender is running, can't send for this.")
	}
}

func (l *learner) sendCheckpointBegin(nodeID, uuid, sequence, checkpointInstanceID uint64) error {
	msg := &comm.CheckpointMsg{
		Type:                 comm.CheckpointMsgType_SendFile.Enum(),
		NodeID:               proto.Uint64(l.instance.getNodeID()),
		Flag:                 comm.CheckPointMsgFlag_Begin.Enum(),
		UUID:                 proto.Uint64(uuid),
		Sequence:             proto.Uint64(sequence),
		CheckpointInstanceID: proto.Uint64(checkpointInstanceID),
	}
	return l.tp.send(nodeID, comm.MsgType_Checkpoint, msg)
}

func (l *learner) sendCheckpointEnd(nodeID, uuid, sequence, checkpointInstanceID uint64) error {
	msg := &comm.CheckpointMsg{
		Type:                 comm.CheckpointMsgType_SendFile.Enum(),
		NodeID:               proto.Uint64(l.instance.getNodeID()),
		Flag:                 comm.CheckPointMsgFlag_End.Enum(),
		UUID:                 proto.Uint64(uuid),
		Sequence:             proto.Uint64(sequence),
		CheckpointInstanceID: proto.Uint64(checkpointInstanceID),
	}
	return l.tp.send(nodeID, comm.MsgType_Checkpoint, msg)
}

func (l *learner) sendCheckpoint(nodeID, uuid, sequence, checkpointInstanceID uint64,
	checksum uint32, path string, smid uint32, offset int64, b []byte) error {
	msg := &comm.CheckpointMsg{
		Type:                 comm.CheckpointMsgType_SendFile.Enum(),
		NodeID:               proto.Uint64(l.instance.getNodeID()),
		Flag:                 comm.CheckPointMsgFlag_Progressing.Enum(),
		UUID:                 proto.Uint64(uuid),
		Sequence:             proto.Uint64(sequence),
		CheckpointInstanceID: proto.Uint64(checkpointInstanceID),
		Checksum:             proto.Uint32(checksum),
		FilePath:             proto.String(path),
		SMID:                 proto.Uint32(smid),
		Offset:               proto.Int64(offset),
		Bytes:               b,
	}
	return l.tp.send(nodeID, comm.MsgType_Checkpoint, msg)
}

func (l *learner) onSendCheckpointBegin(msg *comm.CheckpointMsg) error {
	if err := l.cpr.Prepare(msg.GetNodeID(), msg.GetUUID()); err != nil {
		return err
	}
	return l.cpm.SaveMinChosenInstanceID(msg.GetCheckpointInstanceID())
}

func (l *learner) onSendCheckpointProgressing(msg *comm.CheckpointMsg) error {
	return l.cpr.Receive(msg)
}

func (l *learner) onSendCheckpointEnd(msg *comm.CheckpointMsg) error {
	if !l.cpr.IsFinished(msg.GetNodeID(), msg.GetUUID(), msg.GetSequence()) {
		return errors.New("checkpoint receiver not finished")
	}
	// todo: LoadCheckpointState for state machine
	fmt.Println("util.ExitPaxos(-1)")
	util.ExitPaxos(-1)
	return nil
}

func (l *learner) onSendCheckpoint(msg *comm.CheckpointMsg) {
	var err error
	switch msg.GetFlag() {
	case comm.CheckPointMsgFlag_Begin:
		err = l.onSendCheckpointBegin(msg)
	case comm.CheckPointMsgFlag_Progressing:
		err = l.onSendCheckpointProgressing(msg)
	case comm.CheckPointMsgFlag_End:
		err = l.onSendCheckpointEnd(msg)
	}
	if err != nil {
		llog.Errorf("Learner on send checkpoint error: %v.", err)
		l.cpr.Reset()
		l.askForLearn(time.Second * 5)
		l.ackSendCheckpoint(msg.GetNodeID(), msg.GetUUID(), msg.GetSequence(), comm.CheckPointMsgFlag_Failed)
	} else {
		l.ackSendCheckpoint(msg.GetNodeID(), msg.GetUUID(), msg.GetSequence(), comm.CheckPointMsgFlag_Successful)
		l.askForLearn(time.Minute * 2)
	}
}

func (l *learner) ackSendCheckpoint(nodeID, uuid, sequence uint64, flag comm.CheckPointMsgFlag) {
	msg := &comm.CheckpointMsg{
		Type:     comm.CheckpointMsgType_AckSendFile.Enum(),
		NodeID:   proto.Uint64(l.instance.getNodeID()),
		UUID:     proto.Uint64(uuid),
		Sequence: proto.Uint64(sequence),
		Flag:     flag.Enum(),
	}
	if err := l.tp.send(nodeID, comm.MsgType_Checkpoint, msg); err != nil {
		llog.Errorf("Learner send message error: %v.", err)
	}
}

func (l *learner) onAckSendCheckpoint(msg *comm.CheckpointMsg) {
	if !l.cps.isFinished() {
		if msg.GetFlag() == comm.CheckPointMsgFlag_Successful {
			l.cps.ack(msg.GetNodeID(), msg.GetUUID(), msg.GetSequence())
		} else {
			l.cps.stop()
		}
	}
}

type learnerState struct {
	checksum uint32
	learned  bool
	value    []byte
	st       store.Storage
}

func (l *learnerState) reset() {
	l.learned = false
	l.value = nil
	l.checksum = 0
}

func (l *learnerState) learn(value []byte, checksum uint32) {
	l.value = value
	l.learned = true
	l.checksum = checksum
}

func (l *learnerState) learnAndSave(instanceID uint64, b ballot, value []byte, checksum uint32) error {
	if instanceID > 0 && checksum == 0 {
		l.checksum = 0
	} else if len(l.value) > 0 {
		l.checksum = crc32.Update(checksum, crcTable, l.value)
	}
	state := &comm.AcceptorStateData{
		InstanceID:     proto.Uint64(instanceID),
		AcceptedValue:  l.value,
		PromisedID:     proto.Uint64(b.proposalID),
		PromisedNodeID: proto.Uint64(b.nodeID),
		AcceptedID:     proto.Uint64(b.proposalID),
		AcceptedNodeID: proto.Uint64(b.nodeID),
		Checksum:       proto.Uint32(l.checksum),
	}
	v, err := proto.Marshal(state)
	if err != nil {
		return fmt.Errorf("marshal state: %v", err)
	}
	if err := l.st.Set(instanceID, v); err != nil {
		return err
	}

	l.learn(value, l.checksum)
	return nil
}

type sender struct {
	Learner
	prepared        bool
	confirmed       bool
	lastSendTime    time.Time
	lastAckTime     time.Time
	nodeID          uint64
	startInstanceID uint64
	ackInstanceID   uint64
	wg              sync.WaitGroup
	mu              sync.Mutex
	st              store.Storage
}

func (s *sender) isPrepared() bool {
	if !s.prepared {
		return false
	}

	if time.Now().Sub(s.lastSendTime) >= time.Second*5 {
		return false
	}

	return true
}

func (s *sender) start() {
	defer s.wg.Done()
	s.send(s.startInstanceID, s.nodeID)

	s.mu.Lock()
	defer s.mu.Unlock()
	s.prepared = false
	s.confirmed = false
	s.startInstanceID = math.MaxUint64
	s.lastSendTime = time.Time{}
	s.lastAckTime = time.Time{}
	s.ackInstanceID = 0

}

func (s *sender) stop() {
	s.wg.Wait()
}

func (s *sender) checkAck(instanceID uint64) bool {
	if instanceID < s.ackInstanceID {
		return false
	}
	for instanceID > s.ackInstanceID+51 {
		if time.Now().Sub(s.lastAckTime) >= time.Second*5 {
			return false
		}
		time.Sleep(time.Millisecond * 10)
	}
	return true
}

func (s *sender) ack(ackInstanceID, nodeID uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.isPrepared() && s.confirmed {
		if s.nodeID == nodeID {
			if ackInstanceID > s.ackInstanceID {
				s.ackInstanceID = ackInstanceID
				s.lastAckTime = time.Now()
			}
		}
	}
}

func (s *sender) prepare(instanceID, nodeID uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.isPrepared() && !s.confirmed {
		s.prepared = true
		s.lastSendTime = time.Now()
		s.startInstanceID = instanceID
		s.ackInstanceID = instanceID
		s.nodeID = nodeID
		return true
	}
	return false
}

func (s *sender) confirm(instanceID, nodeID uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.isPrepared() && !s.confirmed {
		if s.startInstanceID == instanceID && s.nodeID == nodeID {
			s.confirmed = true
			s.wg.Add(1)
			go s.start()
			return true
		}
	}
	return false
}

func (s *sender) send(instanceID uint64, nodeID uint64) {
	var (
		cs       uint32
		err      error
		sleep    time.Duration
		interval int
	)
	qps := 100
	if qps > 1000 {
		sleep = 1
		interval = qps/1000 + 1
	} else {
		sleep = time.Duration(1000 / qps)
		interval = 1
	}

	for count, id := 0, instanceID; id < s.Learner.getInstanceID(); id++ {
		if cs, err = s.sendValue(id, nodeID, cs); err != nil {
			llog.Errorf("Learner sender send value error: %v.", err)
			return
		}
		llog.Debugf("Learner sender send instance id %d to node id %d.", id, nodeID)
		s.lastSendTime = time.Now()
		if !s.checkAck(id) {
			break
		}
		count++
		if count >= interval {
			count = 0
			time.Sleep(time.Millisecond * sleep)
		}
	}
}

func (s *sender) sendValue(instanceID, nodeID uint64, checksum uint32) (uint32, error) {
	value, err := s.st.Get(instanceID)
	if err != nil {
		return checksum, err
	}
	var state comm.AcceptorStateData
	if err = proto.Unmarshal(value, &state); err != nil {
		return checksum, err
	}
	b := ballot{state.GetAcceptedID(), state.GetAcceptedNodeID()}
	s.Learner.sendValue(nodeID, instanceID, b, state.GetAcceptedValue(), checksum, true)

	return checksum, nil
}
