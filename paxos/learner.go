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
package paxos1

import (
	"errors"
	"github.com/gogo/protobuf/proto"
	"github.com/sosozhuang/paxos/checkpoint"
	"github.com/sosozhuang/paxos/comm"
	"github.com/sosozhuang/paxos/logger"
	"github.com/sosozhuang/paxos/store"
	"hash/crc32"
	"math"
	"sync"
	"time"
	"github.com/sosozhuang/paxos/util"
)

var (
	llog = logger.LearnerLogger
)

type Learner interface {
	start()
	stop()
	setStopped(<-chan struct{})
	setInstanceID(comm.InstanceID)
	newInstance()
	AskForLearn(time.Duration)
	onAskForLearn(*comm.PaxosMsg)
	onConfirmAskForLearn(*comm.PaxosMsg)
	onProposalFinished(*comm.PaxosMsg)
	onSendValue(*comm.PaxosMsg)
	onAckSendValue(*comm.PaxosMsg)
	onSendInstanceID(*comm.PaxosMsg)
	onAskForCheckpoint(*comm.PaxosMsg)
	proposalFinished(comm.InstanceID, proposalID)
	isLearned() bool
	isReadyForNewValue() bool
	getChecksum() comm.Checksum
	onSendCheckpoint(*comm.CheckpointMsg)
	onAckSendCheckpoint(*comm.CheckpointMsg)
	SendCheckpointBegin(comm.NodeID, uint64, uint64, comm.InstanceID)
	SendCheckpointEnd(comm.NodeID, uint64, uint64, comm.InstanceID)
	sendValue(comm.NodeID, comm.InstanceID, ballot, []byte, comm.Checksum, bool)
	getInstanceID() comm.InstanceID
	getValue() []byte
	getLastSeenInstanceID() comm.InstanceID
}

type learner struct {
	state              learnerState
	acceptor           Acceptor
	learning           bool
	nodeID             comm.NodeID
	instanceID         comm.InstanceID
	lastAckInstanceID  comm.InstanceID
	lastSeenInstanceID comm.InstanceID
	lastSeenNodeID     comm.NodeID
	done               chan struct{}
	stopped <-chan struct{}
	group comm.Group
	instance           comm.Instance
	cpm                checkpoint.CheckpointManager
	cps                checkpoint.Sender
	cpr                *checkpoint.Receiver
	tp                 Transporter
	sender
}

func newLearner(nodeID comm.NodeID, instance comm.Instance, tp Transporter, st store.Storage, acceptor Acceptor, cpm checkpoint.CheckpointManager) (Learner, error) {
	learner := &learner{
		nodeID: nodeID,
		acceptor: acceptor,
		instance: instance,
		cpm: cpm,
		tp: tp,
		done: make(chan struct{}),
	}
	learner.sender = sender{
		Learner: learner,
		st: st,
	}

	learner.cpr = &checkpoint.Receiver{Storage: st}
	return learner, nil
}

func (l *learner) setStopped(stopped <-chan struct{}) {
	l.stopped = stopped
}

func (l *learner) start() {
	//l.sender.start()
}

func (l *learner) stop() {
	l.sender.stop()
	if l.cps != nil {
		l.cps.Stop()
	}
}

func (l *learner) newInstance() {
	//atomic.AddUint64(&l.instanceID, 1)
	l.state.reset()
}

func (l *learner) setInstanceID(id comm.InstanceID) {
	l.instanceID = id
}

func (l *learner) getLastSeenInstanceID() comm.InstanceID {
	return l.lastSeenInstanceID
}

func (l *learner) setLastSeenInstanceID(instanceID comm.InstanceID, nodeID comm.NodeID) {
	if instanceID > l.lastSeenInstanceID {
		l.lastSeenInstanceID = instanceID
		l.lastSeenNodeID = nodeID
	}
}

func (l *learner) getValue() []byte {
	return l.state.value
}

func (l *learner) proposalFinished(instanceID comm.InstanceID, proposalID proposalID) {
	msg := &comm.PaxosMsg{
		Type:       comm.PaxosMsgType_ProposalFinished.Enum(),
		InstanceID: proto.Uint64(uint64(instanceID)),
		NodeID:     proto.Uint64(uint64(l.nodeID)),
		ProposalID: proto.Uint64(uint64(proposalID)),
		Checksum:   proto.Uint32(uint32(l.instance.GetChecksum())),
	}

	l.instance.ReceivePaxosMessage(msg)
	l.tp.broadcast(comm.MsgType_Paxos, msg)
}

func (l *learner) onProposalFinished(msg *comm.PaxosMsg) {
	if l.instanceID != comm.InstanceID(msg.GetInstanceID()) {
		return
	}
	if !l.acceptor.getAcceptorState().acceptedBallot.valid() {
		return
	}

	b := ballot{proposalID(msg.GetProposalID()), comm.NodeID(msg.GetProposalNodeID())}
	if l.acceptor.getAcceptorState().acceptedBallot.ne(b) {
		return
	}

	l.state.learn(l.acceptor.getAcceptorState().acceptedValue, l.acceptor.getAcceptorState().checksum)
	l.broadcastToFollowers()
}

func (l *learner) broadcastToFollowers() {
	msg := &comm.PaxosMsg{
		Type:           comm.PaxosMsgType_SendValue.Enum(),
		InstanceID:     proto.Uint64(uint64(l.instanceID)),
		NodeID:         proto.Uint64(uint64(l.nodeID)),
		ProposalNodeID: proto.Uint64(uint64(l.acceptor.getAcceptorState().acceptedBallot.nodeID)),
		ProposalID:     proto.Uint64(uint64(l.acceptor.getAcceptorState().acceptedBallot.proposalID)),
		Value:          l.acceptor.getAcceptorState().acceptedValue,
		Checksum:       proto.Uint32(uint32(l.instance.GetChecksum())),
	}

	l.tp.broadcastToFollowers(comm.MsgType_Paxos, msg)
}

func (l *learner) sendValue(nodeID comm.NodeID, instanceID comm.InstanceID, b ballot, value []byte, cs comm.Checksum, ack bool) {
	msg := &comm.PaxosMsg{
		Type:           comm.PaxosMsgType_SendValue.Enum(),
		InstanceID:     proto.Uint64(uint64(instanceID)),
		NodeID:         proto.Uint64(uint64(l.nodeID)),
		ProposalID:     proto.Uint64(uint64(b.proposalID)),
		ProposalNodeID: proto.Uint64(uint64(b.nodeID)),
		Value:          value,
		Checksum:       proto.Uint32(uint32(cs)),
	}
	if ack {
		msg.AckFlag = proto.Bool(ack)
	}
	l.tp.send(nodeID, comm.MsgType_Paxos, msg)
}

func (l *learner) onSendValue(msg *comm.PaxosMsg) {
	if comm.InstanceID(msg.GetInstanceID()) != l.instanceID {
		return
	}
	b := ballot{proposalID(msg.GetProposalID()), comm.NodeID(msg.GetProposalNodeID())}
	if err := l.state.learnAndSave(msg.GetInstanceID(), b, msg.GetValue(), 0); err != nil {
		llog.Error(err)
		return
	}
	if msg.GetAckFlag() {
		l.AskForLearn(time.Second * 3)
		l.ackSendValue(comm.NodeID(msg.GetNodeID()))
	}
}

func (l *learner) ackSendValue(nodeID comm.NodeID) {
	if l.instanceID < l.lastAckInstanceID+25 {
		return
	}
	l.lastAckInstanceID = l.instanceID

	msg := &comm.PaxosMsg{
		Type:       comm.PaxosMsgType_AckSendValue.Enum(),
		InstanceID: proto.Uint64(uint64(l.instanceID)),
		NodeID:     proto.Uint64(uint64(l.nodeID)),
	}
	l.tp.send(nodeID, comm.MsgType_Paxos, msg)
}

func (l *learner) onAckSendValue(msg *comm.PaxosMsg) {
	l.sender.ack(comm.InstanceID(msg.GetInstanceID()), comm.NodeID(msg.GetNodeID()))
}

func (l *learner) AskForLearn(d time.Duration) {
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
				go l.askForLearn()
			}
		}
	}()
}

func (l *learner) askForLearn() {
	l.cpm.ExitAskForCheckpoint()
	msg := &comm.PaxosMsg{
		Type:       comm.PaxosMsgType_AskForLearn.Enum(),
		NodeID:     proto.Uint64(uint64(l.nodeID)),
		InstanceID: proto.Uint64(uint64(l.instanceID)),
	}
	// todo: check if in follower mode
	if l.group.FollowerMode() {
		msg.ProposalNodeID = proto.Uint64(uint64(l.group.FollowNodeID()))
	}
	l.tp.broadcast(comm.MsgType_Paxos, msg)
	l.tp.broadcastToLearnNodes(comm.MsgType_Paxos, msg)
}

func (l *learner) onAskForLearn(msg *comm.PaxosMsg) {
	l.setLastSeenInstanceID(comm.InstanceID(msg.GetInstanceID()), comm.NodeID(msg.GetNodeID()))
	if comm.NodeID(msg.GetProposalNodeID()) == l.nodeID {
		// todo: add follower node
		l.group.AddFollower(comm.NodeID(msg.GetNodeID()))
	}
	if comm.InstanceID(msg.GetInstanceID()) >= l.instanceID {
		return
	}
	if comm.InstanceID(msg.GetInstanceID()) >= l.cpm.GetMinChosenInstanceID() {
		if !l.sender.prepare(comm.InstanceID(msg.GetInstanceID()), comm.NodeID(msg.GetNodeID())) {
			if comm.InstanceID(msg.GetInstanceID()) == l.instanceID-1 {
				value, err := l.st.Get(comm.InstanceID(msg.GetInstanceID()))
				if err != nil {
					log.Error(err)
					return
				}
				var state comm.AcceptorStateData
				if err = proto.Unmarshal(value, &state); err != nil {
					log.Error(err)
					return
				}
				b := ballot{proposalID(state.GetAcceptedID()), comm.NodeID(state.GetAcceptedNodeID())}
				l.sendValue(comm.NodeID(msg.GetNodeID()), comm.InstanceID(msg.GetInstanceID()), b, state.GetAcceptedValue(), 0, false)
			}
			return
		}
	}
	l.sendInstanceID(comm.InstanceID(msg.GetInstanceID()), comm.NodeID(msg.GetNodeID()))
}

func (l *learner) confirmAskForLearn(nodeID comm.NodeID) {
	msg := &comm.PaxosMsg{
		Type:       comm.PaxosMsgType_ConfirmAskForLearn.Enum(),
		InstanceID: proto.Uint64(uint64(l.instanceID)),
		NodeID:     proto.Uint64(uint64(l.nodeID)),
	}
	l.tp.send(nodeID, comm.MsgType_Paxos, msg)
	l.learning = true
}

func (l *learner) onConfirmAskForLearn(msg *comm.PaxosMsg) {
	if !l.sender.confirm(comm.InstanceID(msg.GetInstanceID()), comm.NodeID(msg.GetNodeID())) {
		llog.Error("confirm failed.")
	}
}

func (l *learner) sendInstanceID(instanceID comm.InstanceID, nodeID comm.NodeID) {
	msg := &comm.PaxosMsg{
		Type:                comm.PaxosMsgType_SendInstanceID.Enum(),
		InstanceID:          proto.Uint64(uint64(instanceID)),
		NodeID:              proto.Uint64(uint64(l.nodeID)),
		CurInstanceID:       proto.Uint64(uint64(l.instanceID)),
		MinChosenInstanceID: proto.Uint64(uint64(l.cpm.GetMinChosenInstanceID())),
	}
	if l.instanceID-instanceID > 50 {
		// todo: add systemvar mastervar bytes
		msg.SystemVariables = nil
		msg.MasterVariables = nil
	}
	l.tp.send(nodeID, comm.MsgType_Paxos, msg)
}

func (l *learner) onSendInstanceID(msg *comm.PaxosMsg) {
	l.setLastSeenInstanceID(comm.InstanceID(msg.GetCurInstanceID()), comm.NodeID(msg.GetNodeID()))

	// todo: check systemvar and mastervar

	if comm.InstanceID(msg.GetInstanceID()) != l.instanceID {
		return
	}
	if comm.InstanceID(msg.GetCurInstanceID()) <= l.instanceID {
		return
	}
	if comm.InstanceID(msg.GetMinChosenInstanceID()) > l.instanceID {
		l.askForCheckpoint(comm.NodeID(msg.GetNodeID()))
	} else if !l.learning {
		l.confirmAskForLearn(comm.NodeID(msg.GetNodeID()))
	}
}

func (l *learner) isLearned() bool {
	return l.state.learned
}

func (l *learner) isReadyForNewValue() bool {
	return l.instanceID+1 >= l.lastSeenInstanceID
}

func (l *learner) getChecksum() comm.Checksum {
	return l.state.checksum
}

func (l *learner) askForCheckpoint(nodeID comm.NodeID) {
	if err := l.cpm.PrepareAskForCheckpoint(nodeID); err != nil {
		log.Error(err)
		return
	}
	msg := &comm.PaxosMsg{
		Type:       comm.PaxosMsgType_AskForCheckpoint.Enum(),
		NodeID:     proto.Uint64(uint64(l.nodeID)),
		InstanceID: proto.Uint64(uint64(l.instanceID)),
	}
	l.tp.send(nodeID, comm.MsgType_Paxos, msg)
}

func (l *learner) onAskForCheckpoint(msg *comm.PaxosMsg) {
	if l.cps == nil {
		l.cps = checkpoint.NewSender(l, l.cpm)
		l.cps.Start()
	} else if l.cps.IsFinished() {
		l.cps.Start()
	} else {
		log.Error("Checkpoint sender is running.")
		return
	}
}

func (l *learner) SendCheckpointBegin(nodeID comm.NodeID, uuid uint64, sequence uint64, checkpointInstanceID comm.InstanceID) {
	msg := &comm.CheckpointMsg{
		Type:                 comm.CheckpointMsgType_SendFile.Enum(),
		NodeID:               proto.Uint64(uint64(l.nodeID)),
		Flag:                 comm.CheckPointMsgFlag_Begin.Enum(),
		UUID:                 proto.Uint64(uint64(uuid)),
		Sequence:             proto.Uint64(uint64(sequence)),
		CheckpointInstanceID: proto.Uint64(uint64(checkpointInstanceID)),
	}
	l.tp.send(nodeID, comm.MsgType_Checkpoint, msg)
}

func (l *learner) SendCheckpointEnd(nodeID comm.NodeID, uuid uint64, sequence uint64, checkpointInstanceID comm.InstanceID) {
	msg := &comm.CheckpointMsg{
		Type:                 comm.CheckpointMsgType_SendFile.Enum(),
		NodeID:               proto.Uint64(uint64(l.nodeID)),
		Flag:                 comm.CheckPointMsgFlag_End.Enum(),
		UUID:                 proto.Uint64(uint64(uuid)),
		Sequence:             proto.Uint64(uint64(sequence)),
		CheckpointInstanceID: proto.Uint64(uint64(checkpointInstanceID)),
	}
	l.tp.send(nodeID, comm.MsgType_Checkpoint, msg)
}

func (l *learner) sendCheckpoint(nodeID comm.NodeID, uuid uint64, sequence uint64, checkpointInstanceID comm.InstanceID,
	cs comm.Checksum, path string, smid comm.StateMachineID, offset int64, b []byte) {
	msg := &comm.CheckpointMsg{
		Type:                 comm.CheckpointMsgType_SendFile.Enum(),
		NodeID:               proto.Uint64(uint64(l.nodeID)),
		Flag:                 comm.CheckPointMsgFlag_Progressing.Enum(),
		UUID:                 proto.Uint64(uint64(uuid)),
		Sequence:             proto.Uint64(uint64(sequence)),
		CheckpointInstanceID: proto.Uint64(uint64(checkpointInstanceID)),
		Checksum:             proto.Uint32(uint32(cs)),
		FilePath:             proto.String(path),
		SMID:                 proto.Int32(int32(smid)),
		Offset:               proto.Int64(offset),
		Buffer:               b,
	}
	l.tp.send(nodeID, comm.MsgType_Checkpoint, msg)
}

func (l *learner) onSendCheckpointBegin(msg *comm.CheckpointMsg) error {
	if err := l.cpr.Prepare(comm.NodeID(msg.GetNodeID()), msg.GetUUID()); err != nil {
		return err
	}
	return l.cpm.SaveMinChosenInstanceID(comm.InstanceID(msg.GetCheckpointInstanceID()))
}

func (l *learner) onSendCheckpointProgressing(msg *comm.CheckpointMsg) error {
	return l.cpr.Receive(msg)
}

func (l *learner) onSendCheckpointEnd(msg *comm.CheckpointMsg) error {
	if l.cpr.IsFinished(comm.NodeID(msg.GetNodeID()), msg.GetUUID(), msg.GetSequence()) {
		return errors.New("")
	}
	// todo: LoadCheckpointState for state machine
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
		l.cpr.Reset()
		l.AskForLearn(time.Second * 5)
		l.ackSendCheckpoint(comm.NodeID(msg.GetNodeID()), msg.GetUUID(), msg.GetSequence(), comm.CheckPointMsgFlag_Failed)
	} else {
		l.ackSendCheckpoint(comm.NodeID(msg.GetNodeID()), msg.GetUUID(), msg.GetSequence(), comm.CheckPointMsgFlag_Successful)
		l.AskForLearn(time.Minute * 2)
	}
}

func (l *learner) ackSendCheckpoint(nodeID comm.NodeID, uuid, sequence uint64, flag comm.CheckPointMsgFlag) {
	msg := &comm.CheckpointMsg{
		Type:     comm.CheckpointMsgType_AckSendFile.Enum(),
		NodeID:   proto.Uint64(uint64(l.nodeID)),
		UUID:     proto.Uint64(uuid),
		Sequence: proto.Uint64(sequence),
		Flag:     flag.Enum(),
	}
	l.tp.send(nodeID, comm.MsgType_Checkpoint, msg)
}

func (l *learner) onAckSendCheckpoint(msg *comm.CheckpointMsg) {
	if l.cps != nil && !l.cps.IsFinished() {
		if msg.GetFlag() == comm.CheckPointMsgFlag_Successful {
			l.cps.Ack(comm.NodeID(msg.GetNodeID()), msg.GetUUID(), msg.GetSequence())
		} else {
			l.cps.Stop()
		}
	}
}

type learnerState struct {
	checksum comm.Checksum
	learned bool
	value   []byte
	st      store.Storage
}

func (l *learnerState) reset() {
	l.learned = false
	l.value = nil
	l.checksum = comm.Checksum(0)
}

func (l *learnerState) learn(value []byte, cs comm.Checksum) {
	l.value = value
	l.learned = true
	l.checksum = cs
}

func (l *learnerState) learnAndSave(instanceID uint64, b ballot, value []byte, cs uint32) error {
	if instanceID > 0 && cs == 0 {
		l.checksum = 0
	} else if l.value != nil && len(l.value) > 0 {
		l.checksum = comm.Checksum(crc32.Update(cs, crcTable, l.value))
	}
	state := &comm.AcceptorStateData{
		InstanceID:     proto.Uint64(instanceID),
		AcceptedValue:  l.value,
		PromisedID:     proto.Uint64(uint64(b.proposalID)),
		PromisedNodeID: proto.Uint64(uint64(b.nodeID)),
		AcceptedID:     proto.Uint64(uint64(b.proposalID)),
		AcceptedNodeID: proto.Uint64(uint64(b.nodeID)),
		Checksum:       proto.Uint32(uint32(l.checksum)),
	}
	v, err := proto.Marshal(state)
	if err != nil {
		return err
	}
	if err := l.st.Set(comm.InstanceID(instanceID), v); err != nil {
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
	nodeID          comm.NodeID
	startInstanceID comm.InstanceID
	ackInstanceID   comm.InstanceID
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

func (s *sender) checkAck(instanceID comm.InstanceID) bool {
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

func (s *sender) ack(ackInstanceID comm.InstanceID, nodeID comm.NodeID) {
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

func (s *sender) prepare(start comm.InstanceID, nodeID comm.NodeID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.isPrepared() && !s.confirmed {
		s.prepared = true
		s.lastSendTime = time.Now()
		s.startInstanceID = start
		s.ackInstanceID = start
		s.nodeID = nodeID
		return true
	}
	return false
}

func (s *sender) confirm(start comm.InstanceID, nodeID comm.NodeID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.isPrepared() && !s.confirmed {
		if s.startInstanceID == start && s.nodeID == nodeID {
			s.confirmed = true
			s.wg.Add(1)
			go s.start()
			return true
		}
	}
	return false
}

func (s *sender) send(instanceID comm.InstanceID, nodeID comm.NodeID) {
	var (
		cs       comm.Checksum
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
			return
		}
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

func (s *sender) sendValue(instanceID comm.InstanceID, nodeID comm.NodeID, cs comm.Checksum) (comm.Checksum, error) {
	value, err := s.st.Get(instanceID)
	if err != nil {
		return cs, err
	}
	var state comm.AcceptorStateData
	if err = proto.Unmarshal(value, &state); err != nil {
		return cs, err
	}
	b := ballot{proposalID(state.GetAcceptedID()), comm.NodeID(state.GetAcceptedNodeID())}
	s.Learner.sendValue(nodeID, instanceID, b, state.GetAcceptedValue(), cs, true)

	return cs, nil
}
