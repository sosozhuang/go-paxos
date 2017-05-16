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
	"github.com/gogo/protobuf/proto"
	"github.com/sosozhuang/paxos/checkpoint"
	"github.com/sosozhuang/paxos/comm"
	"github.com/sosozhuang/paxos/logger"
	"github.com/sosozhuang/paxos/store"
	"hash/crc32"
	"sync"
	"time"
	"sync/atomic"
)

const (
	llog = logger.LearnerLogger
)

type Learner interface {
	start()
	Stop()
	setInstanceID(comm.InstanceID)
	newInstance()
	AskForLearn()
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
	getChecksum() checksum
	onSendCheckpoint(*comm.CheckpointMsg)
	onAckSendCheckpoint(*comm.CheckpointMsg)
}

func newLearner(nodeID comm.NodeID, instance Instance, tp Transporter, st store.Storage, acceptor Acceptor, cpm checkpoint.CheckpointManager) (Learner, error) {
	return nil, nil
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
	instance           Instance
	cpm                checkpoint.CheckpointManager
	tp                 Transporter
	sender
}

func (l *learner) start() {
	l.sender.start()
}

func (l *learner) newInstance() {
	atomic.AddUint64(&l.instanceID, 1)
	l.state
}

func (l *learner) setInstanceID(id comm.InstanceID) {
	l.instanceID = id
}

func (l *learner) SetLastSeenInstance(instanceID comm.InstanceID, nodeID comm.NodeID) {
	if instanceID > l.lastSeenInstanceID {
		l.lastSeenInstanceID = instanceID
		l.lastSeenNodeID = nodeID
	}

}

func (l *learner) proposalFinished(instanceID comm.InstanceID, proposalID proposalID) {
	msg := &comm.PaxosMsg{
		Type:         comm.PaxosMsgType_ProposalFinished.Enum(),
		InstanceID:   proto.Uint64(instanceID),
		NodeID:       proto.Uint64(l.nodeID),
		ProposalID:   proto.Uint64(proposalID),
		Checksum: proto.Uint32(0),
	}
	l.instance.ReceivePaxosMessage(msg)
	l.tp.broadcast(comm.MsgType_Paxos, msg)
}

func (l *learner) onProposalFinished(msg *comm.PaxosMsg) {
	if l.instanceID != msg.GetInstanceID() {
		return
	}
	if !l.acceptor.getAcceptorState().acceptedBallot.valid() {
		return
	}
	b := ballot{proposalID(0), comm.NodeID(0)}
	if l.acceptor.getAcceptorState().acceptedBallot.ne(b) {
		return
	}

	l.state.learn(l.acceptor.getAcceptorState().acceptedValue, 0)
	l.broadcastToFollower()
}

func (l *learner) broadcastToFollower() {
	msg := &comm.PaxosMsg{
		Type:           comm.PaxosMsgType_SendValue.Enum(),
		InstanceID:     proto.Uint64(l.instanceID),
		NodeID:         proto.Uint64(l.nodeID),
		ProposalNodeID: proto.Uint64(l.acceptor.getAcceptorState().acceptedBallot.nodeID),
		ProposalID:     proto.Uint64(l.acceptor.getAcceptorState().acceptedBallot.proposalID),
		Value:          l.acceptor.getAcceptorState().acceptedValue,
		Checksum:   proto.Uint32(0),
	}

	l.tp.broadcastToFollowers(comm.MsgType_Paxos, msg)
}

func (l *learner) sendValue(nodeID comm.NodeID, instanceID comm.InstanceID, b ballot, value []byte, checksum checksum, ack bool) {
	msg := &comm.PaxosMsg{
		Type: comm.PaxosMsgType_SendValue.Enum(),
		InstanceID: proto.Uint64(instanceID),
		NodeID: proto.Uint64(l.nodeID),
		ProposalID: proto.Uint64(b.proposalID),
		ProposalNodeID: proto.Uint64(b.nodeID),
		Value: value,
		Checksum: proto.Uint32(checksum),
	}
	if ack {
		msg.Flag = 0
	}
	l.tp.send(nodeID, comm.MsgType_Paxos, msg)
}

func (l *learner) onSendValue(msg *comm.PaxosMsg) {
	if msg.GetInstanceID() != l.instanceID {
		return
	}
	b := ballot{msg.GetProposalID(), msg.GetProposalNodeID()}
	if err := l.state.learnAndSave(msg.GetInstanceID(), b, msg.GetValue(), 0); err != nil {
		llog.Error(err)
		return
	}
	if msg.GetFlag() == 1 {
		// todo: stop old goroutine
		l.AskForLearn()
		l.ackSendValue(msg.GetNodeID())
	}
}

func (l *learner) ackSendValue(nodeID comm.NodeID) {
	if l.instanceID < l.lastAckInstanceID+100 {
		return
	}
	l.lastAckInstanceID = l.instanceID

	msg := &comm.PaxosMsg{
		Type:       comm.PaxosMsgType_AckSendValue.Enum(),
		InstanceID: proto.Uint64(l.instanceID),
		NodeID:     proto.Uint64(l.nodeID),
	}
	l.tp.send(nodeID, comm.MsgType_Paxos, msg)
}

func (l *learner) onAckSendValue(msg *comm.PaxosMsg) {
	l.sender.ack(msg.GetInstanceID(), msg.GetNodeID())
}

func (l *learner) AskForLearn() {
	go l.askForLearn()
}

func (l *learner) askForLearn() {
	l.cpm.ExitAskForCheckpoint()
	msg := &comm.PaxosMsg{
		Type:       comm.PaxosMsgType_AskForLearn.Enum(),
		InstanceID: proto.Uint64(l.instanceID),
		NodeID:     proto.Uint64(l.nodeID),
	}
	// todo: check if in follower mode
	if true {
		msg.ProposalNodeID = 0
	}
	l.tp.broadcast(comm.MsgType_Paxos, msg)
	l.instance.ReceivePaxosMessage(msg)
	l.tp.broadcastToLearnNodes()

	time.Sleep(time.Second)
	go l.askForLearn()
}

func (l *learner) onConfirmAskForLearn(msg *comm.PaxosMsg) {
	l.sender.confirm(msg.GetInstanceID(), msg.GetNodeID())
}

func (l *learner) isLearned() bool {
	return l.state.learned
}

func (l *learner) isReadyForNewValue() bool {
	return l.instanceID+1 >= l.lastSeenInstanceID
}

func (l *learner) getChecksum() checksum {
	return l.state.checksum
}

func (l *learner) askForCheckpoint(nodeID comm.NodeID) {
	if err := l.cpm.PrepareForAskForCheckpoint(nodeID); err != nil {
		log.Error(err)
		return
	}
	msg := &comm.PaxosMsg{
		Type:       comm.PaxosMsgType_AskForCheckpoint.Enum(),
		NodeID:     proto.Uint64(l.nodeID),
		InstanceID: proto.Uint64(l.instanceID),
	}
	l.tp.send(nodeID, comm.MsgType_Paxos, msg)
}

func (l *learner) onAskForCheckpoint(msg *comm.PaxosMsg) {

}

func (l *learner) sendCheckpointStarted(nodeID comm.NodeID, uuid uint64, sequence uint64, checkpointInstanceID comm.InstanceID) {
	msg := &comm.CheckpointMsg{
		Type:                 comm.CheckpointMsgType_SendFile.Enum(),
		NodeID:               proto.Uint64(l.nodeID),
		Flag:                 comm.CheckPointMsgFlag_Started.Enum(),
		UUID:                 proto.Uint64(uuid),
		Sequence:             proto.Uint64(sequence),
		CheckpointInstanceID: proto.Uint64(checkpointInstanceID),
	}
	l.tp.send(nodeID, comm.MsgType_Checkpoint, msg)
}

func (l *learner) sendCheckpointFinished(nodeID comm.NodeID, uuid uint64, sequence uint64, checkpointInstanceID comm.InstanceID) {
	msg := &comm.CheckpointMsg{
		Type:                 comm.CheckpointMsgType_SendFile.Enum(),
		NodeID:               proto.Uint64(l.nodeID),
		Flag:                 comm.CheckPointMsgFlag_Finished.Enum(),
		UUID:                 proto.Uint64(uuid),
		Sequence:             proto.Uint64(sequence),
		CheckpointInstanceID: proto.Uint64(checkpointInstanceID),
	}
	l.tp.send(nodeID, comm.MsgType_Checkpoint, msg)
}

func (l *learner) sendCheckpoint(nodeID comm.NodeID, uuid uint64, sequence uint64, checkpointInstanceID comm.InstanceID,
	checksum checksum, path string, smid comm.StateMachineID, offset uint64, b []byte) {
	msg := &comm.CheckpointMsg{
		Type:                 comm.CheckpointMsgType_SendFile.Enum(),
		NodeID:               proto.Uint64(l.nodeID),
		Flag:                 comm.CheckPointMsgFlag_Sending.Enum(),
		UUID:                 proto.Uint64(uuid),
		Sequence:             proto.Uint64(sequence),
		CheckpointInstanceID: proto.Uint64(checkpointInstanceID),
		Checksum:             proto.Uint32(checksum),
		FilePath:             proto.String(path),
		SMID:                 proto.Int32(smid),
		Offset:               proto.Uint64(offset),
		Buffer:               b,
	}
	l.tp.send(nodeID, comm.MsgType_Checkpoint, msg)
}

func (l *learner) onSendCheckpointStarted(msg *comm.CheckpointMsg) {

}

func (l *learner) onSendCheckpointSending(msg *comm.CheckpointMsg) {

}

func (l *learner) onSendCheckpointFinished(msg *comm.CheckpointMsg) {

}

func (l *learner) onSendCheckpoint(msg *comm.CheckpointMsg) {
	var err error
	switch msg.GetFlag() {
	case comm.CheckPointMsgFlag_Started:
	case comm.CheckPointMsgFlag_Sending:
	case comm.CheckPointMsgFlag_Finished:
	}
	if err != nil {
		l.ackSendCheckpoint(msg.GetNodeID(), msg.GetUUID(), msg.GetSequence(), comm.checkflag)
	} else {

	}
}

func (l *learner) ackSendCheckpoint(nodeID comm.NodeID, uuid, sequence uint64, flag comm.CheckPointMsgFlag) {
	msg := &comm.CheckpointMsg{
		Type:     comm.CheckpointMsgType_AckSendFile.Enum(),
		NodeID:   proto.Uint64(l.nodeID),
		UUID:     proto.Uint64(uuid),
		Sequence: proto.Uint64(sequence),
		Flag:     flag.Enum(),
	}
	l.tp.send(nodeID, comm.MsgType_Checkpoint, msg)
}

func (l *learner) onAckSendCheckpoint(msg *comm.CheckpointMsg) {

}

type learnerState struct {
	checksum
	learned bool
	value   []byte
	st      store.Storage
}

func (l *learnerState) reset() {
	l.learned = false
	l.value = nil
	l.checksum = 0
}

func (l *learnerState) learn(value []byte, cs checksum) {
	l.value = value
	l.learned = true
	l.checksum = cs
}

func (l *learnerState) learnAndSave(instanceID comm.InstanceID, b ballot, value []byte, cs checksum) error {
	if instanceID > 0 && cs == 0 {
		l.checksum = 0
	} else if l.value != nil && len(l.value) > 0 {
		l.checksum = crc32.Update(cs, crcTable, l.value)
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
	b, err := proto.Marshal(state)
	if err != nil {
		return err
	}
	if err := l.st.Set(instanceID, b); err != nil {
		return err
	}

	l.learn(value, l.checksum)
	return nil
}

type sender struct {
	Learner
	sending         bool
	confirmed       bool
	lastSendTime    time.Time
	lastAckTime     time.Time
	nodeID          comm.NodeID
	beginInstanceID comm.InstanceID
	ackInstanceID   comm.InstanceID
	mu              sync.Mutex
	st store.Storage
}

func (s *sender) isSending() bool {
	if !s.sending {
		return false
	}

	if time.Now().Sub(s.lastSendTime) > time.Second*5 {
		return false
	}

	return true
}

func (s *sender) start() {
	for {

	}
}

func (s *sender) ack(ackInstanceID comm.InstanceID, nodeID comm.NodeID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.isSending() && s.confirmed {
		if s.nodeID == nodeID {
			if ackInstanceID > s.ackInstanceID {
				s.ackInstanceID = ackInstanceID
				s.lastAckTime = time.Now()
			}
		}
	}
}

func (s *sender) prepare(beginInstanceID comm.InstanceID, nodeID comm.NodeID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.isSending() && !s.confirmed {
		s.sending = true
		s.lastSendTime = time.Now()
		s.beginInstanceID = beginInstanceID
		s.ackInstanceID = beginInstanceID
		s.nodeID = nodeID
		return true
	}
	return false
}

func (s *sender) confirm(beginInstanceID comm.InstanceID, nodeID comm.NodeID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.isSending() && !s.confirmed {
		if s.beginInstanceID == beginInstanceID && s.nodeID == nodeID {
			s.confirmed = true
			return true
		}
	}
	return false
}

func (s *sender) send(instanceID comm.InstanceID, nodeID comm.NodeID) error {
	value, err := s.st.Get(instanceID)
	if err != nil {
		return err
	}
	var state comm.AcceptorStateData
	if err = proto.Unmarshal(value, &state); err != nil {
		return err
	}
	b := ballot{state.GetAcceptedID(), state.GetAcceptedNodeID()}

	return nil
}