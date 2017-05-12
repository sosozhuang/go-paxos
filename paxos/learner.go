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
	"github.com/sosozhuang/paxos/store"
	"github.com/sosozhuang/paxos/comm"
	"hash/crc32"
	"github.com/gogo/protobuf/proto"
)

type Learner interface {
	newInstance()
	onAskForLearn(*comm.PaxosMsg)
	onConfirmAskForLearn(*comm.PaxosMsg)
	onProposalFinished(*comm.PaxosMsg)
	onSendValue(*comm.PaxosMsg)
	onAckSendValue(*comm.PaxosMsg)
	onSendInstanceID(*comm.PaxosMsg)
	onAskForCheckpoint(*comm.PaxosMsg)
	proposalFinished(comm.InstanceID, proposalID)
	isLearned() bool
	isIMLast() bool
}

func NewLearner() (Learner, error) {
	return nil, nil
}

type learner struct {
	state *learnerState
	acceptor Acceptor
	learning bool
	nodeID comm.NodeID
	instanceID comm.InstanceID
	lastAckInstanceID comm.InstanceID
	lastSeenInstanceID comm.InstanceID
	lastSeenNodeID comm.NodeID
	tp Transporter
}

func (l *learner) SetLastSeenInstance(instanceID comm.InstanceID, nodeID comm.NodeID) {
	if instanceID > l.lastSeenInstanceID {
		l.lastSeenInstanceID = instanceID
		l.lastSeenNodeID = nodeID
	}

}

func (l *learner) proposalFinished(instanceID comm.InstanceID, proposalID proposalID) {
	msg := &comm.PaxosMsg{
		Type: comm.PaxosMsgType_ProposalFinished.Enum(),
		InstanceID: proto.Uint64(instanceID),
		NodeID: proto.Uint64(l.nodeID),
		ProposalID: proto.Uint64(proposalID),
		LastChecksum: proto.Uint32(0),
	}
	l.tp.Broadcast(msg, localFirst)
}

type learnerState struct {
	value []byte
	learned bool
	checksum uint32
	storage store.MultiGroupStorage
}

func (l *learnerState) learnWithoutWrite(instanceID comm.InstanceID, value []byte, checksum uint32) {
	l.value = value
	l.learned = true
	l.checksum = checksum
}

func (l *learnerState) learn(instanceID comm.InstanceID, b ballot, value []byte, checksum uint32) error {
	if instanceID > 0 && checksum == 0 {
		l.checksum = 0
	} else if len(l.value) > 0 {
		l.checksum = crc32.Update(checksum, crcTable, l.value)
	}
	state := &comm.AcceptorStateData{
		InstanceID: proto.Uint64(instanceID),
		AcceptedValue: l.value,
		PromiseID: proto.Uint64(b.proposalID),
		PromiseNodeID: proto.Uint64(b.nodeID),
		AcceptedID: proto.Uint64(b.proposalID),
		AcceptedNodeID: proto.Uint64(b.nodeID),
		Checksum: proto.Uint32(l.checksum),
	}
	b, err := proto.Marshal(state)
	if err != nil {
		return err
	}
	if err := l.storage.Set(0, instanceID, b); err != nil {
		return err
	}

	l.learnWithoutWrite(instanceID, value, l.checksum)
	return nil
}