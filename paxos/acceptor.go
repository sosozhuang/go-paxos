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
	"github.com/sosozhuang/paxos/comm"
	"github.com/sosozhuang/paxos/logger"
	"github.com/sosozhuang/paxos/store"
	"hash/crc32"
)

const (
	alog     = logger.AcceptorLogger
	crcTable = crc32.MakeTable(crc32.Castagnoli)
)

type Acceptor interface {
	NewInstance()
	GetInstanceID() comm.InstanceID
	onPrepare(*comm.PaxosMsg)
	onAccept(*comm.PaxosMsg)
}

func NewAcceptor() (Acceptor, error) {
	return nil, nil
}

type acceptor struct {
	state      *acceptorState
	nodeID     comm.NodeID
	instanceID comm.InstanceID
}

func (a *acceptor) onPrepare(msg *comm.PaxosMsg) error {
	replyMsg := &comm.PaxosMsg{
		Type:    comm.PaxosMsgType_PrepareReply.Enum(),
		InstanceID: proto.Uint64(a.instanceID),
		NodeID:     proto.Uint64(a.nodeID),
		ProposalID: proto.Uint64(msg.ProposalID),
	}
	b := ballot{msg.GetProposalID(), msg.GetProposalNodeID()}
	if b.ge(a.state.promisedBallot) {
		if a.state.acceptedBallot.proposalID > 0 {
			replyMsg.Value = a.state.value
		}
		a.state.promisedBallot = b
		if err := a.state.save(a.instanceID); err != nil {
			alog.Error(err)
			return err
		}
	} else {
		replyMsg.RejectByPromiseID = proto.Uint64(a.state.promisedBallot.proposalID)
	}

	//todo: send message
	return nil
}

func (a *acceptor) onAccept(msg *comm.PaxosMsg) error {
	replyMsg := &comm.PaxosMsg{
		Type:    comm.PaxosMsgType_AcceptReply.Enum(),
		InstanceID: proto.Uint64(a.instanceID),
		NodeID:     proto.Uint64(a.nodeID),
		ProposalID: proto.Uint64(msg.ProposalID),
	}
	b := ballot{msg.GetProposalID(), msg.GetProposalNodeID()}
	if b.ge(a.state.promisedBallot) {
		a.state.promisedBallot = b
		a.state.acceptedBallot = b
		a.state.value = msg.Value
		if err := a.state.save(a.instanceID); err != nil {
			return err
		}
	} else {
		replyMsg.RejectByPromiseID = proto.Uint64(a.state.promisedBallot.proposalID)
	}

	//todo: send message
	return nil
}

type acceptorState struct {
	promisedBallot ballot
	acceptedBallot ballot
	value          []byte
	checksum       uint32
	storage        store.Storage
}

func (a *acceptorState) save(instanceID comm.InstanceID, checksum uint32) error {
	if instanceID > 0 && checksum == 0 {
		a.checksum = uint32(0)
	} else if len(a.value) > 0 {
		a.checksum = crc32.Update(checksum, crcTable, a.value)
	}

	state := &comm.AcceptorStateData{
		InstanceID:     proto.Uint64(instanceID),
		PromisedID:      proto.Uint64(a.promisedBallot.proposalID),
		PromisedNodeID:  proto.Uint64(a.promisedBallot.nodeID),
		AcceptedID:     proto.Uint64(a.acceptedBallot.proposalID),
		AcceptedNodeID: proto.Uint64(a.acceptedBallot.nodeID),
		AcceptedValue:  a.value,
		Checksum:       proto.Uint32(a.checksum),
	}

	b, err := proto.Marshal(state)
	if err != nil {
		alog.Error(err)
		return err
	}
	return a.storage.Set(b)
}

func (a *acceptorState) load() (comm.InstanceID, error) {
	instanceID, err := a.storage.GetMaxInstanceID()
	if err != nil {
		return instanceID, err
	}
	b, err := a.storage.Get(instanceID)
	if err != nil {
		return instanceID, err
	}
	state := &comm.AcceptorStateData{}
	if err = proto.Unmarshal(b, state); err != nil {
		return instanceID, err
	}
	a.promisedBallot.proposalID = state.GetPromisedID()
	a.promisedBallot.nodeID = state.GetPromisedNodeID()
	a.acceptedBallot.proposalID = state.GetAcceptedID()
	a.acceptedBallot.nodeID = state.GetAcceptedNodeID()
	a.value = state.GetAcceptedValue()
	a.checksum = state.GetChecksum()
	return nil
}
