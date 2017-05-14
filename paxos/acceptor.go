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
	start() error
	NewInstance()
	GetInstanceID() comm.InstanceID
	SetInstanceID(comm.InstanceID)
	getPromisedProposalID() proposalID
	onPrepare(*comm.PaxosMsg)
	onAccept(*comm.PaxosMsg)
}

func newAcceptor(nodeID comm.NodeID, instance Instance, tp Transporter, st store.Storage) (Acceptor, error) {
	s := acceptorState{
		st: st,
	}
	//instanceID, err := s.load()
	//if err != nil {
	//	return nil, err
	//}
	a := &acceptor{
		nodeID:     nodeID,
		instance:   instance,
		tp:         tp,
		state:      s,
	}
	return a, nil
}

type acceptor struct {
	nodeID     comm.NodeID
	instanceID comm.InstanceID
	instance   Instance
	tp         Transporter
	state      acceptorState
}

func (a *acceptor) start() (err error) {
	a.instanceID, err = a.state.load()
	//a.state.init()
	return
}

func (a *acceptor) GetInstanceID() comm.InstanceID {
	return a.instanceID
}

func (a *acceptor) SetInstanceID(id comm.InstanceID) {
	a.instanceID = id
}

func (a *acceptor) getPromisedProposalID() proposalID {
	return a.state.promisedBallot.proposalID
}

func (a *acceptor) onPrepare(msg *comm.PaxosMsg) error {
	replyMsg := &comm.PaxosMsg{
		Type:       comm.PaxosMsgType_PrepareReply.Enum(),
		InstanceID: proto.Uint64(a.instanceID),
		NodeID:     proto.Uint64(a.nodeID),
		ProposalID: proto.Uint64(msg.ProposalID),
	}
	b := ballot{msg.GetProposalID(), msg.GetProposalNodeID()}
	if b.ge(a.state.promisedBallot) {
		if a.state.acceptedBallot.proposalID > 0 {
			replyMsg.Value = a.state.acceptedValue
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
		Type:       comm.PaxosMsgType_AcceptReply.Enum(),
		InstanceID: proto.Uint64(a.instanceID),
		NodeID:     proto.Uint64(a.nodeID),
		ProposalID: proto.Uint64(msg.ProposalID),
	}
	b := ballot{msg.GetProposalID(), msg.GetProposalNodeID()}
	if b.ge(a.state.promisedBallot) {
		a.state.promisedBallot = b
		a.state.acceptedBallot = b
		a.state.acceptedValue = msg.Value
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
	st             store.Storage
	promisedBallot ballot
	acceptedBallot ballot
	acceptedValue  []byte
	checksum
}

//func (a *acceptorState) init() {
//	a.acceptedBallot.reset()
//	a.acceptedValue = make([]byte, 0)
//	a.checksum = checksum(0)
//}

func (a *acceptorState) save(instanceID comm.InstanceID, checksum uint32) error {
	if instanceID > 0 && checksum == 0 {
		a.checksum = uint32(0)
	} else if len(a.acceptedValue) > 0 {
		a.checksum = crc32.Update(checksum, crcTable, a.acceptedValue)
	}

	state := &comm.AcceptorStateData{
		InstanceID:     proto.Uint64(instanceID),
		PromisedID:     proto.Uint64(a.promisedBallot.proposalID),
		PromisedNodeID: proto.Uint64(a.promisedBallot.nodeID),
		AcceptedID:     proto.Uint64(a.acceptedBallot.proposalID),
		AcceptedNodeID: proto.Uint64(a.acceptedBallot.nodeID),
		AcceptedValue:  a.acceptedValue,
		Checksum:       proto.Uint32(a.checksum),
	}

	b, err := proto.Marshal(state)
	if err != nil {
		alog.Error(err)
		return err
	}
	return a.st.Set(b)
}

func (a *acceptorState) load() (comm.InstanceID, error) {
	instanceID, err := a.st.GetMaxInstanceID()
	if err == store.ErrNotFound {
		return comm.InstanceID(0), nil
	}
	if err != nil {
		return instanceID, err
	}
	b, err := a.st.Get(instanceID)
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
	a.acceptedValue = state.GetAcceptedValue()
	a.checksum = state.GetChecksum()
	return nil
}
