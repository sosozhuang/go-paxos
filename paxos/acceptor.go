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
	"github.com/gogo/protobuf/proto"
	"github.com/sosozhuang/paxos/comm"
	"github.com/sosozhuang/paxos/logger"
	"github.com/sosozhuang/paxos/store"
	"hash/crc32"
)

var (
	alog     = logger.AcceptorLogger
	crcTable = crc32.MakeTable(crc32.Castagnoli)
)

type Acceptor interface {
	start() error
	reset()
	newInstance()
	getInstanceID() comm.InstanceID
	setInstanceID(comm.InstanceID)
	getPromisedProposalID() proposalID
	getAcceptorState() acceptorState
	onPrepare(*comm.PaxosMsg)
	onAccept(*comm.PaxosMsg)
}

type acceptor struct {
	nodeID     comm.NodeID
	instanceID comm.InstanceID
	instance   comm.Instance
	tp         Transporter
	state      acceptorState
}

func newAcceptor(nodeID comm.NodeID, instance comm.Instance, tp Transporter, st store.Storage) (Acceptor, error) {
	s := acceptorState{
		st: st,
	}
	a := &acceptor{
		nodeID:     nodeID,
		instance:   instance,
		tp:         tp,
		state:      s,
	}
	return a, nil
}

func (a *acceptor) start() (err error) {
	a.instanceID, err = a.state.load()
	return
}

func (a *acceptor) reset() {
	a.state.reset()
}

func (a *acceptor) newInstance() {
	//atomic.AddUint64(&a.instanceID, 1)
	a.state.reset()
}

func (a *acceptor) getInstanceID() comm.InstanceID {
	return a.instanceID
}

func (a *acceptor) setInstanceID(id comm.InstanceID) {
	a.instanceID = id
}

func (a *acceptor) getPromisedProposalID() proposalID {
	return a.state.promisedBallot.proposalID
}

func (a *acceptor) getAcceptorState() acceptorState {
	return a.state
}

func (a *acceptor) onPrepare(msg *comm.PaxosMsg) {
	if comm.InstanceID(msg.GetInstanceID()) == a.instanceID+1 {
		newMsg := *msg
		newMsg.InstanceID = proto.Uint64(uint64(a.instanceID))
		newMsg.Type = comm.PaxosMsgType_ProposalFinished.Enum()
		a.instance.ReceivePaxosMessage(&newMsg)
		return
	}

	replyMsg := &comm.PaxosMsg{
		Type:       comm.PaxosMsgType_PrepareReply.Enum(),
		InstanceID: proto.Uint64(uint64(a.instanceID)),
		NodeID:     proto.Uint64(uint64(a.nodeID)),
		ProposalID: proto.Uint64(msg.GetProposalID()),
	}
	b := ballot{proposalID(msg.GetProposalID()), comm.NodeID(msg.GetProposalNodeID())}
	if b.ge(a.state.promisedBallot) {
		if a.state.acceptedBallot.proposalID > 0 {
			replyMsg.Value = a.state.acceptedValue
		}
		a.state.promisedBallot = b
		if err := a.state.save(a.instanceID, a.instance.GetChecksum()); err != nil {
			alog.Error(err)
			return
		}
	} else {
		replyMsg.RejectByPromiseID = proto.Uint64(uint64(a.state.promisedBallot.proposalID))
	}

	a.tp.send(comm.NodeID(msg.GetNodeID()), comm.MsgType_Paxos, replyMsg)
}

func (a *acceptor) onAccept(msg *comm.PaxosMsg) {
	if comm.InstanceID(msg.GetInstanceID()) == a.instanceID+1 {
		newMsg := *msg
		newMsg.InstanceID = proto.Uint64(uint64(a.instanceID))
		newMsg.Type = comm.PaxosMsgType_ProposalFinished.Enum()
		a.instance.ReceivePaxosMessage(&newMsg)
		return
	}

	replyMsg := &comm.PaxosMsg{
		Type:       comm.PaxosMsgType_AcceptReply.Enum(),
		InstanceID: proto.Uint64(uint64(a.instanceID)),
		NodeID:     proto.Uint64(uint64(a.nodeID)),
		ProposalID: proto.Uint64(msg.GetProposalID()),
	}
	b := ballot{proposalID(msg.GetProposalID()), comm.NodeID(msg.GetProposalNodeID())}
	if b.ge(a.state.promisedBallot) {
		a.state.promisedBallot = b
		a.state.acceptedBallot = b
		a.state.acceptedValue = msg.Value
		if err := a.state.save(a.instanceID, a.instance.GetChecksum()); err != nil {
			return
		}
	} else {
		replyMsg.RejectByPromiseID = proto.Uint64(uint64(a.state.promisedBallot.proposalID))
	}

	a.tp.send(comm.NodeID(msg.GetNodeID()), comm.MsgType_Paxos, replyMsg)
}

type acceptorState struct {
	st             store.Storage
	promisedBallot ballot
	acceptedBallot ballot
	acceptedValue  []byte
	checksum uint32
}

func (a *acceptorState) reset() {
	a.acceptedBallot.reset()
	a.acceptedValue = nil
	a.checksum = 0
}

func (a *acceptorState) save(instanceID comm.InstanceID, checksum uint32) error {
	if instanceID > 0 && checksum == 0 {
		a.checksum = 0
	} else if len(a.acceptedValue) > 0 {
		a.checksum = crc32.Update(checksum, crcTable, a.acceptedValue)
	}

	state := &comm.AcceptorStateData{
		InstanceID:     proto.Uint64(uint64(instanceID)),
		PromisedID:     proto.Uint64(uint64(a.promisedBallot.proposalID)),
		PromisedNodeID: proto.Uint64(uint64(a.promisedBallot.nodeID)),
		AcceptedID:     proto.Uint64(uint64(a.acceptedBallot.proposalID)),
		AcceptedNodeID: proto.Uint64(uint64(a.acceptedBallot.nodeID)),
		AcceptedValue:  a.acceptedValue,
		Checksum:       proto.Uint32(uint32(a.checksum)),
	}

	b, err := proto.Marshal(state)
	if err != nil {
		alog.Error(err)
		return err
	}
	return a.st.Set(instanceID, b)
}

func (a *acceptorState) load() (comm.InstanceID, error) {
	instanceID, err := a.st.GetMaxInstanceID()
	if err == store.ErrNotFound {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	b, err := a.st.Get(instanceID)
	if err != nil {
		return instanceID, err
	}
	state := &comm.AcceptorStateData{}
	if err = proto.Unmarshal(b, state); err != nil {
		return instanceID, err
	}
	a.promisedBallot.proposalID = proposalID(state.GetPromisedID())
	a.promisedBallot.nodeID = comm.NodeID(state.GetPromisedNodeID())
	a.acceptedBallot.proposalID = proposalID(state.GetAcceptedID())
	a.acceptedBallot.nodeID = comm.NodeID(state.GetAcceptedNodeID())
	a.acceptedValue = state.GetAcceptedValue()
	a.checksum = state.GetChecksum()
	return instanceID, nil
}
