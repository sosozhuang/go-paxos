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
	"github.com/sosozhuang/paxos/node"
	"github.com/sosozhuang/paxos/store"
	"time"
)

const (
	log = logger.PaxosLogger
)

type Instance interface {
	NewInstance()
	GetCurrentInstanceID() comm.InstanceID
	IsIMLast() bool
	GetAcceptor() Acceptor
	GetLearner() Learner
	GetProposer() Proposer
	NewValue([]byte)
	HandleMessage([]byte, int) error
	Cleaner() checkpoint.Cleaner
	Replayer() checkpoint.Replayer
	AddStateMachine(...node.StateMachine)
	ReceivePaxosMessage(*comm.PaxosMsg)
	ReceiveCheckpointMessage(*comm.CheckpointMsg)
}

func NewInstance(tp Transporter, st store.Storage) (Instance, error) {
	a, err := NewAcceptor()
	if err != nil {
		return nil, err
	}
	l, err := NewLearner()
	if err != nil {
		return nil, err
	}
	p, err := NewProposer()
	if err != nil {
		return nil, err
	}
	return &instance{
		proposer: p,
		acceptor: a,
		learner:  l,
		ch:       make(chan []byte, 100),
		sms:      make(map[comm.StateMachineID]node.StateMachine),
	}, nil
}

type instance struct {
	group    node.Group
	proposer Proposer
	acceptor Acceptor
	learner  Learner
	ch       chan []byte
	sms      map[comm.StateMachineID]node.StateMachine
}

func (i *instance) NewInstance() {
	i.proposer.NewInstance()
	i.acceptor.NewInstance()
	i.learner.newInstance()
}

func (i *instance) NewValue(value []byte) {
	i.proposer.NewValue(value)
}

func (i *instance) OnReceive(message []byte) error {
	select {
	case i.ch <- message:
		return nil
	case time.After(time.Second * 3):
		return errors.New("time out")
	}
}

func (i *instance) CheckNewValue() {
	for value := range i.ch {
		if !i.learner.isIMLast() {
			break
		}

		if !i.group.CheckMemberShip() {
			break
		}

		//if len(value) > maxValueLength &&
		//	(i.proposer.GetInstanceID() == 0 || ) {
		//	break
		//}

		if i.group.IsUseMembership() {

		} else {
			msg := i.proposer.NewValue(value)
			// todo: send to acceptor and remote
		}

	}
}

func (i *instance) AddStateMachine(sms ...node.StateMachine) {
	for _, sm := range sms {
		if _, ok := i.sms[sm.GetStateMachineID()]; !ok {
			i.sms[sm.GetStateMachineID()] = sm
		}
	}
}

func (i *instance) ReceivePaxosMessage(msg *comm.PaxosMsg) {
	switch msg.GetType() {
	case comm.PaxosMsgType_NewValue:
	case comm.PaxosMsgType_PrepareReply:
		if msg.GetInstanceID() != i.proposer.GetInstanceID() {
			log.Info("instance not equal.")
			return
		}
		i.proposer.OnPrepareReply(msg)
	case comm.PaxosMsgType_AcceptReply:
		if msg.GetInstanceID() != i.proposer.GetInstanceID() {
			log.Info("instance not equal.")
			return
		}
		i.proposer.OnAcceptReply(msg)

	case comm.PaxosMsgType_Prepare:
		if msg.GetInstanceID() == i.acceptor.GetInstanceID()+1 {
			m := *msg
			m.InstanceID = proto.Uint64(i.acceptor.GetInstanceID())
			m.Type = comm.PaxosMsgType_ProposalFinished.Enum()
			i.ReceivePaxosMessage(&m)
		}
		if msg.GetInstanceID() == i.acceptor.GetInstanceID() {
			i.acceptor.onPrepare(msg)
		} else if msg.GetInstanceID() > i.acceptor.GetInstanceID() {

		}

	case comm.PaxosMsgType_Accept:
		if msg.GetInstanceID() == i.acceptor.GetInstanceID()+1 {
			m := *msg
			m.InstanceID = proto.Uint64(i.acceptor.GetInstanceID())
			m.Type = comm.PaxosMsgType_ProposalFinished.Enum()
			i.ReceivePaxosMessage(&m)
		}
		if msg.GetInstanceID() == i.acceptor.GetInstanceID() {
			i.acceptor.onAccept(msg)
		} else if msg.GetInstanceID() > i.acceptor.GetInstanceID() {

		}

	case comm.PaxosMsgType_AskForLearn:
		i.learner.onAskForLearn(msg)
		if i.learner.isLearned() {
			i.NewInstance()
		}
	case comm.PaxosMsgType_ConfirmAskForLearn:
		i.learner.onConfirmAskForLearn(msg)
		i.NewInstance()
	case comm.PaxosMsgType_ProposalFinished:
		i.learner.onProposalFinished(msg)
		i.NewInstance()
	case comm.PaxosMsgType_SendValue:
		i.learner.onSendValue(msg)
		i.NewInstance()
	case comm.PaxosMsgType_SendInstanceID:
		i.learner.onSendInstanceID(msg)
		i.NewInstance()
	case comm.PaxosMsgType_AckSendValue:
		i.learner.onAckSendValue(msg)
		i.NewInstance()
	case comm.PaxosMsgType_AskForCheckpoint:
		i.learner.onAskForCheckpoint(msg)
		i.NewInstance()
	default:
		log.Errorf("Unknown paxos message type %v.\n", msg.GetType())
		return
	}
}

func (i *instance) ReceiveCheckpointMessage(msg *comm.CheckpointMsg) {
	switch msg.GetType() {
	case comm.CheckpointMsgType_SendFile:
	case comm.CheckpointMsgType_AckSendFile:
	default:
		log.Errorf("Unknown checkpoint message type %v.\n", msg.GetType())
		return
	}
}
