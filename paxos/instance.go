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
	"context"
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

type proposalID uint64
type checksum uint32

type Instance interface {
	Start(context.Context, <-chan struct{}) error
	Stop()
	IsReadyForNewValue() bool
	NewInstance()
	GetCurrentInstanceID() comm.InstanceID
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

func NewInstance(nodeID comm.NodeID, tp Transporter, st store.Storage,
	enableReplayer bool) (Instance, error) {
	var err error
	i := &instance{
		ch:  make(chan []byte, 100),
		sms: make(map[comm.StateMachineID]node.StateMachine),
	}

	i.acceptor, err = newAcceptor(nodeID, i, tp, st)
	if err != nil {
		return nil, err
	}

	i.learner, err = newLearner(nodeID, i, tp, st, i.acceptor, i.cpm)
	if err != nil {
		return nil, err
	}

	i.proposer = newProposer(nodeID, tp, i.learner, time.Second, time.Second)

	i.cpm, err = checkpoint.NewCheckpointManager(enableReplayer, st)
	if err != nil {
		return nil, err
	}
	return i, nil
}

type instance struct {
	checksum
	ch       chan []byte
	sms      map[comm.StateMachineID]node.StateMachine
	group    node.Group
	proposer Proposer
	acceptor Acceptor
	learner  Learner
	cpm      checkpoint.CheckpointManager
	st       store.Storage
}

func (i *instance) Start(ctx context.Context, stopped <-chan struct{}) error {
	i.learner.start()
	if err := i.acceptor.start(); err != nil {
		return err
	}

	instanceID := i.GetCheckpointInstanceID() + 1
	if instanceID < i.acceptor.GetInstanceID() {
		if err := i.replay(instanceID, i.acceptor.GetInstanceID()); err != nil {
			return err
		}
		instanceID = i.acceptor.GetInstanceID()
	} else {
		i.acceptor.SetInstanceID(instanceID)
	}

	i.learner.SetInstanceID(instanceID)
	i.proposer.SetInstanceID(instanceID)
	i.proposer.setProposalID(i.acceptor.getPromisedProposalID() + 1)
	i.cpm.SetMaxChosenInstanceID(instanceID)

	if err := i.initChecksum(); err != nil {
		return err
	}

	i.learner.AskForLearn()

	if err := i.cpm.Start(ctx, stopped); err != nil {
		return err
	}

	return nil
}

func (i *instance) initChecksum() error {
	if i.acceptor.GetInstanceID() == comm.InstanceID(0) ||
		i.acceptor.GetInstanceID() <= i.cpm.GetMinChosenInstanceID() {
		i.checksum = checksum(0)
		return nil
	}
	value, err := i.st.Get(i.acceptor.GetInstanceID()-1)
	if err != nil && err != store.ErrNotFound {
		return err
	} else if err == store.ErrNotFound {
		i.checksum = checksum(0)
		return nil
	}
	var state comm.AcceptorStateData
	if err := proto.Unmarshal(value, &state); err != nil {
		return err
	}
	i.checksum = state.GetChecksum()
	return nil
}

func (i *instance) Stop() {
	if i.cpm != nil {
		i.cpm.Stop()
	}
	if i.learner != nil {
		i.learner.Stop()
	}
}

func (i *instance) getChecksum() checksum {
	return i.checksum
}

func (i *instance) Cleaner() checkpoint.Cleaner {
	return i.cpm.Cleaner()
}

func (i *instance) Replayer() checkpoint.Replayer {
	return i.cpm.Replayer()
}

func (i *instance) IsReadyForNewValue() bool {
	return i.learner.isReadyForNewValue()
}

func (i *instance) GetInstanceID() comm.InstanceID {
	return i.acceptor.GetInstanceID()
}

func (i *instance) NewInstance() {
	i.proposer.NewInstance()
	i.acceptor.NewInstance()
	i.learner.newInstance()
}

func (i *instance) NewValue(value []byte) {
	i.proposer.NewValue(value)
}

func (i *instance) replay(start, end comm.InstanceID) error {
	if start < i.cpm.GetMinChosenInstanceID() {
		return errors.New("")
	}
	var state comm.AcceptorStateData
	for id := start; id < end; id += 1 {
		b, err := i.st.Get(id)
		if err != nil {
			return err
		}
		if err = proto.Unmarshal(b, &state); err != nil {
			return err
		}
		// todo: execute statemachine
	}
	return nil
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

func (i *instance) ChecksumLogic(msg *comm.PaxosMsg) {
	if msg.GetLastChecksum() == 0 ||
		msg.GetInstanceID() != i.acceptor.GetInstanceID() {
		return
	}
	if i.acceptor.GetInstanceID() > comm.InstanceID(0) && i.checksum == checksum(0) {
		i.checksum = msg.GetLastChecksum()
		return
	}
	if msg.GetLastChecksum() != i.checksum {
		log.Error("checksum failed")
	}
}

func (i *instance) GetCheckpointInstanceID() comm.InstanceID {
	for _, sm := range i.sms {

	}
}
