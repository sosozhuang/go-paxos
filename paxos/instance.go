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
	"github.com/gogo/protobuf/proto"
	"github.com/sosozhuang/paxos/checkpoint"
	"github.com/sosozhuang/paxos/comm"
	"github.com/sosozhuang/paxos/logger"
	"github.com/sosozhuang/paxos/node"
	"github.com/sosozhuang/paxos/store"
	"time"
	"fmt"
	"errors"
)

const (
	log = logger.PaxosLogger
)

var (
	errChecksumFailed = errors.New("checksum failed")
)

type proposalID uint64
type checksum uint32

type Instance interface {
	Start(context.Context, <-chan struct{}) error
	Stop()
	IsReadyForNewValue() bool
	newInstance()
	GetInstanceID() comm.InstanceID
	getChecksum() checksum
	NewValue(context.Context)
	HandleMessage([]byte, int) error
	AddStateMachine(...node.StateMachine)
	ReceivePaxosMessage(*comm.PaxosMsg)
	ReceiveCheckpointMessage(*comm.CheckpointMsg)
	PauseReplayer()
	ContinueReplayer()
	PauseCleaner()
	ContinueCleaner()
	SetMaxLogCount(int)
	getMajorityCount() int
	GetProposerInstanceID() comm.InstanceID
}

func NewInstance(nodeID comm.NodeID, tp Transporter, st store.Storage,
	enableReplayer bool) (Instance, error) {
	var err error
	i := &instance{
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
	sms      map[comm.StateMachineID]node.StateMachine
	group    node.Group
	proposer Proposer
	acceptor Acceptor
	learner  Learner
	cpm      checkpoint.CheckpointManager
	st       store.Storage
	ctx context.Context
}

func (i *instance) Start(ctx context.Context, stopped <-chan struct{}) error {
	i.learner.start()
	if err := i.acceptor.start(); err != nil {
		return err
	}

	instanceID := i.GetCheckpointInstanceID() + 1
	if instanceID < i.acceptor.getInstanceID() {
		if err := i.replay(instanceID, i.acceptor.getInstanceID()); err != nil {
			return err
		}
		instanceID = i.acceptor.getInstanceID()
	} else {
		if instanceID > i.acceptor.getInstanceID() {
			i.acceptor.reset()
		}
		i.acceptor.setInstanceID(instanceID)
	}

	i.learner.setInstanceID(instanceID)
	i.proposer.setInstanceID(instanceID)
	i.proposer.setProposalID(i.acceptor.getPromisedProposalID() + 1)
	i.cpm.SetMaxChosenInstanceID(instanceID)

	if err := i.initChecksum(); err != nil {
		return err
	}

	i.learner.AskForLearn(time.Second * 3)

	if err := i.cpm.Start(ctx, stopped); err != nil {
		return err
	}

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

func (i *instance) initChecksum() error {
	if i.acceptor.getInstanceID() == 0 ||
		i.acceptor.getInstanceID() <= i.cpm.GetMinChosenInstanceID() {
		i.checksum = 0
		return nil
	}
	value, err := i.st.Get(i.acceptor.getInstanceID()-1)
	if err == store.ErrNotFound {
		i.checksum = 0
		return nil
	}
	if err != nil {
		return err
	}
	var state comm.AcceptorStateData
	if err := proto.Unmarshal(value, &state); err != nil {
		return err
	}
	i.checksum = state.GetChecksum()
	return nil
}

func (i *instance) replay(start, end comm.InstanceID) error {
	if start < i.cpm.GetMinChosenInstanceID() {
		return fmt.Errorf("%d%d", start, i.cpm.GetMinChosenInstanceID())
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
		if err = i.execute(id, state.GetAcceptedValue()); err != nil {
			return err
		}
	}
	return nil
}

func (i *instance) getChecksum() checksum {
	return i.checksum
}

func (i *instance) GetCleaner() checkpoint.Cleaner {
	return i.cpm.GetCleaner()
}

func (i *instance) GetReplayer() checkpoint.Replayer {
	return i.cpm.GetReplayer()
}

func (i *instance) IsReadyForNewValue() bool {
	return i.learner.isReadyForNewValue()
}

func (i *instance) GetInstanceID() comm.InstanceID {
	return i.acceptor.getInstanceID()
}

func (i *instance) newInstance() {
	i.proposer.newInstance()
	i.acceptor.newInstance()
	i.learner.newInstance()
}

func (i *instance) NewValue(ctx context.Context) {
	i.ctx = context.WithValue(ctx, "instance_id", i.proposer.getInstanceID())
	i.proposer.newValue(ctx)
}

func (i *instance) AddStateMachine(sms... node.StateMachine) {
	for _, sm := range sms {
		if _, ok := i.sms[sm.GetStateMachineID()]; !ok {
			i.sms[sm.GetStateMachineID()] = sm
		}
	}
}

func compare(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i, x := range a {
		if x != b[i] {
			return false
		}
	}
	return true
}
func (i *instance) ReceivePaxosMessage(msg *comm.PaxosMsg) {
	if i.cpm.IsAskForCheckpoint() {
		return
	}
	switch msg.GetType() {
	case comm.PaxosMsgType_NewValue:
	case comm.PaxosMsgType_PrepareReply:
		i.proposer.onPrepareReply(msg)
	case comm.PaxosMsgType_AcceptReply:
		i.proposer.onAcceptReply(msg)

	case comm.PaxosMsgType_Prepare:
		if err := i.verifyChecksum(msg); err != nil {
			return
		}
		if msg.GetInstanceID() == i.acceptor.getInstanceID() {
			i.acceptor.onPrepare(msg)
		} else if msg.GetInstanceID() > i.acceptor.getInstanceID() {

		}

	case comm.PaxosMsgType_Accept:
		if err := i.verifyChecksum(msg); err != nil {
			return
		}
		if msg.GetInstanceID() == i.acceptor.getInstanceID() {
			i.acceptor.onAccept(msg)
		} else if msg.GetInstanceID() > i.acceptor.getInstanceID() {

		}

	case comm.PaxosMsgType_AskForLearn:
		if err := i.verifyChecksum(msg); err != nil {
			return
		}
		i.learner.onAskForLearn(msg)
		if i.learner.isLearned() {
			i.checksum = i.learner.getChecksum()
			i.newInstance()
			i.cpm.SetMaxChosenInstanceID(i.acceptor.getInstanceID())
		}
	case comm.PaxosMsgType_ConfirmAskForLearn:
		if err := i.verifyChecksum(msg); err != nil {
			return
		}
		i.learner.onConfirmAskForLearn(msg)
		if i.learner.isLearned() {
			i.checksum = i.learner.getChecksum()
			i.newInstance()
			i.cpm.SetMaxChosenInstanceID(i.acceptor.getInstanceID())
		}
	case comm.PaxosMsgType_ProposalFinished:
		if err := i.verifyChecksum(msg); err != nil {
			return
		}
		i.learner.onProposalFinished(msg)
		if i.learner.isLearned() {
			i.checksum = i.learner.getChecksum()
			i.newInstance()
			i.cpm.SetMaxChosenInstanceID(i.acceptor.getInstanceID())
		}
	case comm.PaxosMsgType_SendValue:
		if err := i.verifyChecksum(msg); err != nil {
			return
		}
		i.learner.onSendValue(msg)
		if i.learner.isLearned() {
			i.checksum = i.learner.getChecksum()
			i.newInstance()
			i.cpm.SetMaxChosenInstanceID(i.acceptor.getInstanceID())
		}
	case comm.PaxosMsgType_SendInstanceID:
		if err := i.verifyChecksum(msg); err != nil {
			return
		}
		i.learner.onSendInstanceID(msg)
		if i.learner.isLearned() {
			i.checksum = i.learner.getChecksum()
			i.newInstance()
			i.cpm.SetMaxChosenInstanceID(i.acceptor.getInstanceID())
		}
	case comm.PaxosMsgType_AckSendValue:
		if err := i.verifyChecksum(msg); err != nil {
			return
		}
		i.learner.onAckSendValue(msg)
		if i.learner.isLearned() {
			i.checksum = i.learner.getChecksum()
			i.newInstance()
			i.cpm.SetMaxChosenInstanceID(i.acceptor.getInstanceID())
		}
	case comm.PaxosMsgType_AskForCheckpoint:
		if err := i.verifyChecksum(msg); err != nil {
			return
		}
		i.learner.onAskForCheckpoint(msg)
		if i.learner.isLearned() {
			if err := i.execute(i.learner.getInstanceID(), i.learner.getValue()); err != nil {
				log.Error(err)
				i.proposer.cancelSkipPrepare()
				return
			}
			i.checksum = i.learner.getChecksum()
			i.newInstance()
			i.cpm.SetMaxChosenInstanceID(i.acceptor.getInstanceID())
		}
	default:
		log.Errorf("Unknown paxos message type %v.\n", msg.GetType())
		return
	}
}

func (i *instance) ReceiveCheckpointMessage(msg *comm.CheckpointMsg) {
	switch msg.GetType() {
	case comm.CheckpointMsgType_SendFile:
		if !i.cpm.IsAskForCheckpoint() {
			return
		}
		i.learner.onSendCheckpoint(msg)
	case comm.CheckpointMsgType_AckSendFile:
		i.learner.onAckSendCheckpoint(msg)
	default:
		log.Errorf("Unknown checkpoint message type %v.\n", msg.GetType())
		return
	}
}

func (i *instance) verifyChecksum(msg *comm.PaxosMsg) error {
	if msg.GetChecksum() == 0 ||
		msg.GetInstanceID() != i.acceptor.getInstanceID() {
		return nil
	}
	if i.acceptor.getInstanceID() > 0 && i.checksum == 0 {
		i.checksum = msg.GetChecksum()
		return nil
	}
	if msg.GetChecksum() != i.checksum {
		log.Error("checksum failed")
		return errChecksumFailed
	}
	return nil
}

func (i *instance) execute(instanceID comm.InstanceID, v []byte) error {
	var smid comm.StateMachineID
	if err := comm.BytesToObject(v[:comm.SMIDLen], &smid); err != nil {
		return err
	}
	sm, ok := i.sms[smid]
	if !ok {
		return errors.New("smid not found")
	}
	var (
		ret interface{}
		err error
	)
	cid, ok := i.ctx.Value("instance_id").(comm.InstanceID)
	if ok && cid == instanceID {
		c := make(chan struct{})
		go func() {
			ret, err = sm.Execute(i.ctx, instanceID, v[comm.SMIDLen:])
			close(c)
		}()

		select {
		case <-i.ctx.Done():
			return i.ctx.Err()
		case <-c:
			result, ok := i.ctx.Value("result").(chan comm.Result)
			if ok {
				result <- comm.Result{ret, err}
			}
			return err
		}
	} else {
		_, err = sm.Execute(context.Background(), instanceID, v[comm.SMIDLen:])
		return err
	}
}

func (i *instance) GetCheckpointInstanceID() comm.InstanceID {
	for _, sm := range i.sms {
		sm
	}
	return 0
}

func (i *instance) PauseReplayer() {
	i.cpm.GetReplayer().Pause()
}

func (i *instance) ContinueReplayer() {
	i.cpm.GetReplayer().Continue()
}

func (i *instance) PauseCleaner() {
	i.cpm.GetCleaner().Pause()
}

func (i *instance) ContinueCleaner() {
	i.cpm.GetCleaner().Continue()
}

func (i *instance) SetMaxLogCount(c int) {
	i.cpm.GetCleaner().SetMaxLogCount(c)
}

func (i *instance) getMajorityCount() int {
	return i.group.GetMajorityCount()
}

func (i *instance) GetProposerInstanceID() comm.InstanceID {
	return i.proposer.getInstanceID()
}