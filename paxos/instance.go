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
	"container/heap"
	"context"
	"errors"
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/sosozhuang/paxos/checkpoint"
	"github.com/sosozhuang/paxos/comm"
	"github.com/sosozhuang/paxos/logger"
	"github.com/sosozhuang/paxos/store"
	"time"
)

var (
	log               = logger.PaxosLogger
	errChecksumFailed = errors.New("verify checksum failed")
)

type Instance interface {
	getNodeID() uint64
	getChecksum() uint32
	getMajorityCount() int
	ReceivePaxosMessage(*comm.PaxosMsg)
	lockCheckpointState() error
	unlockCheckpointState()
	getAllCheckpoint() ([]uint32, []uint64, []string, [][]string)
}

type instance struct {
	checksum uint32
	sms      map[uint32]comm.StateMachine
	groupCfg comm.GroupConfig
	proposer Proposer
	acceptor Acceptor
	learner  Learner
	cpm      checkpoint.CheckpointManager
	st       store.Storage
	ctx      context.Context
	ch       chan *comm.PaxosMsg
	retries  *msgHeap
}

func NewInstance(groupCfg comm.GroupConfig, sender comm.Sender, st store.Storage) (comm.Instance, error) {
	log = logger.PaxosLogger
	var err error
	i := &instance{
		groupCfg: groupCfg,
		st:       st,
		sms:      make(map[uint32]comm.StateMachine),
		ctx:      context.TODO(),
		ch:       make(chan *comm.PaxosMsg, 10),
		retries:  &msgHeap{},
	}
	heap.Init(i.retries)

	i.cpm, err = checkpoint.NewCheckpointManager(groupCfg, i, st)
	if err != nil {
		return nil, err
	}
	tp := newTransporter(groupCfg, sender)
	i.acceptor = newAcceptor(i, tp, st)
	i.learner = newLearner(groupCfg, i, tp, st, i.acceptor, i.cpm)
	i.proposer = newProposer(i, tp, i.learner)

	return i, nil
}

func (i *instance) Start(ctx context.Context, stopped <-chan struct{}) error {
	ch := make(chan error)
	go func() {
		if err := i.start(stopped); err != nil {
			ch <- err
		} else {
			close(ch)
		}
	}()

	select {
	case <-stopped:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case err, ok := <-ch:
		if !ok {
			return nil
		}
		return err
	}
}

func (i *instance) start(stopped <-chan struct{}) error {
	go i.handleRetry(stopped)
	if err := i.acceptor.load(); err != nil {
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

	i.learner.start(stopped)
	i.cpm.Start(stopped)

	return nil
}

func (i *instance) Stop() {
	if i.cpm != nil {
		i.cpm.Stop()
		i.cpm = nil
	}
	if i.learner != nil {
		i.learner.stop()
		i.learner = nil
	}
}

func (i *instance) getNodeID() uint64 {
	return i.groupCfg.GetNodeID()
}

func (i *instance) getMajorityCount() int {
	return i.groupCfg.GetMajorityCount()
}

func (i *instance) initChecksum() error {
	if i.acceptor.getInstanceID() == 0 ||
		i.acceptor.getInstanceID() <= i.cpm.GetMinChosenInstanceID() {
		i.checksum = 0
		return nil
	}
	value, err := i.st.Get(i.acceptor.getInstanceID() - 1)
	if err == store.ErrNotFound {
		i.checksum = 0
		return nil
	}
	if err != nil {
		return err
	}
	var state comm.AcceptorStateData
	if err := proto.Unmarshal(value, &state); err != nil {
		return fmt.Errorf("instance: unmarshal acceptor state error: %v", err)
	}
	i.checksum = state.GetChecksum()
	return nil
}

func (i *instance) replay(start, end uint64) error {
	if start < i.cpm.GetMinChosenInstanceID() {
		return fmt.Errorf("instance: start instance id %d less then min chosen instance id %d", start, i.cpm.GetMinChosenInstanceID())
	}
	var state comm.AcceptorStateData
	for id := start; id < end; id++ {
		b, err := i.st.Get(id)
		if err != nil {
			return err
		}
		if err = proto.Unmarshal(b, &state); err != nil {
			return fmt.Errorf("instance: unmarshal acceptor state error: %v", err)
		}
		if err = i.execute(id, state.GetAcceptedValue()); err != nil {
			return err
		}
	}
	return nil
}

func (i *instance) getChecksum() uint32 {
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

func (i *instance) GetInstanceID() uint64 {
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

func (i *instance) AddStateMachine(sms ...comm.StateMachine) {
	for _, sm := range sms {
		if _, ok := i.sms[sm.GetStateMachineID()]; !ok {
			i.sms[sm.GetStateMachineID()] = sm
		} else {
			log.Warningf("Instance state machine %d already exist.\n", sm.GetStateMachineID())
		}
	}
}

func (i *instance) handleRetry(stopped <-chan struct{}) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-stopped:
			return
		case <-ticker.C:
			i.retryPaxosMessage()
		case msg := <-i.ch:
			heap.Push(i.retries, msg)
		}
	}
}

func (i *instance) retryPaxosMessage() {
	for i.retries.Len() > 0 {
		msg := heap.Pop(i.retries).(*comm.PaxosMsg)
		if msg.GetInstanceID() == i.GetInstanceID() {
			i.ReceivePaxosMessage(msg)
		} else {
			heap.Push(i.retries, msg)
			break
		}

	}
	if i.retries.Len() > 300 {
		i.retries = &msgHeap{}
	}
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
			log.Errorf("Instance verify prepare message checksum error: %v.\n", err)
			return
		}
		if msg.GetInstanceID() == i.acceptor.getInstanceID() ||
			msg.GetInstanceID() == i.acceptor.getInstanceID()+1 {
			i.acceptor.onPrepare(msg)
		} else if msg.GetInstanceID() > i.acceptor.getInstanceID() {
			if msg.GetInstanceID() >= i.learner.getLastSeenInstanceID() {
				if msg.GetInstanceID() < i.acceptor.getInstanceID()+300 {
					i.ch <- msg
				}
			}
		}

	case comm.PaxosMsgType_Accept:
		if err := i.verifyChecksum(msg); err != nil {
			log.Errorf("Instance verify accept message checksum error: %v.\n", err)
			return
		}
		if msg.GetInstanceID() == i.acceptor.getInstanceID() ||
			msg.GetInstanceID() == i.acceptor.getInstanceID()+1 {
			i.acceptor.onAccept(msg)
		} else if msg.GetInstanceID() > i.acceptor.getInstanceID() {
			if msg.GetInstanceID() >= i.learner.getLastSeenInstanceID() {
				if msg.GetInstanceID() < i.acceptor.getInstanceID()+300 {
					i.ch <- msg
				}
			}
		}

	case comm.PaxosMsgType_AskForLearn:
		if err := i.verifyChecksum(msg); err != nil {
			log.Errorf("Instance verify ask for learn message checksum error: %v.\n", err)
			return
		}
		i.learner.onAskForLearn(msg)
	case comm.PaxosMsgType_ConfirmAskForLearn:
		if err := i.verifyChecksum(msg); err != nil {
			log.Errorf("Instance verify confirm ask for learn message checksum error: %v.\n", err)
			return
		}
		i.learner.onConfirmAskForLearn(msg)
	case comm.PaxosMsgType_ProposalFinished:
		if err := i.verifyChecksum(msg); err != nil {
			log.Errorf("Instance verify proposal finished message checksum error: %v.\n", err)
			return
		}
		i.learner.onProposalFinished(msg)
		if i.learner.isLearned() {
			if err := i.execute(i.learner.getInstanceID(), i.learner.getValue()); err != nil {
				log.Errorf("Instance execute state machine error: %v.\n", err)
				i.proposer.cancelSkipPrepare()
				return
			}
			i.checksum = i.learner.getChecksum()
			i.newInstance()
			i.cpm.SetMaxChosenInstanceID(i.acceptor.getInstanceID())
		}
	case comm.PaxosMsgType_SendValue:
		if err := i.verifyChecksum(msg); err != nil {
			log.Errorf("Instance verify send value message checksum error: %v.\n", err)
			return
		}
		i.learner.onSendValue(msg)
		if i.learner.isLearned() {
			if err := i.execute(i.learner.getInstanceID(), i.learner.getValue()); err != nil {
				log.Errorf("Instance execute state machine error: %v.\n", err)
				i.proposer.cancelSkipPrepare()
				return
			}
			i.checksum = i.learner.getChecksum()
			i.newInstance()
			i.cpm.SetMaxChosenInstanceID(i.acceptor.getInstanceID())
		}
	case comm.PaxosMsgType_SendInstanceID:
		if err := i.verifyChecksum(msg); err != nil {
			log.Errorf("Instance verify send instance id message checksum error: %v.\n", err)
			return
		}
		i.learner.onSendInstanceID(msg)
	case comm.PaxosMsgType_AckSendValue:
		if err := i.verifyChecksum(msg); err != nil {
			log.Errorf("Instance verify ack send value message checksum error: %v.\n", err)
			return
		}
		i.learner.onAckSendValue(msg)
	case comm.PaxosMsgType_AskForCheckpoint:
		if err := i.verifyChecksum(msg); err != nil {
			log.Errorf("Instance verify ask for checkpoint message checksum error: %v.\n", err)
			return
		}
		i.learner.onAskForCheckpoint(msg)
	default:
		log.Errorf("Instance receive unknown paxos message type %v.\n", msg.GetType())
		return
	}
}

func (i *instance) ReceiveCheckpointMessage(msg *comm.CheckpointMsg) {
	switch msg.GetType() {
	case comm.CheckpointMsgType_SendFile:
		if !i.cpm.IsAskForCheckpoint() {
			log.Warning("Instance checkpoint manager not in ask for checkpoint.")
			return
		}
		i.learner.onSendCheckpoint(msg)
	case comm.CheckpointMsgType_AckSendFile:
		i.learner.onAckSendCheckpoint(msg)
	default:
		log.Errorf("Instance receive unknown checkpoint message type %v.\n", msg.GetType())
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
		return errChecksumFailed
	}
	return nil
}

func (i *instance) execute(instanceID uint64, v []byte) error {
	var smid uint32
	if err := comm.BytesToObject(v[:comm.SMIDLen], &smid); err != nil {
		return fmt.Errorf("convert bytes to sm id error: %v", err)
	}
	if smid == 0 {
		return nil
	}
	sm, ok := i.sms[smid]
	if !ok {
		return errors.New("smid not found")
	}
	var (
		ret interface{}
		err error
	)
	cid, ok := i.ctx.Value("instance_id").(uint64)
	if ok && cid == instanceID {
		done := make(chan struct{})
		go func() {
			ret, err = sm.Execute(i.ctx, instanceID, v[comm.SMIDLen:])
			close(done)
		}()

		select {
		case <-i.ctx.Done():
			return i.ctx.Err()
		case <-done:
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

func (i *instance) GetCheckpointInstanceID() uint64 {
	a, b, id := comm.UnknownInstanceID, comm.UnknownInstanceID, comm.UnknownInstanceID
	for _, sm := range i.sms {
		id = sm.GetCheckpointInstanceID()
		if id == comm.UnknownInstanceID {
			continue
		}
		if sm.GetStateMachineID() == comm.MasterStateMachineID ||
			sm.GetStateMachineID() == comm.SystemStateMachineID {
			if id > a || a == comm.UnknownInstanceID {
				a = id
			}
		} else {
			if id > b || b == comm.UnknownInstanceID {
				b = id
			}
		}
	}
	if b != comm.UnknownInstanceID {
		return b
	}
	return a
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

func (i *instance) SetMaxLogCount(c int64) {
	i.cpm.GetCleaner().SetMaxLogCount(c)
}

func (i *instance) GetProposerInstanceID() uint64 {
	return i.proposer.getInstanceID()
}

func (i *instance) ExecuteForCheckpoint(instanceID uint64, v []byte) error {
	var smid uint32
	if err := comm.BytesToObject(v[:comm.SMIDLen], &smid); err != nil {
		return fmt.Errorf("instance: convert bytes to smid error: %v", err)
	}
	sm, ok := i.sms[smid]
	if !ok {
		return errors.New("smid not found")
	}
	return sm.ExecuteForCheckpoint(instanceID, v[comm.SMIDLen:])
}

func (i *instance) lockCheckpointState() (err error) {
	locks := make([]comm.StateMachine, 0)
	defer func() {
		if err != nil {
			for _, sm := range locks {
				sm.UnlockCheckpointState()
			}
		}
	}()
	for _, sm := range i.sms {
		if err = sm.LockCheckpointState(); err != nil {
			log.Errorf("Lock sm %d checkpoint state error: %v.\n", sm.GetStateMachineID(), err)
			return
		}
		locks = append(locks, sm)
	}
	return
}

func (i *instance) unlockCheckpointState() {
	for _, sm := range i.sms {
		sm.UnlockCheckpointState()
	}
}

func (i *instance) getAllCheckpoint() ([]uint32, []uint64, []string, [][]string) {
	count := len(i.sms)
	smids := make([]uint32, count)
	iids := make([]uint64, count)
	dirs := make([]string, count)
	files := make([][]string, count)
	for i, sm := range i.sms {
		smids[i] = sm.GetStateMachineID()
		iids[i] = sm.GetCheckpointInstanceID()
		dirs[i], files[i] = sm.GetCheckpointState()
	}
	return smids, iids, dirs, files
}

type msgHeap []*comm.PaxosMsg

func (h msgHeap) Len() int {
	return len(h)
}
func (h msgHeap) Less(i, j int) bool {
	return h[i].GetInstanceID() < h[j].GetInstanceID()
}
func (h msgHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}
func (h *msgHeap) Push(x interface{}) {
	*h = append(*h, x.(*comm.PaxosMsg))
}
func (h *msgHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
