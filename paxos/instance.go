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
	"sync"
	"sync/atomic"
	"time"
)

var (
	log               = logger.GetLogger("instance")
	errChecksumFailed = errors.New("checksum failed")
)

type Instance interface {
	getNodeID() uint64
	getChecksum() uint32
	getMemberCount() int
	getMajorityCount() int
	exitNewValue()
	NewValue(context.Context)
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
	wg       sync.WaitGroup
	token    chan struct{}
}

func NewInstance(groupCfg comm.GroupConfig, sender comm.Sender, st store.Storage) (comm.Instance, error) {
	var err error
	i := &instance{
		groupCfg: groupCfg,
		st:       st,
		sms:      make(map[uint32]comm.StateMachine),
		ctx:      context.TODO(),
		ch:       make(chan *comm.PaxosMsg, 10),
		retries:  &msgHeap{},
		token:    make(chan struct{}, 1),
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
			log.Debugf("Instance of group %d started.", i.groupCfg.GetGroupID())
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
	i.wg.Add(1)
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
	i.proposer.start(stopped)

	return nil
}

func (i *instance) Stop() {
	if i.proposer != nil {
		i.proposer.stop()
	}
	if i.cpm != nil {
		i.cpm.Stop()
	}
	if i.learner != nil {
		i.learner.stop()
	}
	i.wg.Wait()
	log.Debugf("Instance of group %d stopped.", i.groupCfg.GetGroupID())
}

func (i *instance) getNodeID() uint64 {
	return i.groupCfg.GetNodeID()
}

func (i *instance) getMajorityCount() int {
	return i.groupCfg.GetMajorityCount()
}

func (i *instance) getMemberCount() int {
	return i.groupCfg.GetMemberCount()
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
		return fmt.Errorf("instance: unmarshal acceptor state: %v", err)
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
			return fmt.Errorf("instance: unmarshal acceptor state: %v", err)
		}
		if err = i.execute(id, state.GetAcceptedValue(), false); err != nil {
			return err
		}
	}
	return nil
}

func (i *instance) getChecksum() uint32 {
	return atomic.LoadUint32(&i.checksum)
}

func (i *instance) GetCleaner() checkpoint.Cleaner {
	return i.cpm.GetCleaner()
}

func (i *instance) GetReplayer() checkpoint.Replayer {
	return i.cpm.GetReplayer()
}

func (i *instance) IsReadyForNewValue() bool {
	if !i.learner.isReadyForNewValue() {
		return false
	}
	select {
	case i.token <- struct{}{}:
		return true
	default:
		return false
	}
}

func (i *instance) GetInstanceID() uint64 {
	return i.acceptor.getInstanceID()
}

func (i *instance) newInstance() {
	i.proposer.newInstance()
	i.acceptor.newInstance()
	i.learner.newInstance()
	i.exitNewValue()
}

func (i *instance) NewValue(ctx context.Context) {
	i.ctx = context.WithValue(ctx, "instance_id", i.proposer.getInstanceID())
	i.proposer.newValue(i.ctx)
}

func (i *instance) exitNewValue() {
	//select {
	//case <-i.token:
	//default:
	//}
	fmt.Println("exitNewValue",)
	<- i.token
}

func (i *instance) AddStateMachine(sms ...comm.StateMachine) {
	for _, sm := range sms {
		if _, ok := i.sms[sm.GetStateMachineID()]; !ok {
			i.sms[sm.GetStateMachineID()] = sm
		} else {
			log.Warningf("State machine id %d already exists.", sm.GetStateMachineID())
		}
	}
}

func (i *instance) handleRetry(stopped <-chan struct{}) {
	ticker := time.NewTicker(time.Millisecond * 30)
	defer func() {
		ticker.Stop()
		i.wg.Done()
	}()
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
			log.Debugf("Going to retry message, instance id %d.", msg.GetInstanceID())
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
			log.Errorf("Verify prepare message error: %v.", err)
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
			log.Errorf("Verify accept message error: %v.", err)
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
			log.Errorf("Verify ask for learn message error: %v.", err)
			return
		}
		i.learner.onAskForLearn(msg)
	case comm.PaxosMsgType_ConfirmAskForLearn:
		if err := i.verifyChecksum(msg); err != nil {
			log.Errorf("Verify confirm ask for learn message error: %v.", err)
			return
		}
		i.learner.onConfirmAskForLearn(msg)
	case comm.PaxosMsgType_ProposalFinished:
		if err := i.verifyChecksum(msg); err != nil {
			log.Errorf("Verify proposal finished message error: %v.", err)
			return
		}
		i.learner.onProposalFinished(msg)
		if i.learner.isLearned() {
			//defer i.exitNewValue()
			if err := i.execute(i.learner.getInstanceID(), i.learner.getValue(), msg.GetNodeID() == i.getNodeID()); err != nil {
				log.Errorf("Execute state machine error: %v.", err)
				i.proposer.cancelSkipPrepare()
				return
			}
			atomic.StoreUint32(&i.checksum, i.learner.getChecksum())
			//i.checksum = i.learner.getChecksum()
			i.newInstance()
			i.cpm.SetMaxChosenInstanceID(i.acceptor.getInstanceID())
		}
	case comm.PaxosMsgType_SendValue:
		if err := i.verifyChecksum(msg); err != nil {
			log.Errorf("Verify send value message error: %v.", err)
			return
		}
		i.learner.onSendValue(msg)
		if i.learner.isLearned() {
			if err := i.execute(i.learner.getInstanceID(), i.learner.getValue(), false); err != nil {
				log.Errorf("Execute state machine error: %v.", err)
				i.proposer.cancelSkipPrepare()
				return
			}
			atomic.StoreUint32(&i.checksum, i.learner.getChecksum())
			//i.checksum = i.learner.getChecksum()
			i.newInstance()
			i.cpm.SetMaxChosenInstanceID(i.acceptor.getInstanceID())
		}
	case comm.PaxosMsgType_SendInstanceID:
		if err := i.verifyChecksum(msg); err != nil {
			log.Errorf("Verify send instance id message error: %v.", err)
			return
		}
		i.learner.onSendInstanceID(msg)
	case comm.PaxosMsgType_AckSendValue:
		if err := i.verifyChecksum(msg); err != nil {
			log.Errorf("Verify ack send value message error: %v.", err)
			return
		}
		i.learner.onAckSendValue(msg)
	case comm.PaxosMsgType_AskForCheckpoint:
		if err := i.verifyChecksum(msg); err != nil {
			log.Errorf("Verify ask for checkpoint message error: %v.", err)
			return
		}
		i.learner.onAskForCheckpoint(msg)
	default:
		log.Errorf("Receive unknown paxos message type %v.", msg.GetType())
		return
	}
}

func (i *instance) ReceiveCheckpointMessage(msg *comm.CheckpointMsg) {
	switch msg.GetType() {
	case comm.CheckpointMsgType_SendFile:
		if !i.cpm.IsAskForCheckpoint() {
			log.Warning("Checkpoint manager not in ask for checkpoint.")
			return
		}
		i.learner.onSendCheckpoint(msg)
	case comm.CheckpointMsgType_AckSendFile:
		i.learner.onAckSendCheckpoint(msg)
	default:
		log.Errorf("Receive unknown checkpoint message type %v.", msg.GetType())
		return
	}
}

func (i *instance) verifyChecksum(msg *comm.PaxosMsg) error {
	if msg.GetChecksum() == 0 ||
		msg.GetInstanceID() != i.acceptor.getInstanceID() {
		return nil
	}
	if i.acceptor.getInstanceID() > 0 && i.getChecksum() == 0 {
		atomic.StoreUint32(&i.checksum, msg.GetChecksum())
		//i.checksum = msg.GetChecksum()
		return nil
	}
	if msg.GetChecksum() != i.getChecksum() {
		return errChecksumFailed
	}
	return nil
}

func (i *instance) execute(instanceID uint64, v []byte, local bool) error {
	var smid uint32
	if err := comm.BytesToObject(v[:comm.SMIDLen], &smid); err != nil {
		return fmt.Errorf("convert bytes to sm id: %v", err)
	}
	if smid == 0 {
		return nil
	}
	sm, ok := i.sms[smid]
	if !ok {
		return fmt.Errorf("smid %d not found", smid)
	}
	var (
		ret interface{}
		err error
	)
	cid, ok := i.ctx.Value("instance_id").(uint64)
	if local && ok && cid == instanceID {
		log.Debugf("Executing state machine %d, instance id %d.", smid, instanceID)
		done := make(chan struct{})
		i.wg.Add(1)
		go func() {
			defer i.wg.Done()
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
			return nil
		}
	} else {
		log.Debugf("Executing state machine %d, instance id %d, recevied from remote.", smid, instanceID)
		sm.Execute(context.Background(), instanceID, v[comm.SMIDLen:])
		return nil
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
		return fmt.Errorf("instance: convert bytes to smid: %v", err)
	}
	sm, ok := i.sms[smid]
	if !ok {
		return fmt.Errorf("smid %d not found", smid)
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
			log.Errorf("Lock sm %d checkpoint state error: %v.", sm.GetStateMachineID(), err)
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
	n := 0
	for _, sm := range i.sms {
		smids[n] = sm.GetStateMachineID()
		iids[n] = sm.GetCheckpointInstanceID()
		dirs[n], files[n] = sm.GetCheckpointState()
		n++
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
