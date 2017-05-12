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
	"fmt"
	"github.com/Masterminds/glide/msg"
	"github.com/gogo/protobuf/proto"
	"github.com/sosozhuang/paxos/comm"
	"github.com/sosozhuang/paxos/logger"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

type proposalID uint64
type Reply func(*comm.PaxosMsg)
type Proposer interface {
	NewInstance()
	GetInstanceID() comm.InstanceID
	SetInstanceID(comm.InstanceID)
	NewValue([]byte) *comm.PaxosMsg
	OnPrepareReply(*comm.PaxosMsg)
	OnAcceptReply(*comm.PaxosMsg)
}

const (
	plog                = logger.ProposerLogger
	startPrepareTimeout = time.Second * 2
	startAcceptTimeout  = time.Second
	maxPrepareTimeout   = time.Second * 8
	maxAcceptTimeout    = time.Second * 8
)

func NewProposer(pd, ad time.Duration, learner Learner) (Proposer, error) {
	state := proposerState{
		proposalID:     proposalID(1),
		lastProposalID: proposalID(0),
	}
	return &proposer{
		state:          state,
		learner:        learner,
		prepareTimeout: pd,
		acceptTimeout:  ad,
		//receiveNodes:         make(map[comm.NodeID]bool),
		rejectNodes:          make(map[comm.NodeID]bool),
		promiseOrAcceptNodes: make(map[comm.NodeID]bool),
		prepareReplies:       make(map[proposalID]Reply),
		acceptReplies:        make(map[proposalID]Reply),
	}, nil
}

type proposer struct {
	state          proposerState
	nodeCount      int
	nodeID         comm.NodeID
	instanceID     comm.InstanceID
	learner        Learner
	preparing      bool
	accepting      bool
	skipPrepare    bool
	rejected       bool
	prepareTimeout time.Duration
	acceptTimeout  time.Duration
	//receiveNodes         map[comm.NodeID]struct{}
	rejectNodes          map[comm.NodeID]struct{}
	promiseOrAcceptNodes map[comm.NodeID]struct{}
	mu                   sync.Mutex
	prepareMu            sync.RWMutex
	prepareReplies       map[proposalID]Reply
	acceptMu             sync.RWMutex
	acceptReplies        map[proposalID]Reply
	tp                   Transporter
}

func (p *proposer) isWorking() bool {
	return p.preparing || p.accepting
}

func (p *proposer) GetInstanceID() comm.InstanceID {
	return p.instanceID
}

func (p *proposer) SetInstanceID(instanceID comm.InstanceID) {
	p.instanceID = instanceID
}

func (p *proposer) NewValue(ctx context.Context, stopped <-chan struct{}, errc chan<- error, value []byte) {
	if v, ok := ctx.Value("").([]byte); ok {
		p.state.setValue(v)
	}
	p.state.setValue(value)
	p.prepareTimeout = startPrepareTimeout
	p.acceptTimeout = startAcceptTimeout

	if p.skipPrepare && !p.rejected {
		p.accept(ctx, stopped, errc)
	} else {
		p.prepare(ctx, stopped, errc, p.rejected)
	}
	select {
	case <-ctx.Done():
		p.preparing = false
		p.accepting = false
	case <-stopped:
	}
}

func (p *proposer) prepare(ctx context.Context, stopped <-chan struct{}, errc chan<- error, rejected bool) {
	p.preparing = true
	p.accepting = false
	p.skipPrepare = false
	p.rejected = false

	p.state.resetBallot()
	if rejected {
		p.state.newState()
	}

	p.startNewRound()
	msg := &comm.PaxosMsg{
		Type:       comm.PaxosMsgType_Prepare.Enum(),
		InstanceID: proto.Uint64(p.instanceID),
		NodeID:     proto.Uint64(p.nodeID),
		ProposalID: proto.Uint64(p.state.proposalID),
	}

	select {
	case <-ctx.Done():
	default:
		go p.broadcastPrepareMessage(ctx, stopped, errc, msg)
	}
}

func (p *proposer) majorityCount() int {
	return p.nodeCount/2 + 1
}

func (p *proposer) broadcastPrepareMessage(ctx context.Context, stopped <-chan struct{}, errc chan<- error, msg *comm.PaxosMsg) {
	ac, rc := make(chan struct{}, p.majorityCount()), make(chan struct{}, p.majorityCount())
	p.prepareMu.Lock()
	p.prepareReplies[msg.ProposalID] = func(msg *comm.PaxosMsg) {
		if !p.preparing {
			return
		}
		if msg.ProposalID != p.state.proposalID {
			return
		}
		//p.addReceiveNode(msg.NodeID)
		if msg.GetRejectByPromiseID() == 0 {
			b := ballot{msg.GetPreAcceptID(), msg.GetPreAcceptNodeID()}
			p.addPromiseOrAcceptNode(msg.NodeID)
			p.state.setPreAcceptValue(b, msg.GetValue())
		} else {
			p.addRejectNode(msg.NodeID)
			p.rejected = true
			p.state.SetLastPropalID(msg.GetRejectByPromiseID())
		}
		if p.isPassed() {
			ac <- struct{}{}
		} else if p.isRejected() {
			rc <- struct{}{}
		}
	}
	p.prepareMu.Unlock()

	if deadline, ok := ctx.Deadline(); ok {
		d := deadline.Sub(time.Now())
		if d < p.prepareTimeout {
			p.prepareTimeout = d
		}
	}
	pctx, cancel := context.WithTimeout(context.Background(), p.prepareTimeout)
	defer func() {
		cancel()
		close(ac)
		close(rc)
		p.prepareMu.Lock()
		delete(p.prepareReplies, msg.ProposalID)
		p.prepareMu.Unlock()
	}()

	p.tp.Broadcast()

	select {
	case <-ctx.Done():
		return
	case <-pctx.Done():
		//timeout
		plog.Warning(pctx.Err())
		if p.prepareTimeout < maxPrepareTimeout {
			p.prepareTimeout *= 2
		}
		go p.prepare(ctx, stopped, errc, p.rejected)
	case <-ac:
		//accept
		p.skipPrepare = true
		p.accept(ctx, stopped, errc)
	case <-rc:
		//reject
		go func() {
			time.Sleep(time.Millisecond * 30)
			p.prepare(ctx, stopped, errc, p.rejected)
		}()
	}
}

func (p *proposer) OnPrepareReply(msg *comm.PaxosMsg) {
	p.prepareMu.RLock()
	defer p.prepareMu.RUnlock()
	if f, ok := p.prepareReplies[msg.GetProposalID()]; ok {
		f(msg)
	}
}

func (p *proposer) startNewRound() {
	p.mu.Lock()
	defer p.mu.Unlock()
	//p.receiveNodes = make(map[comm.NodeID]struct{})
	p.rejectNodes = make(map[comm.NodeID]struct{})
	p.promiseOrAcceptNodes = make(map[comm.NodeID]struct{})
}

//func (p *proposer) addReceiveNode(nodeID comm.NodeID) {
//	p.mu.Lock()
//	defer p.mu.Unlock()
//	p.receiveNodes[nodeID] = struct{}{}
//}

func (p *proposer) addRejectNode(nodeID comm.NodeID) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rejectNodes[nodeID] = struct{}{}
	//p.receiveNodes[nodeID] = struct{}{}
}

func (p *proposer) addPromiseOrAcceptNode(nodeID comm.NodeID) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.promiseOrAcceptNodes[nodeID] = struct{}{}
	//p.receiveNodes[nodeID] = struct{}{}
}

func (p *proposer) isPassed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.promiseOrAcceptNodes) >= p.majorityCount()
}

func (p *proposer) isRejected() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.rejectNodes) >= p.majorityCount()
}

//func (p *proposer) isAllReceived() bool {
//	p.mu.Lock()
//	defer p.mu.Unlock()
//	return len(p.receiveNodes) == 0
//}

func (p *proposer) accept(ctx context.Context, stopped <-chan struct{}, errc chan<- error) {
	p.preparing = false
	p.accepting = true

	p.startNewRound()
	msg := &comm.PaxosMsg{
		Type:         comm.PaxosMsgType_Accept.Enum(),
		InstanceID:   proto.Uint64(p.instanceID),
		NodeID:       proto.Uint64(p.nodeID),
		ProposalID:   proto.Uint64(p.state.proposalID),
		Value:        p.state.value,
		LastChecksum: proto.Uint32(0),
	}

	select {
	case <-ctx.Done():
	default:
		go p.broadcastAcceptMessage(ctx, stopped, errc, msg)
	}
}

func (p *proposer) broadcastAcceptMessage(ctx context.Context, stopped <-chan struct{}, errc chan<- error, msg *comm.PaxosMsg) {
	ac, rc := make(chan struct{}, p.majorityCount()), make(chan struct{}, p.majorityCount())
	p.acceptMu.Lock()
	p.acceptReplies[msg.ProposalID] = func(msg *comm.PaxosMsg) {
		if !p.accepting {
			return
		}
		if msg.ProposalID != p.state.proposalID {
			return
		}
		//p.addReceiveNode(msg.NodeID)
		if msg.GetRejectByPromiseID() == 0 {
			p.addPromiseOrAcceptNode(msg.NodeID)
		} else {
			p.addRejectNode(msg.NodeID)
			p.rejected = true
			p.state.SetLastPropalID(msg.GetRejectByPromiseID())
		}
		if p.isPassed() {
			ac <- struct{}{}
		} else if p.isRejected() {
			rc <- struct{}{}
		}
	}
	p.acceptMu.Unlock()

	if deadline, ok := ctx.Deadline(); ok {
		d := deadline.Sub(time.Now())
		if d < p.acceptTimeout {
			p.acceptTimeout = d
		}
	}
	pctx, cancel := context.WithTimeout(context.Background(), p.acceptTimeout)
	defer func() {
		cancel()
		close(ac)
		close(rc)
		p.acceptMu.Lock()
		delete(p.acceptReplies, msg.ProposalID)
		p.acceptMu.Unlock()
	}()

	p.tp.Broadcast()

	select {
	case <-ctx.Done():
		return
	case <-pctx.Done():
		//timeout
		plog.Warning(pctx.Err())
		if p.acceptTimeout < maxAcceptTimeout {
			p.acceptTimeout *= 2
		}
		if deadline, ok := ctx.Deadline(); ok {
			d := deadline.Sub(time.Now())
			if d < p.acceptTimeout {
				p.acceptTimeout = d
			}
		}
		go p.prepare(ctx, stopped, errc, p.rejected)
	case <-ac:
		//accept
		p.accepting = false
		close(stopped)
		p.learner.ProposerSendSuccess(p.instanceID, msg.GetProposalID())
	case <-rc:
		//reject
		go func() {
			time.Sleep(time.Millisecond * 30)
			p.prepare(ctx, stopped, errc, p.rejected)
		}()
	}
}

func (p *proposer) OnAcceptReply(msg *comm.PaxosMsg) {
	p.acceptMu.RLock()
	defer p.acceptMu.RUnlock()
	if f, ok := p.acceptReplies[msg.GetProposalID()]; ok {
		f(msg)
	}
}

type proposerState struct {
	ballot
	proposalID
	lastProposalID proposalID
	value          []byte
}

func (s *proposerState) setValue(value []byte) {
	if s.value == nil || len(s.value) == 0 {
		s.value = value
	}
}

func (s *proposerState) setPreAcceptValue(b ballot, value []byte) {
	if !b.valid() {
		return
	}
	if b.gt(s.ballot) {
		s.ballot = b
		s.value = value
	}
}

func (s *proposerState) SetLastPropalID(id proposalID) {
	if id > s.lastProposalID {
		s.lastProposalID = id
	}
}

func (s *proposerState) newState() {
	if s.proposalID < s.lastProposalID {
		s.proposalID = s.lastProposalID
	}
	atomic.AddUint64(&s.proposalID, 1)
}

func (s *proposerState) resetBallot() {
	s.ballot.reset()
}

type ballot struct {
	proposalID proposalID
	nodeID     comm.NodeID
}

func (b *ballot) reset() {
	b.proposalID = proposalID(0)
	b.nodeID = comm.NodeID(0)
}

func (b *ballot) ge(o ballot) {
	if b.proposalID == o.proposalID {
		return b.nodeID >= o.nodeID
	}
	return b.proposalID >= o.proposalID
}

func (b *ballot) gt(o ballot) {
	if b.proposalID == o.proposalID {
		return b.nodeID > o.nodeID
	}
	return b.proposalID > o.proposalID
}

func (b *ballot) eq(o ballot) {
	return b.proposalID == o.proposalID && b.nodeID == o.nodeID
}

func (b *ballot) ne(o ballot) {
	return b.proposalID != o.proposalID || b.nodeID != b.nodeID
}

func (b *ballot) le(o ballot) {
	if b.proposalID == o.proposalID {
		return b.nodeID <= o.nodeID
	}
	return b.proposalID <= o.proposalID
}

func (b *ballot) lt(o ballot) {
	if b.proposalID == o.proposalID {
		return b.nodeID < o.nodeID
	}
	return b.proposalID < o.proposalID
}

func (b *ballot) valid() bool {
	return b.proposalID != 0
}
