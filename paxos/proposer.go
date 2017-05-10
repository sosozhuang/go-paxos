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
	"github.com/gogo/protobuf/proto"
	"github.com/sosozhuang/paxos/comm"
	"github.com/sosozhuang/paxos/logger"
	"sync"
	"sync/atomic"
	"time"
)

type proposalID uint64
type msgType int
type Proposer interface {
	GetInstanceID() comm.InstanceID
	NewValue([]byte) *comm.PaxosMsg
}

var log = logger.ProposerLogger

const (
	prepare msgType = iota
	accept
)

const (
	startPrepareDuration = time.Second * 2
	startAcceptDuration  = time.Second
	maxPrepareDuration   = time.Second * 8
	maxAcceptDuration    = time.Second * 8
)

func NewProposer(pd, ad time.Duration, learner Learner) (Proposer, error) {
	state := proposerState{
		id:             proposalID(1),
		lastProposalID: proposalID(0),
	}
	return &proposer{
		state:                state,
		learner:              learner,
		prepareDuration:      pd,
		acceptDuration:       ad,
		receiveNodes:         make(map[comm.NodeID]bool),
		rejectNodes:          make(map[comm.NodeID]bool),
		promiseOrAcceptNodes: make(map[comm.NodeID]bool),
		prepareReplies:       make(map[proposalID]func(*comm.PaxosMsg)),
		acceptReplies:        make(map[proposalID]func(*comm.PaxosMsg)),
	}, nil
}

type proposer struct {
	state                proposerState
	learner              Learner
	preparing            bool
	accepting            bool
	skipPrepare          bool
	rejected             bool
	prepareDuration      time.Duration
	acceptDuration       time.Duration
	receiveNodes         map[comm.NodeID]struct{}
	rejectNodes          map[comm.NodeID]struct{}
	promiseOrAcceptNodes map[comm.NodeID]struct{}
	mu                   sync.Mutex
	prepareMu            sync.RWMutex
	prepareReplies       map[proposalID]func(*comm.PaxosMsg)
	acceptMu             sync.RWMutex
	acceptReplies        map[proposalID]func(*comm.PaxosMsg)
}

func (p *proposer) isWorking() bool {
	return p.preparing || p.accepting
}

func (p *proposer) NewValue(value []byte) {
	p.state.setValue(value)
	p.prepareDuration = startPrepareDuration
	p.acceptDuration = startAcceptDuration

	if p.skipPrepare && !p.rejected {
		p.accept()
	} else {
		p.prepare(p.rejected)
	}
}

func (p *proposer) prepare(rejected bool) {
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
		MsgType:    proto.Int(prepare),
		InstanceID: proto.Uint64(""),
		NodeID:     proto.Uint64(""),
		ProposalID: proto.Uint64(p.state.id),
	}

	go p.broadcastPrepareMessage(msg)
}

func (p *proposer) majorityCount() int {
	return 3
}

func (p *proposer) broadcastPrepareMessage(msg *comm.PaxosMsg) {
	ac, rc := make(chan struct{}, p.majorityCount()), make(chan struct{}, p.majorityCount())
	p.prepareMu.Lock()
	p.prepareReplies[msg.ProposalID] = func(msg *comm.PaxosMsg) {
		if !p.preparing {
			return
		}
		if msg.ProposalID != p.state.id {
			return
		}
		p.addReceiveNode(msg.NodeID)
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
		} else if p.isRejected() || p.isAllReceived() {
			rc <- struct{}{}
		}
	}
	p.prepareMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), p.prepareDuration)
	defer func() {
		cancel()
		close(ac)
		close(rc)
		p.prepareMu.Lock()
		delete(p.prepareReplies, msg.ProposalID)
		p.prepareMu.Unlock()
	}()

	p.i.onPrepare(id)

	select {
	case <-ctx.Done():
		//timeout
		log.Warning(ctx.Err())
		if p.prepareDuration < maxPrepareDuration {
			p.prepareDuration *= 2
		}
		go p.prepare(p.rejected)
	case <-ac:
		//accept
		p.skipPrepare = true
		p.accept()
	case <-rc:
		//reject
		if p.prepareDuration < maxPrepareDuration {
			p.prepareDuration *= 2
		}
		go func() {
			time.Sleep(time.Millisecond * 30)
			p.prepare(p.rejected)
		}()
	}
}

func (p *proposer) onPrepareReply(msg *comm.PaxosMsg) {
	defer p.prepareMu.RLock()
	defer p.prepareMu.RUnlock()
	if f, ok := p.prepareReplies[msg.GetProposalID()]; ok {
		f(msg)
	}
}

func (p *proposer) startNewRound() {
	defer p.mu.Lock()
	defer p.mu.Unlock()
	p.receiveNodes = make(map[comm.NodeID]struct{})
	p.rejectNodes = make(map[comm.NodeID]struct{})
	p.promiseOrAcceptNodes = make(map[comm.NodeID]struct{})
}

func (p *proposer) addReceiveNode(nodeID comm.NodeID) {
	defer p.mu.Lock()
	defer p.mu.Unlock()
	p.receiveNodes[nodeID] = struct{}{}
}

func (p *proposer) addRejectNode(nodeID comm.NodeID) {
	defer p.mu.Lock()
	defer p.mu.Unlock()
	p.rejectNodes[nodeID] = struct{}{}
}

func (p *proposer) addPromiseOrAcceptNode(nodeID comm.NodeID) {
	defer p.mu.Lock()
	defer p.mu.Unlock()
	p.promiseOrAcceptNodes[nodeID] = struct{}{}
}

func (p *proposer) isPassed() bool {
	defer p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.promiseOrAcceptNodes) == 0
}

func (p *proposer) isRejected() bool {
	defer p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.rejectNodes) == 0
}

func (p *proposer) isAllReceived() bool {
	defer p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.receiveNodes) == 0
}

func (p *proposer) accept() {
	p.preparing = false
	p.accepting = true

	p.startNewRound()
	msg := &comm.PaxosMsg{
		MsgType:    proto.Int(accept),
		InstanceID: proto.Uint64(""),
		NodeID:     proto.Uint64(""),
		ProposalID: proto.Uint64(p.state.id),
	}

	go p.broadcastAcceptMessage(msg)
}

func (p *proposer) broadcastAcceptMessage(msg *comm.PaxosMsg) {
	ac, rc := make(chan struct{}, p.majorityCount()), make(chan struct{}, p.majorityCount())
	p.acceptMu.Lock()
	p.acceptReplies[msg.ProposalID] = func(msg *comm.PaxosMsg) {
		if !p.accepting {
			return
		}
		if msg.ProposalID != p.state.id {
			return
		}
		p.addReceiveNode(msg.NodeID)
		if msg.GetRejectByPromiseID() == 0 {
			p.addPromiseOrAcceptNode(msg.NodeID)
		} else {
			p.addRejectNode(msg.NodeID)
			p.rejected = true
			p.state.SetLastPropalID(msg.GetRejectByPromiseID())
		}
		if p.isPassed() {
			ac <- struct{}{}
		} else if p.isRejected() || p.isAllReceived() {
			rc <- struct{}{}
		}
	}
	p.acceptMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), p.prepareDuration)
	defer func() {
		cancel()
		close(ac)
		close(rc)
		p.acceptMu.Lock()
		delete(p.acceptReplies, msg.ProposalID)
		p.acceptMu.Unlock()
	}()

	p.i.onPrepare(id)

	select {
	case <-ctx.Done():
		//timeout
		log.Warning(ctx.Err())
		if p.acceptDuration < maxAcceptDuration {
			p.acceptDuration *= 2
		}
		go p.prepare(p.rejected)
	case <-ac:
		//accept
		p.accepting = false
		p.accept()
	case <-rc:
		//reject
		if p.acceptDuration < maxAcceptDuration {
			p.acceptDuration *= 2
		}
		go func() {
			time.Sleep(time.Millisecond * 30)
			p.prepare(p.rejected)
		}()
	}
}

func (p *proposer) onAcceptReply(msg *comm.PaxosMsg) {
	defer p.acceptMu.Lock()
	defer p.acceptMu.Unlock()
	if f, ok := p.acceptReplies[msg.GetProposalID()]; ok {
		f(msg)
	}
}

type proposerState struct {
	ballot         ballot
	id             proposalID
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
	if s.id < s.lastProposalID {
		s.id = s.lastProposalID
	}
	atomic.AddUint64(&s.id, 1)
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
