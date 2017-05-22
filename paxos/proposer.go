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
	"github.com/sosozhuang/paxos/comm"
	"github.com/sosozhuang/paxos/logger"
	"sync"
	"sync/atomic"
	"time"
)

type Reply func(*comm.PaxosMsg)
type Proposer interface {
	newInstance()
	getInstanceID() uint64
	setInstanceID(uint64)
	setProposalID(uint64)
	newValue(context.Context)
	onPrepareReply(*comm.PaxosMsg)
	onAcceptReply(*comm.PaxosMsg)
	cancelSkipPrepare()
}

type proposer struct {
	state                proposerState
	nodeID               uint64
	instance             comm.Instance
	instanceID           uint64
	learner              Learner
	preparing            bool
	accepting            bool
	skipPrepare          bool
	rejected             bool
	prepareTimeout       time.Duration
	acceptTimeout        time.Duration
	rejectNodes          map[uint64]struct{}
	promiseOrAcceptNodes map[uint64]struct{}
	mu                   sync.Mutex
	prepareMu            sync.RWMutex
	prepareReplies       map[uint64]Reply
	acceptMu             sync.RWMutex
	acceptReplies        map[uint64]Reply
	tp                   Transporter
}

var (
	plog = logger.ProposerLogger
)

const (
	startPrepareTimeout = time.Second * 2
	startAcceptTimeout  = time.Second
	maxPrepareTimeout   = time.Second * 8
	maxAcceptTimeout    = time.Second * 8
)

func newProposer(nodeID uint64, instance comm.Instance, tp Transporter, learner Learner) Proposer {
	state := proposerState{
		proposalID:     1,
		lastProposalID: 0,
	}
	return &proposer{
		state:                state,
		nodeID:               nodeID,
		instance:             instance,
		learner:              learner,
		rejectNodes:          make(map[uint64]struct{}),
		promiseOrAcceptNodes: make(map[uint64]struct{}),
		prepareReplies:       make(map[uint64]Reply),
		acceptReplies:        make(map[uint64]Reply),
		tp:                   tp,
	}
}

func (p *proposer) newInstance() {
	atomic.AddUint64(&p.instanceID, 1)
	p.startNewRound()
	p.state.reset()
	p.preparing = false
	p.accepting = false
}

func (p *proposer) setProposalID(proposalID uint64) {
	p.state.setProposalID(proposalID)
}

func (p *proposer) getInstanceID() uint64 {
	return p.instanceID
}

func (p *proposer) setInstanceID(id uint64) {
	p.instanceID = id
}

func (p *proposer) newValue(ctx context.Context) {
	value, ok := ctx.Value("value").([]byte)
	if !ok {
		plog.Error("")
		return
	}
	p.state.setValue(value)
	p.prepareTimeout = startPrepareTimeout
	p.acceptTimeout = startAcceptTimeout

	done := make(chan struct{})
	if p.skipPrepare && !p.rejected {
		p.accept(ctx, done)
	} else {
		p.prepare(ctx, done, p.rejected)
	}
	select {
	case <-ctx.Done():
		p.preparing = false
		p.accepting = false
	case <-done:
	}
}

func (p *proposer) prepare(ctx context.Context, done chan struct{}, rejected bool) {
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
		go p.broadcastPrepareMessage(ctx, done, msg)
	}
}

func (p *proposer) getMajorityCount() int {
	return p.instance.GetMajorityCount()
}

func (p *proposer) broadcastPrepareMessage(ctx context.Context, done chan struct{}, msg *comm.PaxosMsg) {
	size := p.getMajorityCount()
	ac, rc := make(chan struct{}, size), make(chan struct{}, size)
	p.prepareMu.Lock()
	p.prepareReplies[msg.GetProposalID()] = func(msg *comm.PaxosMsg) {
		if !p.preparing {
			return
		}
		if msg.GetProposalID() != p.state.proposalID {
			return
		}
		p.mu.Lock()
		defer p.mu.Unlock()
		if msg.GetRejectByPromiseID() == 0 {
			p.addPromiseOrAcceptNode(msg.GetNodeID())
			b := ballot{msg.GetPreAcceptID(), msg.GetPreAcceptNodeID()}
			p.state.setPreAcceptValue(b, msg.GetValue())
		} else {
			p.addRejectNode(msg.GetNodeID())
			p.rejected = true
			p.state.SetLastProposalID(msg.GetRejectByPromiseID())
		}
		if p.isPassed() {
			ac <- struct{}{}
		} else if p.isRejected() {
			rc <- struct{}{}
		}
	}
	p.prepareMu.Unlock()

	if deadline, ok := ctx.Deadline(); ok {
		if d := deadline.Sub(time.Now()); d < p.prepareTimeout {
			p.prepareTimeout = d
		}
	}
	pctx, cancel := context.WithTimeout(context.Background(), p.prepareTimeout)
	defer func() {
		cancel()
		close(ac)
		close(rc)
		p.prepareMu.Lock()
		delete(p.prepareReplies, msg.GetProposalID())
		p.prepareMu.Unlock()
	}()

	go func() {
		p.instance.ReceivePaxosMessage(msg)
		p.tp.broadcast(comm.MsgType_Paxos, msg)
	}()

	select {
	case <-ctx.Done():
		return
	case <-pctx.Done():
		//timeout
		plog.Warning(pctx.Err())
		if p.prepareTimeout < maxPrepareTimeout {
			p.prepareTimeout *= 2
		}
		go p.prepare(ctx, done, p.rejected)
	case <-ac:
		//accept
		p.skipPrepare = true
		p.accept(ctx, done)
	case <-rc:
		//reject
		go func() {
			time.Sleep(time.Millisecond * 30)
			p.prepare(ctx, done, p.rejected)
		}()
	}
}

func (p *proposer) onPrepareReply(msg *comm.PaxosMsg) {
	if msg.GetInstanceID() != p.instanceID {
		log.Warningf("%d%d\n", msg.GetInstanceID(), p.instanceID)
		return
	}
	p.prepareMu.RLock()
	defer p.prepareMu.RUnlock()
	if f, ok := p.prepareReplies[msg.GetProposalID()]; ok {
		f(msg)
	}
}

func (p *proposer) startNewRound() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rejectNodes = make(map[uint64]struct{})
	p.promiseOrAcceptNodes = make(map[uint64]struct{})
}

func (p *proposer) addRejectNode(id uint64) {
	p.rejectNodes[id] = struct{}{}
}

func (p *proposer) addPromiseOrAcceptNode(id uint64) {
	p.promiseOrAcceptNodes[id] = struct{}{}
}

func (p *proposer) isPassed() bool {
	return len(p.promiseOrAcceptNodes) >= p.getMajorityCount()
}

func (p *proposer) isRejected() bool {
	return len(p.rejectNodes) >= p.getMajorityCount()
}

func (p *proposer) accept(ctx context.Context, done chan struct{}) {
	p.preparing = false
	p.accepting = true

	p.startNewRound()
	msg := &comm.PaxosMsg{
		Type:       comm.PaxosMsgType_Accept.Enum(),
		InstanceID: proto.Uint64(p.instanceID),
		NodeID:     proto.Uint64(p.nodeID),
		ProposalID: proto.Uint64(p.state.proposalID),
		Value:      p.state.value,
		Checksum:   proto.Uint32(p.instance.GetChecksum()),
	}

	select {
	case <-ctx.Done():
	default:
		go p.broadcastAcceptMessage(ctx, done, msg)
	}
}

func (p *proposer) broadcastAcceptMessage(ctx context.Context, done chan struct{}, msg *comm.PaxosMsg) {
	size := p.getMajorityCount()
	ac, rc := make(chan struct{}, size), make(chan struct{}, size)
	p.acceptMu.Lock()
	p.acceptReplies[msg.GetProposalID()] = func(msg *comm.PaxosMsg) {
		if !p.accepting {
			return
		}
		if msg.GetProposalID() != p.state.proposalID {
			return
		}
		p.mu.Lock()
		defer p.mu.Unlock()
		if msg.GetRejectByPromiseID() == 0 {
			p.addPromiseOrAcceptNode(msg.GetNodeID())
		} else {
			p.addRejectNode(msg.GetNodeID())
			p.rejected = true
			p.state.SetLastProposalID(msg.GetRejectByPromiseID())
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
		delete(p.acceptReplies, msg.GetProposalID())
		p.acceptMu.Unlock()
	}()

	go func() {
		p.tp.broadcast(comm.MsgType_Paxos, msg)
		p.instance.ReceivePaxosMessage(msg)
	}()

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
		go p.prepare(ctx, done, p.rejected)
	case <-ac:
		//accept
		p.accepting = false
		p.learner.proposalFinished(p.instanceID, msg.GetProposalID())
		close(done)
	case <-rc:
		//reject
		go func() {
			time.Sleep(time.Millisecond * 30)
			p.prepare(ctx, done, p.rejected)
		}()
	}
}

func (p *proposer) onAcceptReply(msg *comm.PaxosMsg) {
	if msg.GetInstanceID() != p.instanceID {
		log.Warningf("%d%d\n", msg.GetInstanceID(), p.instanceID)
		return
	}
	p.acceptMu.RLock()
	defer p.acceptMu.RUnlock()
	if f, ok := p.acceptReplies[msg.GetProposalID()]; ok {
		f(msg)
	}
}

func (p *proposer) cancelSkipPrepare() {
	p.skipPrepare = false
}

type proposerState struct {
	ballot
	proposalID     uint64
	lastProposalID uint64
	value          []byte
}

func (s *proposerState) reset() {
	s.lastProposalID = 0
	s.value = nil
}

func (s *proposerState) setProposalID(proposalID uint64) {
	s.proposalID = proposalID
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

func (s *proposerState) SetLastProposalID(id uint64) {
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
	proposalID uint64
	nodeID     uint64
}

func (b *ballot) reset() {
	b.proposalID = 0
	b.nodeID = 0
}

func (b ballot) ge(o ballot) bool {
	if b.proposalID == o.proposalID {
		return b.nodeID >= o.nodeID
	}
	return b.proposalID >= o.proposalID
}

func (b ballot) gt(o ballot) bool {
	if b.proposalID == o.proposalID {
		return b.nodeID > o.nodeID
	}
	return b.proposalID > o.proposalID
}

func (b ballot) eq(o ballot) bool {
	return b.proposalID == o.proposalID && b.nodeID == o.nodeID
}

func (b ballot) ne(o ballot) bool {
	return b.proposalID != o.proposalID || b.nodeID != b.nodeID
}

func (b ballot) le(o ballot) bool {
	if b.proposalID == o.proposalID {
		return b.nodeID <= o.nodeID
	}
	return b.proposalID <= o.proposalID
}

func (b ballot) lt(o ballot) bool {
	if b.proposalID == o.proposalID {
		return b.nodeID < o.nodeID
	}
	return b.proposalID < o.proposalID
}

func (b ballot) valid() bool {
	return b.proposalID != 0
}
