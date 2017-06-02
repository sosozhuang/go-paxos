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
package election

import (
	"context"
	"github.com/gogo/protobuf/proto"
	"github.com/sosozhuang/paxos/comm"
	"github.com/sosozhuang/paxos/logger"
	"github.com/sosozhuang/paxos/store"
	"math/rand"
	"time"
)

var (
	log = logger.GetLogger("election")
)

const (
	minElectionTimeout = time.Second * 10
	hundredMilliSecond = time.Second / 10
	leaderLeaseTermKey = "_leader_lease_term"
)

type LeaderConfig struct {
	NodeID          uint64
	GroupID         uint16
	ElectionTimeout time.Duration
}

func (cfg *LeaderConfig) validate() error {
	if cfg.ElectionTimeout < minElectionTimeout {
		log.Warningf("Leader election time out %v, should not less than %v.", cfg.ElectionTimeout, minElectionTimeout)
		cfg.ElectionTimeout = minElectionTimeout
	}
	return nil
}

type Leader interface {
	Start(context.Context, <-chan struct{}) error
	Stop()
	SetElectionTimeout(time.Duration)
	GiveUp()
	GetStateMachine() comm.StateMachine
}

type leader struct {
	cfg      *LeaderConfig
	proposer comm.Proposer
	sm       *leaderStateMachine
	giveUp   bool
	done     chan struct{}
}

func NewLeader(cfg LeaderConfig, proposer comm.Proposer, st store.Storage) (Leader, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	leader := &leader{
		cfg:      &cfg,
		proposer: proposer,
	}
	var err error
	leader.sm, err = newLeaderStateMachine(leader.cfg.NodeID, st)
	if err != nil {
		return nil, err
	}
	return leader, nil
}

func (m *leader) Start(ctx context.Context, stopped <-chan struct{}) error {
	m.done = make(chan struct{})
	go m.start(stopped)
	return nil
}

func (l *leader) Stop() {
	if l.done != nil {
		<-l.done
	}
}

func (l *leader) start(stopped <-chan struct{}) {
	defer close(l.done)
	for {
		select {
		case <-stopped:
			return
		default:
		}
		leaseTime := l.cfg.ElectionTimeout
		start := time.Now()
		l.elect(leaseTime)
		continueTime := (leaseTime - hundredMilliSecond) / 3
		continueTime = continueTime/2 + time.Duration(rand.Int63())%continueTime
		if l.giveUp {
			l.giveUp = false
			continueTime *= 2
		}
		duration := time.Now().Sub(start)
		if continueTime > duration {
			time.Sleep(continueTime - duration)
		}
	}
}

func (l *leader) SetElectionTimeout(d time.Duration) {
	if d < minElectionTimeout {
		log.Warningf("Leader election time out %v, should not less than %v.", d, minElectionTimeout)
		return
	}
	l.cfg.ElectionTimeout = d
}

func (l *leader) elect(d time.Duration) {
	nodeID, version := l.sm.safeGetLeaderInfo()
	if nodeID != comm.UnknownNodeID && nodeID != l.cfg.NodeID {
		log.Debugf("Currently leader node id is %d, give up election.", nodeID)
		return
	}
	log.Debugf("Start leader election.")

	op := &comm.LeaderElection{
		NodeID:       proto.Uint64(l.cfg.NodeID),
		Version:      proto.Uint64(version),
		ElectionTimeout: proto.Int64(int64(d)),
	}

	value, err := proto.Marshal(op)
	if err != nil {
		log.Errorf("Leader election marshal data error: %v.", err)
		return
	}
	ctx := context.WithValue(context.Background(), leaderLeaseTermKey, time.Now().Add(d-hundredMilliSecond))
	if err = l.proposer.ProposeWithCtx(ctx, l.cfg.GroupID, comm.LeaderStateMachineID, value); err != nil {
		log.Errorf("Leader election propose return: %v.", err)
	}
}

func (l *leader) GiveUp() {
	l.giveUp = true
}

func (l *leader) GetStateMachine() comm.StateMachine {
	return l.sm
}
