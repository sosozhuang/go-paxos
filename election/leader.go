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
	"github.com/sosozhuang/paxos/storage"
	"math/rand"
	"time"
	"errors"
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
	Name            string
	Addr            string
	ServiceUrl      string
	GroupID         uint16
	ElectionTimeout time.Duration
}

func (cfg *LeaderConfig) validate() error {
	if cfg.ElectionTimeout < minElectionTimeout {
		log.Warningf("Leader election time out %v, should not less than %v.", cfg.ElectionTimeout, minElectionTimeout)
		cfg.ElectionTimeout = minElectionTimeout
	}
	if cfg.ServiceUrl == "" {
		return errors.New("leader: empty service url")
	}
	return nil
}

type Leadership interface {
	Start(<-chan struct{}) error
	Stop()
	IsLeader() bool
	GetLeader() comm.Member
	SetElectionTimeout(time.Duration)
	GiveUp()
	GetStateMachine() comm.StateMachine
}

type leadership struct {
	cfg      *LeaderConfig
	proposer comm.Proposer
	sm       *leaderStateMachine
	giveUp   bool
	done     chan struct{}
}

func NewLeadership(cfg LeaderConfig, proposer comm.Proposer, st storage.Storage) (Leadership, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	ls := &leadership{
		cfg:      &cfg,
		proposer: proposer,
	}
	var err error
	ls.sm, err = newLeaderStateMachine(ls.cfg.NodeID, st)
	if err != nil {
		return nil, err
	}
	return ls, nil
}

func (l *leadership) Start(stopped <-chan struct{}) error {
	l.done = make(chan struct{})
	go l.start(stopped)
	return nil
}

func (l *leadership) Stop() {
	if l.done != nil {
		<-l.done
	}
}

func (l *leadership) start(stopped <-chan struct{}) {
	defer close(l.done)
	for {
		select {
		case <-stopped:
			return
		default:
		}
		leaseTime := l.cfg.ElectionTimeout
		start := time.Now()
		l.proposeLeaderElection(leaseTime)
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

func (l *leadership) GetLeader() (leader comm.Member) {
	leader, _ = l.sm.getLeaderInfo()
	return
}

func (l *leadership) IsLeader() bool {
	return l.sm.isLeader()
}

func (l *leadership) SetElectionTimeout(d time.Duration) {
	if d < minElectionTimeout {
		log.Warningf("Leader election time out %v, should not less than %v.", d, minElectionTimeout)
		return
	}
	l.cfg.ElectionTimeout = d
}

func (l *leadership) proposeLeaderElection(d time.Duration) {
	leader, version := l.sm.safeGetLeaderInfo()
	if leader.GetNodeID() != comm.UnknownNodeID && leader.GetNodeID() != l.cfg.NodeID {
		log.Debugf("Currently leader is %v, give up election.", &leader)
		return
	}
	log.Debugf("Start to propose leader election.")

	leader = comm.Member{
		NodeID: proto.Uint64(l.cfg.NodeID),
		Name: proto.String(l.cfg.Name),
		Addr: proto.String(l.cfg.Addr),
		ServiceUrl: proto.String(l.cfg.ServiceUrl),
	}
	info := &comm.LeaderInfo{
		Leader:       &leader,
		Version:      proto.Uint64(version),
		ElectionTimeout: proto.Int64(int64(d)),
	}

	value, err := proto.Marshal(info)
	if err != nil {
		log.Errorf("Leader election marshal data error: %v.", err)
		return
	}
	ctx := context.WithValue(context.Background(), leaderLeaseTermKey, time.Now().Add(d-hundredMilliSecond))
	if err = l.proposer.ProposeWithCtx(ctx, l.cfg.GroupID, comm.LeaderStateMachineID, value); err != nil {
		log.Errorf("Propose leader election return: %v.", err)
		return
	}
	leader, version = l.sm.getLeaderInfo()
	log.Debugf("Propose leader election result: leader %v, version %d.", &leader, version)
}

func (l *leadership) GiveUp() {
	l.giveUp = true
}

func (l *leadership) GetStateMachine() comm.StateMachine {
	return l.sm
}
