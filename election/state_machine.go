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
	"errors"
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/sosozhuang/paxos/comm"
	"github.com/sosozhuang/paxos/store"
	"sync"
	"time"
)

type leaderStateMachine struct {
	nodeID          uint64
	st              store.Storage
	leaderNodeID    uint64
	leaderVersion   uint64
	electionTimeout time.Duration
	expireTime      time.Time
	mu              sync.RWMutex
}

func newLeaderStateMachine(nodeID uint64, st store.Storage) (*leaderStateMachine, error) {
	m := &leaderStateMachine{
		nodeID:        nodeID,
		st:            st,
		leaderNodeID:  comm.UnknownNodeID,
		leaderVersion: comm.UnknownVersion,
	}
	v, err := st.GetLeaderInfo()
	if err == store.ErrNotFound {
		return m, nil
	}
	if err != nil {
		return nil, err
	}

	m.leaderVersion = v.GetVersion()
	if v.GetNodeID() == m.nodeID {
		m.leaderNodeID = comm.UnknownNodeID
		m.expireTime = time.Now()
	} else {
		m.leaderNodeID = v.GetNodeID()
		m.expireTime = time.Now().Add(time.Duration(v.GetElectionTimeout()))
	}
	return m, nil
}

func (l *leaderStateMachine) saveLeaderInfo(nodeID, version uint64, d int64) error {
	return l.st.SetLeaderInfo(&comm.LeaderInfo{
		NodeID:          proto.Uint64(nodeID),
		Version:         proto.Uint64(version),
		ElectionTimeout: proto.Int64(d),
	})
}

func (l *leaderStateMachine) learn(instanceID uint64, election *comm.LeaderElection, t time.Time) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.leaderVersion != election.GetVersion() {
		return nil
	}

	if err := l.saveLeaderInfo(election.GetNodeID(), instanceID, election.GetElectionTimeout()); err != nil {
		return err
	}

	l.leaderNodeID = election.GetNodeID()
	if l.leaderNodeID == l.nodeID {
		l.expireTime = t
	} else {
		l.expireTime = time.Now().Add(time.Duration(election.GetElectionTimeout()))
	}
	l.electionTimeout = time.Duration(election.GetElectionTimeout())
	l.leaderVersion = instanceID
	return nil
}

func (l *leaderStateMachine) safeGetLeaderInfo() (uint64, uint64) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.getLeaderInfo()
}

func (l *leaderStateMachine) getLeaderInfo() (uint64, uint64) {
	if time.Now().After(l.expireTime) {
		return comm.UnknownNodeID, l.leaderVersion
	}

	return l.leaderNodeID, l.leaderVersion
}

func (l *leaderStateMachine) isLeader() bool {
	n, _ := l.getLeaderInfo()
	return n == l.nodeID
}

func (l *leaderStateMachine) Execute(ctx context.Context, instanceID uint64, value []byte) (interface{}, error) {
	election := &comm.LeaderElection{}
	if err := proto.Unmarshal(value, election); err != nil {
		return nil, fmt.Errorf("leader: unmarshal data: %v", err)
	}

	t, ok := ctx.Value(leaderLeaseTermKey).(time.Time)
	if !ok {
		t = time.Now()
	}
	return election, l.learn(instanceID, election, t)
}

func (l *leaderStateMachine) GetStateMachineID() uint32 {
	return comm.LeaderStateMachineID
}

func (l *leaderStateMachine) GetCheckpoint() ([]byte, error) {
	if l.leaderVersion == comm.UnknownVersion {
		return nil, nil
	}

	v := &comm.LeaderInfo{
		NodeID:          proto.Uint64(l.leaderNodeID),
		Version:         proto.Uint64(l.leaderVersion),
		ElectionTimeout: proto.Int64(int64(l.electionTimeout)),
	}
	b, err := proto.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("leader: marshal data: %v", err)
	}
	return b, nil
}

func (l *leaderStateMachine) UpdateByCheckpoint(b []byte) error {
	if len(b) <= 0 {
		return errors.New("leader: empty bytes")
	}
	v := &comm.LeaderInfo{}
	if err := proto.Unmarshal(b, v); err != nil {
		return fmt.Errorf("leader: unmarshal data: %v", err)
	}

	if v.GetVersion() <= l.leaderVersion && l.leaderVersion != comm.UnknownVersion {
		return nil
	}
	if err := l.saveLeaderInfo(v.GetNodeID(), v.GetVersion(), v.GetElectionTimeout()); err != nil {
		return err
	}
	l.leaderVersion = v.GetVersion()
	if l.nodeID == v.GetNodeID() {
		l.leaderNodeID = comm.UnknownNodeID
		l.expireTime = time.Now()
	} else {
		l.leaderNodeID = v.GetNodeID()
		l.expireTime = time.Now().Add(time.Duration(v.GetElectionTimeout()))
	}
	return nil
}

func (l *leaderStateMachine) ExecuteForCheckpoint(uint64, []byte) error {
	return nil
}

func (l *leaderStateMachine) GetCheckpointInstanceID() uint64 {
	return l.leaderVersion
}

func (l *leaderStateMachine) LockCheckpointState() error {
	return nil
}

func (l *leaderStateMachine) UnlockCheckpointState() {

}

func (l *leaderStateMachine) GetCheckpointState() (string, []string) {
	return "", nil
}
