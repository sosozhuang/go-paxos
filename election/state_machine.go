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
	"github.com/sosozhuang/go-paxos/comm"
	"github.com/sosozhuang/go-paxos/storage"
	"sync"
	"time"
)

var (
	UnknownLeader = comm.Member{
		NodeID: proto.Uint64(comm.UnknownNodeID),
		Name:   proto.String(""),
		ServiceUrl:   proto.String(""),
	}
)
type leaderStateMachine struct {
	nodeID     uint64
	st         storage.Storage
	info       *comm.LeaderInfo
	expireTime time.Time
	mu         sync.RWMutex
	//leaderNodeID    uint64
	//leaderVersion   uint64
	//electionTimeout time.Duration
}

func newLeaderStateMachine(nodeID uint64, st storage.Storage) (*leaderStateMachine, error) {
	info := &comm.LeaderInfo{
		Leader: &UnknownLeader,
		Version: proto.Uint64(comm.UnknownVersion),
	}
	m := &leaderStateMachine{
		nodeID: nodeID,
		st:     st,
		info:   info,
		//leaderNodeID:  comm.UnknownNodeID,
		//leaderVersion: comm.UnknownVersion,
	}
	info, err := st.GetLeaderInfo()
	if err == storage.ErrNotFound {
		return m, nil
	}
	if err != nil {
		return nil, err
	}

	if info.GetLeader().GetNodeID() == m.nodeID {
		m.expireTime = time.Now()
	} else {
		m.info = info
		m.expireTime = time.Now().Add(time.Duration(info.GetElectionTimeout()))
	}
	return m, nil
}

func (l *leaderStateMachine) saveLeaderInfo(info *comm.LeaderInfo) error {
	return l.st.SetLeaderInfo(info)
}

func (l *leaderStateMachine) learn(info *comm.LeaderInfo, t time.Time) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if err := l.saveLeaderInfo(info); err != nil {
		return err
	}

	l.info = info
	if l.info.GetLeader().GetNodeID() == l.nodeID {
		l.expireTime = t
	} else {
		l.expireTime = time.Now().Add(time.Duration(l.info.GetElectionTimeout()))
	}
	return nil
}

func (l *leaderStateMachine) safeGetLeaderInfo() (comm.Member, uint64) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.getLeaderInfo()
}

func (l *leaderStateMachine) getLeaderInfo() (comm.Member, uint64) {
	if time.Now().After(l.expireTime) {
		return UnknownLeader, l.info.GetVersion()
	}

	return comm.Member{
		NodeID: proto.Uint64(l.info.GetLeader().GetNodeID()),
		Name: proto.String(l.info.GetLeader().GetName()),
		ServiceUrl: proto.String(l.info.GetLeader().GetServiceUrl()),
	}, l.info.GetVersion()
}

func (l *leaderStateMachine) isLeader() bool {
	leader, _ := l.getLeaderInfo()
	return leader.GetNodeID() == l.nodeID
}

func (l *leaderStateMachine) Execute(ctx context.Context, instanceID uint64, value []byte) (interface{}, error) {
	info := &comm.LeaderInfo{}
	if err := proto.Unmarshal(value, info); err != nil {
		return nil, fmt.Errorf("leader: unmarshal data: %v", err)
	}
	if info.GetVersion() != l.info.GetVersion() {
		log.Warningf("Leader state machine receive leader info version %d not equals to %d.", info.GetVersion(), l.info.GetVersion())
		return nil, nil
	}

	t, ok := ctx.Value(leaderLeaseTermKey).(time.Time)
	if !ok {
		t = time.Now()
	}
	info.Version = proto.Uint64(instanceID)
	return info.GetLeader(), l.learn(info, t)
}

func (l *leaderStateMachine) GetStateMachineID() uint32 {
	return comm.LeaderStateMachineID
}

func (l *leaderStateMachine) GetCheckpoint() ([]byte, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.info.GetVersion() == comm.UnknownVersion {
		return nil, nil
	}

	//v := &comm.LeaderInfo{
	//	NodeID:          proto.Uint64(l.leaderNodeID),
	//	Version:         proto.Uint64(l.leaderVersion),
	//	ElectionTimeout: proto.Int64(int64(l.electionTimeout)),
	//}
	b, err := proto.Marshal(l.info)
	if err != nil {
		return nil, fmt.Errorf("leader: marshal data: %v", err)
	}
	return b, nil
}

func (l *leaderStateMachine) UpdateByCheckpoint(b []byte) error {
	if len(b) <= 0 {
		return errors.New("leader: empty bytes")
	}
	info := &comm.LeaderInfo{}
	if err := proto.Unmarshal(b, info); err != nil {
		return fmt.Errorf("leader: unmarshal data: %v", err)
	}

	if l.info.GetVersion() != comm.UnknownVersion && info.GetVersion() <= l.info.GetVersion() {
		return nil
	}
	if err := l.saveLeaderInfo(info); err != nil {
		return err
	}
	if l.nodeID == info.GetLeader().GetNodeID() {
		l.info.GetLeader().NodeID = proto.Uint64(comm.UnknownNodeID)
		l.expireTime = time.Now()
	} else {
		l.info = info
		l.expireTime = time.Now().Add(time.Duration(info.GetElectionTimeout()))
	}
	return nil
}

func (l *leaderStateMachine) ExecuteForCheckpoint(uint64, []byte) error {
	return nil
}

func (l *leaderStateMachine) GetCheckpointInstanceID() uint64 {
	return l.info.GetVersion()
}

func (l *leaderStateMachine) LockCheckpointState() error {
	return nil
}

func (l *leaderStateMachine) UnlockCheckpointState() {

}

func (l *leaderStateMachine) GetCheckpointState() (string, []string) {
	return "", nil
}
