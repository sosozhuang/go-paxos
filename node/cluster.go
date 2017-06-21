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
package node

import (
	"context"
	"errors"
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/sosozhuang/paxos/comm"
	"github.com/sosozhuang/paxos/storage"
	"math/rand"
	"sync"
)

type clusterStateMachine struct {
	nodeID          uint64
	forceNewMembers bool
	st              storage.Storage
	info            *comm.ClusterInfo
	mu              sync.RWMutex
	members         map[uint64]comm.Member
}

func newClusterStateMachine(nodeID uint64, forceNewMembers bool, st storage.Storage) (*clusterStateMachine, error) {
	c := &clusterStateMachine{
		nodeID:          nodeID,
		forceNewMembers: forceNewMembers,
		st:              st,
		info:            &comm.ClusterInfo{},
		members:         make(map[uint64]comm.Member),
	}

	if forceNewMembers {
		log.Warning("Force to create cluster membership.")
		c.info.ClusterID = proto.Uint64(comm.UnknownClusterID)
		c.info.Version = proto.Uint64(comm.UnknownVersion)
		return c, nil
	}

	info, err := st.GetClusterInfo()
	log.Debugf("Get cluster info from storage: %v.", info)
	if err == storage.ErrNotFound {
		c.info.ClusterID = proto.Uint64(comm.UnknownClusterID)
		c.info.Version = proto.Uint64(comm.UnknownVersion)
		return c, nil
	}
	if err != nil {
		return nil, err
	}
	c.update(info)
	return c, nil
}

func (c *clusterStateMachine) setMembers(members map[uint64]comm.Member) {
	if c.isInitialized() {
		log.Info("Cluster membership initialized, ignore set members.")
		return
	}
	if len(members) <= 0 {
		log.Warning("Try to set members, but length is empty")
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.members = make(map[uint64]comm.Member)
	c.info.Members = make([]*comm.Member, len(members))
	i := 0
	for id, member := range members {
		c.members[id] = comm.Member{
			NodeID: proto.Uint64(id),
			Name:   proto.String(member.GetName()),
			Addr:   proto.String(member.GetAddr()),
		}
		c.info.Members[i] = &comm.Member{
			NodeID: proto.Uint64(id),
			Name:   proto.String(member.GetName()),
			Addr:   proto.String(member.GetAddr()),
		}
		i++
	}
}

func (c *clusterStateMachine) update(info *comm.ClusterInfo) {
	m := make(map[uint64]comm.Member)
	for _, x := range info.GetMembers() {
		m[x.GetNodeID()] = comm.Member{
			NodeID: proto.Uint64(x.GetNodeID()),
			Name:   proto.String(x.GetName()),
			Addr:   proto.String(x.GetAddr()),
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.info = info
	c.members = m
}

func (c *clusterStateMachine) isInitialized() bool {
	return c.getClusterID() != comm.UnknownClusterID
}

func (c *clusterStateMachine) newClusterID() uint64 {
	return c.nodeID ^ uint64(rand.Uint32()) + uint64(rand.Uint32())
}

func (c *clusterStateMachine) getClusterID() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.info.GetClusterID()
}

func (c *clusterStateMachine) getMembers() map[uint64]comm.Member {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.members
}

func (c *clusterStateMachine) getMembersWithVersion() (map[uint64]comm.Member, uint64) {
	return c.getMembers(), c.info.GetVersion()
}

func (c *clusterStateMachine) getMemberCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.info.Members)
}

func (c *clusterStateMachine) getMajorityCount() int {
	return c.getMemberCount()/2 + 1
}

func (c *clusterStateMachine) isValidNodeID(nodeID uint64) (ok bool) {
	if !c.isInitialized() {
		ok = true
	} else {
		c.mu.RLock()
		_, ok = c.members[nodeID]
		c.mu.RUnlock()
	}
	return
}

func (c *clusterStateMachine) isInMembership() (ok bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, ok = c.members[c.nodeID]
	return
}

func (c *clusterStateMachine) save(info *comm.ClusterInfo) error {
	if err := c.st.SetClusterInfo(info); err != nil {
		return err
	}
	c.update(info)
	return nil
}

func (c *clusterStateMachine) GetStateMachineID() uint32 {
	return comm.ClusterStateMachineID
}

func (c *clusterStateMachine) Execute(ctx context.Context, instanceID uint64, b []byte) (interface{}, error) {
	v := &comm.ClusterInfo{}
	if err := proto.Unmarshal(b, v); err != nil {
		return nil, fmt.Errorf("cluster: unmarshal data: %v", err)
	}
	if !c.forceNewMembers && c.isInitialized() && v.GetClusterID() != c.info.GetClusterID() {
		log.Warningf("Cluster is initialized and proposal cluster id %d not equal to %d.", v.GetClusterID(), c.info.GetClusterID())
		return c.info, nil
	}
	if !c.forceNewMembers && v.GetVersion() != c.info.GetVersion() {
		log.Warningf("Cluster proposal version %d not equal to %d.", v.GetVersion(), c.info.GetVersion())
		return c.info, nil
	}
	v.Version = proto.Uint64(instanceID)
	return v, c.save(v)
}

func (c *clusterStateMachine) GetCheckpoint() ([]byte, error) {
	if c.info.GetVersion() == comm.UnknownVersion || !c.isInitialized() {
		return nil, nil
	}

	b, err := proto.Marshal(c.info)
	if err != nil {
		return nil, fmt.Errorf("cluster: marshal data: %v", err)
	}
	return b, nil
}

func (c *clusterStateMachine) UpdateByCheckpoint(b []byte) error {
	if len(b) <= 0 {
		return errors.New("cluster: empty bytes")
	}
	v := &comm.ClusterInfo{}
	if err := proto.Unmarshal(b, v); err != nil {
		return fmt.Errorf("cluster: unmarshal data: %v", err)
	}
	if v.GetVersion() == comm.UnknownVersion {
		return errors.New("cluster: unknown version")
	}

	if !c.forceNewMembers && c.isInitialized() && v.GetClusterID() != c.info.GetClusterID() {
		return fmt.Errorf("cluster: cluster id %d not equal to %d", v.GetClusterID(), c.info.GetClusterID())
	}

	if c.info.GetVersion() != comm.UnknownVersion && v.GetVersion() <= c.info.GetVersion() {
		return nil
	}

	return c.save(v)
}

func (c *clusterStateMachine) ExecuteForCheckpoint(uint64, []byte) error {
	return nil
}

func (c *clusterStateMachine) GetCheckpointInstanceID() uint64 {
	return c.info.GetVersion()
}

func (c *clusterStateMachine) LockCheckpointState() error {
	return nil
}

func (c *clusterStateMachine) UnlockCheckpointState() {

}

func (c *clusterStateMachine) GetCheckpointState() (string, []string) {
	return "", nil
}

type Echo struct {
	Count uint64
}

func (e *Echo) GetStateMachineID() uint32 {
	return comm.KVStoreStateMachineID
}
func (e *Echo) Execute(ctx context.Context, instanceID uint64, b []byte) (interface{}, error) {
	e.Count++
	fmt.Printf("Received message: %d, %v\n", instanceID, string(b))
	return "message received", nil
}
func (*Echo) ExecuteForCheckpoint(uint64, []byte) error {
	return nil
}
func (*Echo) GetCheckpointInstanceID() uint64 {
	return comm.UnknownInstanceID
}
func (*Echo) GetCheckpoint() ([]byte, error) {
	return nil, nil
}
func (*Echo) UpdateByCheckpoint(b []byte) error {
	return nil
}
func (*Echo) LockCheckpointState() error {
	return nil
}
func (*Echo) UnlockCheckpointState() {
}
func (*Echo) GetCheckpointState() (string, []string) {
	return "", nil
}
