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
	"fmt"
	"github.com/sosozhuang/paxos/comm"
	"github.com/sosozhuang/paxos/election"
	"github.com/sosozhuang/paxos/logger"
	"github.com/sosozhuang/paxos/network"
	"github.com/sosozhuang/paxos/paxos"
	"github.com/sosozhuang/paxos/store"
	"sync"
	"time"
	"strings"
	"strconv"
	"net"
	"errors"
)

const (
	log = logger.PaxosLogger
	maxGroupCount = 1000
)

type NodeConfig struct {
	//storage
	DataDir          string
	StorageType      string
	Sync             bool
	SyncDuration     time.Duration
	SyncInterval     int
	DisableWAL       bool

	//network
	network          string
	Token            string
	AdvertiseIP      string
	ListenIP         string
	ListenPort       int

	EnableMaster     bool
	EnableMemberShip bool
	FollowerMode     bool
	FollowNodeID     comm.NodeID
	Name             string
	GroupCount       int
	Peers            string

	//runtime
	id               comm.NodeID
	listenAddr       string
	peersMap         map[comm.NodeID]net.Addr

}

func (cfg *NodeConfig) validate() error {
	if cfg.GroupCount > maxGroupCount {
		return fmt.Errorf("group count %d too large", cfg.GroupCount)
	}

	var ip string
	if cfg.AdvertiseIP != "" {
		ip = cfg.AdvertiseIP
	} else {
		ip = cfg.ListenIP
	}
	id, err := network.AddrToInt64(ip, cfg.ListenPort)
	if err != nil {
		return err
	}
	cfg.id = id

	//currently only tcp network supported
	cfg.network = "tcp"
	cfg.listenAddr = fmt.Sprintf("%s:%d", cfg.ListenIP, cfg.ListenPort)

	peers := strings.Split(cfg.Peers, ",")
	cfg.peersMap = make(map[comm.NodeID]net.Addr, len(peers))
	for _, peer := range peers {
		addr, err := network.ResolveAddr(cfg.network, peer)
		if err != nil {
			return err
		}
		id, err = network.AddrToInt64(cfg.ListenIP, cfg.ListenPort)
		if err != nil {
			return err
		}
		cfg.peersMap[id] = addr
	}
	if _, ok := cfg.peersMap[cfg.id]; ok {
		return errors.New("listen addr should not in peers")
	}

	return nil
}

type node struct {
	cfg           *NodeConfig
	ready         chan struct{}
	done          chan struct{}
	stopped       chan struct{}
	errChan       chan error
	wg            sync.WaitGroup
	storage       store.MultiGroupStorage
	network       comm.NetWork
	masters       []election.Master
	groups        []Group
	batches       []Batch
	stateMachines [][]StateMachine
	transporter   paxos.Transporter
}

func NewNode(cfg NodeConfig) (n *node, err error) {
	if err = cfg.validate(); err != nil {
		return nil, err
	}
	n = &node{
		cfg: cfg,
		ready: make(chan struct{}),
		done: make(chan struct{}),
		stopped: make(chan struct{}),
		// todo: error chan size gt network,group, master...
		errChan: make(chan error, 10),
	}

	defer func() {
		if err != nil && n != nil {
			n.stop()
			n = nil
		}
	}()

	storageType, err := store.ParseStorageType(n.cfg.StorageType)
	if err != nil {
		return
	}
	storageCfg := store.StorageConfig{
		GroupCount: n.cfg.GroupCount,
		DataDir: n.cfg.DataDir,
		Type:    storageType,
		DisableSync: !n.cfg.Sync,
		SyncDuration: n.cfg.SyncDuration,
		SyncInterval: n.cfg.SyncInterval,
		DisableWAL: n.cfg.DisableWAL,
	}
	n.storage, err = store.NewDiskStorage(storageCfg)
	if err != nil {
		log.Error(err)
		return
	}

	networkCfg := network.NetWorkConfig{
		NetWork: n.cfg.network,
		Token: n.cfg.Token,
		ListenAddr: n.cfg.listenAddr,
		ListenTimeout: 0,
		ReadTimeout: 0,
		ServerChanCap: 0,
		DialTimeout: 0,
		KeepAlive: 0,
		WriteTimeout: 0,
		ClientChanCap: 0,
	}
	n.network, err = network.NewPeerNetWork(networkCfg, n)
	if err != nil {
		log.Error(err)
		return
	}

	if n.cfg.EnableMaster {
		n.masters = make([]election.Master, n.cfg.GroupCount)
		masterCfg := election.MasterConfig{}
		for i := 0; i < n.cfg.GroupCount; i++ {
			if n.masters[i], err = election.NewMaster(masterCfg, n, i, n.storage); err != nil {
				log.Error(err)
				return
			}
		}
	}

	n.transporter, err = paxos.NewTransporter()
	if err != nil {
		log.Error(err)
		return
	}

	n.groups = make([]Group, n.cfg.GroupCount)
	groupCfg := GroupConfig{}
	for i := 0; i < n.cfg.GroupCount; i++ {
		if n.groups[i], err = newGroup(groupCfg, n, i, n.masters[i].GetStateMachine()); err != nil {
			log.Error(err)
			return
		}
	}

	return
}

func (n *node) Start() (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer func() {
		cancel()
		if err != nil {
			n.stop()
		}
	}()
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		if err := n.storage.Open(ctx, n.NotifyStop()); err != nil {
			n.errChan <- err
		}
	}()
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		if err := n.network.Start(ctx, n.NotifyStop()); err != nil {
			n.errChan <- err
		}
	}()

	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		for _, master := range n.masters {
			if err := master.Start(ctx, n.NotifyStop()); err != nil {
				n.errChan <- err
				break
			}
		}
	}()



	go func() {
		n.wg.Wait()
		close(n.ready)
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-n.ready:
		return nil
	case err := <-n.errChan:
		return err
	}
}

func (n *node) Serve() error {
	defer n.stop()
	log.Debug("node ready")
	select {
	case <-n.stop:
		return nil
	case err := <-n.errChan:
		return err
	}
}

func (n *node) stop() {
	if n.storage != nil {
		n.storage.Close()
	}
	if n.network != nil {
		n.network.Stop()
	}
	for _, master := range n.masters {

	}
	for _, group := range n.groups {

	}
	for _, batch := range n.batches {

	}
	for _, sm := range n.stateMachines {

	}
	close(n.done)
}

func (n *node) Stop() {
	close(n.stopped)
	<-n.done
}

func (n *node) NotifyStop() <-chan struct{} {
	return n.stopped
}
func (n *node) NotifyError(err error) {
	n.errChan <- err
}

func (n *node) initStateMachine() error {
	for i, group := range n.groups {
		group.AddStateMachine(n.stateMachines[i]...)
	}
	if n.cfg.EnableMaster && !n.cfg.FollowerMode {
		for _, master := range n.masters {
			master.Start()
		}
	}
	return nil
}

func (n *node) checkGroupID(groupID comm.GroupID) bool {
	return groupID < 0 || groupID >= n.cfg.GroupCount
}

func (n *node) ReceiveMessage(b []byte) {
	var groupID uint16
	if err := comm.BytesToObject(b[:comm.GroupIDLen], &groupID); err != nil {
		log.Error(err)
		return
	}
	if !n.checkGroupID(groupID) {
		log.Errorf("group %d out of range\n", groupID)
		return
	}
	n.groups[groupID].ReceiveMessage(b[comm.GroupIDLen:])
}

func (n *node) Propose(groupID comm.GroupID, value []byte) error {
	return n.ProposeWithTimeout(time.Minute, groupID, value)
}

func (n *node) ProposeWithTimeout(d time.Duration, groupID comm.GroupID, value []byte) error {
	if !n.checkGroupID(groupID) {
		return fmt.Errorf("group %d out of range", groupID)
	}
	if d < time.Millisecond {
		return fmt.Errorf("timeout %v too short", d)
	}
	ctx := context.WithValue(context.Background(), "value", value)
	ctx, cancel := context.WithTimeout(ctx, d)
	defer cancel()

	stopped, errc, err := n.groups[groupID].Propose(ctx, value)
	if err != nil {
		return err
	}

	select {
	case <- ctx.Done():
		return ctx.Err()
	case <- stopped:
		return nil
	case err := <- errc:
		return err
	}
}
func (n *node) BatchPropose(groupID comm.GroupID, value []byte, batch uint32) error {
	return n.BatchProposeWithTimeout(time.Minute, groupID, value, batch)
}
func (n *node) BatchProposeWithTimeout(d time.Duration, groupID uint16, value []byte, batch uint32) error {
	if !n.checkGroupID(groupID) {
		return fmt.Errorf("group %d out of range", groupID)
	}
	if d < time.Millisecond {
		return fmt.Errorf("timeout %v too short", d)
	}
	ctx := context.WithValue(context.Background(), "value", value)
	ctx = context.WithValue(ctx, "batch", batch)
	ctx, cancel := context.WithTimeout(ctx, d)
	defer cancel()

	stopped, errc, err := n.groups[groupID].BatchPropose(ctx, value, batch)
	if err != nil {
		return err
	}

	select {
	case <- ctx.Done():
		return ctx.Err()
	case <- stopped:
		return nil
	case err := <- errc:
		return err
	}
}

func (n *node) CurrentInstanceID(groupID comm.GroupID) (comm.InstanceID, error) {
	if !n.checkGroupID(groupID) {
		return comm.InstanceID(0), fmt.Errorf("group %d out of range", groupID)
	}
	return n.groups[groupID].GetCurrentInstanceID()
}

func (n *node) GetNodeID() comm.NodeID {
	return n.cfg.id
}

func (n *node) GetNodeCount() int {
	return len(n.cfg.Peers) + 1
}

func (n *node) GetGroupCount() int {
	return n.cfg.GroupCount
}

func (n *node) MemberShipEnabled() bool {
	return n.cfg.EnableMemberShip
}

func (n *node) IsFollower() bool {
	return n.cfg.FollowerMode
}

func (n *node) GetFollowNodeID() comm.NodeID {
	return n.cfg.FollowNodeID
}

func (n *node) handleMessage(msg []byte, length int) error {
	group := uint16(0)
	if !n.checkGroupID(group) {
		return fmt.Errorf("group %d out of range", group)
	}
	return n.groups[group].HandleMessage(msg, length)
}

func (n *node) SetTimeout(d time.Duration) {
	for _, group := range n.groups {
		group.SetTimeout(d)
	}
}

func (n *node) SetMaxLogCount(c uint64) {
	for _, group := range n.groups {
		group.Cleaner().SetMaxLogCount(c)
	}
}

func (n *node) PauseCheckpointReplayer() {
	for _, group := range n.groups {
		group.Replayer().Pause()
	}
}

func (n *node) ContinueCheckpointReplayer() {
	for _, group := range n.groups {
		group.Replayer().Continue()
	}
}

func (n *node) PausePaxosLogCleaner() {
	for _, group := range n.groups {
		group.Cleaner().Pause()
	}
}

func (n *node) ContinuePaxosLogCleaner() {
	for _, group := range n.groups {
		group.Cleaner().Continue()
	}
}

func (n *node) AddMember(group uint16, peer Node) error {
	return nil
}

func (n *node) RemoveMember(group uint16, peer Node) error {
	return nil
}

func (n *node) ChangeMember(group uint16, from Node, to Node) error {
	return nil
}

func (n *node) ShowMemberShip(group uint16) error {
	return nil
}

func (n *node) GetMasterNode(groupID comm.GroupID) (Node, error) {
	if !n.checkGroupID(groupID) {
		return nil, fmt.Errorf("group %d out of range", groupID)
	}
	return n.masters[groupID], nil
}

func (n *node) MasterNodeWithVersion(groupID comm.GroupID, version uint64) Node {
	return nil
}

func (n *node) IsMasterNode(groupID comm.GroupID) bool {
	return false
}

func (n *node) SetMasterLeaseTime(groupID comm.GroupID, duration time.Duration) error {
	return nil
}

func (n *node) GiveUpMasterElection(groupID comm.GroupID) error {
	return nil
}

func (n *node) SetMaxHoldThreads(groupID comm.GroupID, count uint) error {
	return nil
}

func (n *node) SetProposeWaitTimeThreshold(groupID comm.GroupID, duration time.Duration) error {
	return nil
}

func (n *node) SetLogSync(groupID comm.GroupID, sync bool) error {
	return nil
}

func (n *node) GetInstanceValue(groupID comm.GroupID, instance paxos.InstanceID) ([][]byte, error) {
	return make([][]byte, 0), nil
}

func (n *node) SetBatchCount(groupID comm.GroupID, count uint16) error {
	return nil
}

func (n *node) SetBatchDelayTime(groupID comm.GroupID, duration time.Duration) error {
	return nil
}
