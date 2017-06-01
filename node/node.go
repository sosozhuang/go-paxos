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
	"github.com/sosozhuang/paxos/comm"
	"github.com/sosozhuang/paxos/election"
	"github.com/sosozhuang/paxos/logger"
	"github.com/sosozhuang/paxos/network"
	"github.com/sosozhuang/paxos/store"
	"math"
	"strconv"
	"strings"
	"time"
)

const (
	maxGroupCount = math.MaxUint16
)

var (
	log              = logger.GetLogger("node")
	minProposeTimeout = time.Second
	maxProposeTimeout = time.Second * 10
	errGroupOutRange = errors.New("group out of range")
	errNodeStopping  = errors.New("node is stopping")
)

type NodeConfig struct {
	name             string
	groupCount       int
	members          string
	enableElection   bool
	enableMemberShip bool
	followerMode     bool
	followNode       string
	proposeTimeout   time.Duration

	advertiseIP      string
	listenIP         string
	listenPort       int

	id           uint64
	listenAddr   string
	followNodeID uint64
	membersMap map[uint64]string
}

func (cfg *NodeConfig) validate() error {
	if cfg.groupCount <= 0 {
		return errors.New("group count should greater than 0")
	}
	if cfg.groupCount > maxGroupCount {
		return fmt.Errorf("group count %d exceeded limit", cfg.groupCount)
	}
	if cfg.followerMode && cfg.followNode == "" {
		return errors.New("node in follower mode but not follow a node")
	}
	if cfg.listenIP == "" {
		return errors.New("listen ip is empty")
	}
	if cfg.listenPort <= 0 {
		return fmt.Errorf("listen port %d invalid", cfg.listenPort)
	}

	var ip string
	if cfg.advertiseIP != "" {
		ip = cfg.advertiseIP
	} else {
		ip = cfg.listenIP
	}
	id, err := network.AddrToUint64(ip, cfg.listenPort)
	if err != nil {
		return err
	}
	cfg.id = id

	if cfg.followerMode {
		addr := strings.SplitN(cfg.followNode, ":", 2)
		if len(addr) != 2 {
			return fmt.Errorf("follow node address %s unrecognize", cfg.followNode)
		}
		port, err := strconv.Atoi(addr[1])
		if err != nil {
			return fmt.Errorf("parse follow node address %s: %v", addr, err)
		}
		id, err = network.AddrToUint64(addr[0], port)
		if err != nil {
			return err
		}
		cfg.followNodeID = id
		if cfg.followNodeID == cfg.id {
			return errors.New("can't follow node itself")
		}
	}

	cfg.listenAddr = fmt.Sprintf("%s:%d", cfg.listenIP, cfg.listenPort)
	members := strings.Split(cfg.members, ",")
	cfg.membersMap = make(map[uint64]string, len(members))
	for _, member := range members {
		addr := strings.SplitN(member, ":", 2)
		if len(addr) != 2 {
			return fmt.Errorf("member address %s unrecognize", addr)
		}
		port, err := strconv.Atoi(addr[1])
		if err != nil {
			return fmt.Errorf("parse member address %s: %v", addr, err)
		}
		id, err = network.AddrToUint64(addr[0], port)
		if err != nil {
			return err
		}
		cfg.membersMap[id] = member
	}
	if _, ok := cfg.membersMap[cfg.id]; !ok {
		return errors.New("listen addr not in members")
	}

	if cfg.proposeTimeout < minProposeTimeout {
		log.Warningf("Config propose timeout %v too short, fix to %v.", cfg.proposeTimeout, minProposeTimeout)
		cfg.proposeTimeout = minProposeTimeout
	} else if cfg.proposeTimeout > maxProposeTimeout {
		log.Warningf("Config propose timeout %v too long, fix to %v.", cfg.proposeTimeout, maxProposeTimeout)
		cfg.proposeTimeout = maxProposeTimeout
	}

	return nil
}

type node struct {
	cfg *NodeConfig
	//ready   chan struct{}
	done     chan struct{}
	stopped  chan struct{}
	errChan  chan error
	stopping bool
	//wg      sync.WaitGroup
	storage store.MultiGroupStorage
	network comm.NetWork
	masters []election.Master
	groups  []comm.Group
	//batches []Batch
	sms [][]comm.StateMachine
}

func NewNode(cfg *comm.Config) (n *node, err error) {
	log.Debugf("Using config:\n%v.", cfg)
	nodeCfg := &NodeConfig{
		name:             cfg.Name,
		groupCount:       cfg.GroupCount,
		members:          cfg.Members,
		enableElection:   cfg.EnableElection,
		enableMemberShip: cfg.EnableMemberShip,
		followerMode:     cfg.FollowerMode,
		followNode:       cfg.FollowNode,
		proposeTimeout:   cfg.ProposeTimeout,
		advertiseIP:      cfg.AdvertiseIP,
		listenIP:         cfg.ListenIP,
		listenPort:       cfg.ListenPort,
	}
	if err = nodeCfg.validate(); err != nil {
		return
	}

	n = &node{
		cfg: nodeCfg,
		//ready:   make(chan struct{}),
		done:    make(chan struct{}),
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

	var t store.StorageType
	t, err = store.ParseStorageType(cfg.StorageType)
	if err != nil {
		return
	}
	storageCfg := store.StorageConfig{
		GroupCount: cfg.GroupCount,
		DataDir:    cfg.DataDir,
		Type:       t,
		Sync:       cfg.Sync,
		SyncPeriod: cfg.SyncPeriod,
		SyncCount:  cfg.SyncCount,
		DisableWAL: cfg.DisableWAL,
	}
	n.storage, err = store.NewDiskStorage(storageCfg)
	if err != nil {
		log.Errorf("Node create storage error: %v.", err)
		return
	}

	networkCfg := network.NetWorkConfig{
		NetWork:       cfg.ListenMode,
		Token:         cfg.Token,
		ListenAddr:    n.cfg.listenAddr,
		ListenTimeout: cfg.ListenTimeout,
		ReadTimeout:   cfg.ReadTimeout,
		ServerChanCap: cfg.ServerChanCap,
		DialTimeout:   cfg.DialTimeout,
		KeepAlive:     cfg.KeepAlive,
		WriteTimeout:  cfg.WriteTimeout,
		ClientChanCap: cfg.ClientChanCap,
	}
	n.network, err = network.NewPeerNetWork(networkCfg, n)
	if err != nil {
		log.Errorf("Node create peer network error: %v.", err)
		return
	}

	if n.cfg.enableElection {
		n.masters = make([]election.Master, n.cfg.groupCount)
		for i := uint16(0); i < uint16(n.cfg.groupCount); i++ {
			masterCfg := election.MasterConfig{
				NodeID:          n.cfg.id,
				GroupID:         i,
				ElectionTimeout: cfg.ElectionTimeout,
			}
			if n.masters[i], err = election.NewMaster(masterCfg, n, n.storage.GetStorage(i)); err != nil {
				log.Errorf("Node create master[%d] error: %v.", i, err)
				return nil, err
			}
		}
	}

	n.groups = make([]comm.Group, n.cfg.groupCount)
	for i := uint16(0); i < uint16(n.cfg.groupCount); i++ {
		groupCfg := groupConfig{
			nodeID:  n.cfg.id,
			groupID: i,
			//members: n.cfg.membersMap,
			followerMode:     n.cfg.followerMode,
			followNodeID:     n.cfg.followNodeID,
			forceNewMembers:  cfg.ForceNewMembers,
			enableMemberShip: n.cfg.enableMemberShip,
		}
		if n.cfg.enableElection {
			groupCfg.masterSM = n.masters[i].GetStateMachine()
		}
		if n.groups[i], err = newGroup(groupCfg, n.network, n.storage.GetStorage(i)); err != nil {
			log.Errorf("Node create group[%d] error: %v.", i, err)
			return nil, err
		}
	}
	n.SetMembers()
	n.SetMaxLogCount(cfg.MaxLogCount)

	return
}

func (n *node) Start() (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer func() {
		cancel()
		if err != nil {
			close(n.stopped)
			n.stop()
		}
	}()

	//n.wg.Add(1)
	ready := make(chan struct{})
	go func() {
		if err := n.storage.Open(ctx, n.NotifyStop()); err != nil {
			log.Errorf("Node %d open storage error: %v.", n.cfg.id, err)
			n.errChan <- err
			return
		}
		log.Debug("Node storage opened.")

		for i, g := range n.groups {
			if err := g.Start(ctx, n.NotifyStop()); err != nil {
				log.Errorf("Node %d start group %d error: %v.", n.cfg.id, i, err)
				n.errChan <- err
				return
			}
		}

		log.Debug("Node groups started.")

		if n.cfg.enableElection && !n.cfg.followerMode {
			for i, master := range n.masters {
				if err := master.Start(ctx, n.NotifyStop()); err != nil {
					log.Errorf("Node %d start master %d error: %v.", n.cfg.id, i, err)
					n.errChan <- err
					return
				}
			}
		}
		log.Debug("Node masters started.")
		close(ready)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-ready:
		return nil
	case err := <-n.errChan:
		return err
	}
}

func (n *node) Serve() {
	defer n.stop()
	n.network.Start(n.NotifyStop())
	log.Debugf("Node %d ready to serve.", n.cfg.id)

	select {
	case <-n.stopped:
	case err := <-n.errChan:
		log.Errorf("Node receive error: %v, going to stop.", err)
		close(n.stopped)
	}
}

func (n *node) stop() {
	n.stopping = true
	for _, master := range n.masters {
		master.Stop()
	}
	log.Debug("Node masters stopped.")

	if n.network != nil {
		n.network.StopServer()
	}
	log.Debug("Node network server stopped.")

	for _, group := range n.groups {
		group.Stop()
	}
	log.Debug("Node groups stopped.")

	if n.network != nil {
		n.network.StopClient()
	}
	log.Debug("Node network client stopped.")

	if n.storage != nil {
		n.storage.Close()
	}
	log.Debug("Node storage stopped.")
	//for _, batch := range n.batches {
	//	batch
	//}
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

func (n *node) checkGroupID(groupID uint16) bool {
	return groupID >= 0 && groupID < uint16(n.cfg.groupCount)
}

func (n *node) ReceiveMessage(b []byte) {
	if n.stopping {
		log.Warning("Node receive message while stopping.")
		return
	}
	var groupID uint16
	if err := comm.BytesToObject(b[:comm.GroupIDLen], &groupID); err != nil {
		log.Errorf("Node receive message convert byte to group id error: %v.", err)
		return
	}
	if !n.checkGroupID(groupID) {
		log.Errorf("Node receive message, group %d out of range.", groupID)
		return
	}
	n.groups[groupID].ReceiveMessage(b)
}

func (n *node) Propose(groupID uint16, smid uint32, value []byte) error {
	return n.ProposeWithTimeout(groupID, smid, value, n.cfg.proposeTimeout)
}

func (n *node) ProposeWithTimeout(groupID uint16, smid uint32, value []byte, d time.Duration) error {
	return n.ProposeWithCtxTimeout(context.Background(), groupID, smid, value, d)
}

func (n *node) ProposeWithCtx(ctx context.Context, groupID uint16, smid uint32, value []byte) error {
	return n.ProposeWithCtxTimeout(ctx, groupID, smid, value, n.cfg.proposeTimeout)
}

func (n *node) ProposeWithCtxTimeout(ctx context.Context, groupID uint16, smid uint32, value []byte, d time.Duration) error {
	if n.stopping {
		return errNodeStopping
	}
	if !n.checkGroupID(groupID) {
		return errGroupOutRange
	}
	if d < minProposeTimeout || d > maxProposeTimeout {
		return fmt.Errorf("propose timeout invalid: %v", d)
	}
	ctx, cancel := context.WithTimeout(ctx, d)
	defer cancel()

	result, err := n.groups[groupID].Propose(ctx, smid, value)
	if err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-n.stopped:
		return errNodeStopping
	case r := <-result:
		return r.Err
	}
}

func (n *node) BatchPropose(groupID uint16, smid uint32, value []byte, batch uint32) error {
	return n.BatchProposeWithTimeout(groupID, smid, value, batch, n.cfg.proposeTimeout)
}
func (n *node) BatchProposeWithTimeout(groupID uint16, smid uint32, value []byte, batch uint32, d time.Duration) error {
	if n.stopping {
		return errNodeStopping
	}
	if !n.checkGroupID(groupID) {
		return errGroupOutRange
	}
	if d < minProposeTimeout || d > maxProposeTimeout {
		return fmt.Errorf("propose timeout invalid: %v", d)
	}
	ctx := context.WithValue(context.Background(), "batch", batch)
	ctx, cancel := context.WithTimeout(ctx, d)
	defer cancel()

	result, err := n.groups[groupID].BatchPropose(ctx, smid, value, batch)
	if err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case r := <-result:
		return r.Err
	}
}

func (n *node) GetCurrentInstanceID(groupID uint16) (uint64, error) {
	if !n.checkGroupID(groupID) {
		return 0, errGroupOutRange
	}
	return n.groups[groupID].GetCurrentInstanceID(), nil
}

func (n *node) GetNodeID() uint64 {
	return n.cfg.id
}

func (n *node) GetNodeCount() int {
	return n.groups[0].GetNodeCount()
}

func (n *node) GetGroupCount() int {
	return n.cfg.groupCount
}

func (n *node) IsEnableMemberShip() bool {
	return n.cfg.enableMemberShip
}

func (n *node) IsFollower() bool {
	return n.cfg.followerMode
}

func (n *node) GetFollowNodeID() uint64 {
	return n.cfg.followNodeID
}

func (n *node) SetProposeTimeout(d time.Duration) {
	if d < minProposeTimeout {
		log.Warningf("Can't set propose timeout %v, shorter than %v.", d, minProposeTimeout)
		return
	} else if d > maxProposeTimeout {
		log.Warningf("Can't set propose timeout %v, longer than %v.", d, maxProposeTimeout)
		return
	}
	n.cfg.proposeTimeout = d
}

func (n *node) SetMaxLogCount(c int64) {
	for _, group := range n.groups {
		group.SetMaxLogCount(c)
	}
}

func (n *node) PauseReplayer() {
	for _, group := range n.groups {
		group.PauseReplayer()
	}
}

func (n *node) ContinueReplayer() {
	for _, group := range n.groups {
		group.ContinueReplayer()
	}
}

func (n *node) PauseCleaner() {
	for _, group := range n.groups {
		group.PauseCleaner()
	}
}

func (n *node) ContinueCleaner() {
	for _, group := range n.groups {
		group.ContinueCleaner()
	}
}

func (n *node) SetMembers() {
	for _, group := range n.groups {
		group.SetMembers(n.cfg.membersMap)
	}
}

func (n *node) AddMember(groupID uint16, ip string, port int) error {
	if !n.checkGroupID(groupID) {
		return errGroupOutRange
	}

	nodeID, err := network.AddrToUint64(ip, port)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), n.cfg.proposeTimeout)
	defer cancel()

	result, err := n.groups[groupID].AddMember(ctx, nodeID, fmt.Sprintf("%s:%d", ip, port))
	if err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case r := <-result:
		return r.Err
	}
}

func (n *node) RemoveMember(groupID uint16, ip string, port int) error {
	if !n.checkGroupID(groupID) {
		return errGroupOutRange
	}
	nodeID, err := network.AddrToUint64(ip, port)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), n.cfg.proposeTimeout)
	defer cancel()

	result, err := n.groups[groupID].RemoveMember(ctx, nodeID)
	if err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case r := <-result:
		return r.Err
	}
}

func (n *node) ChangeMember(groupID uint16, dstip string, dstport int, srcip string, srcport int) error {
	if !n.checkGroupID(groupID) {
		return errGroupOutRange
	}
	dst, err := network.AddrToUint64(dstip, dstport)
	if err != nil {
		return err
	}
	src, err := network.AddrToUint64(srcip, srcport)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), n.cfg.proposeTimeout)
	defer cancel()

	result, err := n.groups[groupID].ChangeMember(ctx, dst, fmt.Sprintf("%s:%d", dstip, dstport), src)
	if err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case r := <-result:
		return r.Err
	}
}

//func (n *node) ShowMemberShip(groupID uint16) error {
//	if !n.checkGroupID(groupID) {
//		return errGroupOutRange
//	}
//	return nil
//}
//
//func (n *node) GetMasterNode(groupID uint16) (Node, error) {
//	if !n.checkGroupID(groupID) {
//		return nil, fmt.Errorf("group %d out of range", groupID)
//	}
//	return n.masters[groupID], nil
//}
//
//func (n *node) MasterNodeWithVersion(groupID uint16, version uint64) Node {
//	if !n.checkGroupID(groupID) {
//		return errGroupOutRange
//	}
//	return nil
//}
//
//func (n *node) IsMasterNode(groupID uint16) bool {
//	if !n.checkGroupID(groupID) {
//		return errGroupOutRange
//	}
//	return false
//}

func (n *node) SetElectionTimeout(groupID uint16, d time.Duration) error {
	if !n.checkGroupID(groupID) {
		return errGroupOutRange
	}
	n.masters[groupID].SetElectionTimeout(d)
	return nil
}

func (n *node) GiveUpElection(groupID uint16) error {
	if !n.checkGroupID(groupID) {
		return errGroupOutRange
	}
	n.masters[groupID].GiveUp()
	return nil
}

func (n *node) SetMaxHoldThreads(groupID uint16, count uint) error {
	return nil
}

func (n *node) SetProposeWaitTimeThreshold(groupID uint16, duration time.Duration) error {
	return nil
}

func (n *node) SetSync(groupID uint16, sync bool) error {
	if !n.checkGroupID(groupID) {
		return errGroupOutRange
	}
	return nil
}

func (n *node) SetBatchCount(groupID uint16, count int) error {
	return nil
}

func (n *node) SetBatchDelayTime(groupID uint16, duration time.Duration) error {
	return nil
}

func (n *node) AddStateMachine(groupID uint16, sms ...comm.StateMachine) error {
	if !n.checkGroupID(groupID) {
		return errGroupOutRange
	}
	n.groups[groupID].AddStateMachine(sms...)
	return nil
}
