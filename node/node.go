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
	"github.com/sosozhuang/paxos/election"
	"github.com/sosozhuang/paxos/http"
	"github.com/sosozhuang/paxos/logger"
	"github.com/sosozhuang/paxos/network"
	"github.com/sosozhuang/paxos/storage"
	"github.com/sosozhuang/paxos/util"
	"math"
	"path"
	"strconv"
	"strings"
	"time"
)

const (
	maxGroupCount = math.MaxUint16
)

var (
	log               = logger.GetLogger("node")
	minProposeTimeout = time.Second
	maxProposeTimeout = time.Second * 10
	errGroupOutRange  = errors.New("group out of range")
	errNotLeader      = errors.New("not leader")
	errNodeStopping   = errors.New("node is stopping")
	errNodeNotReady   = errors.New("node not ready")
)

type NodeConfig struct {
	name           string
	groupCount     int
	members        string
	followerMode   bool
	followNode     string
	proposeTimeout time.Duration

	advertiseIP    string
	listenPeerIP   string
	listenPeerPort int

	id             uint64
	listenPeerAddr string
	followNodeID   uint64
	membersMap     map[uint64]comm.Member
}

func (cfg *NodeConfig) validate() error {
	if cfg.name == "" {
		return errors.New("empty name")
	}
	if cfg.groupCount <= 0 {
		return fmt.Errorf("invalid group count: %d", cfg.groupCount)
	}
	if cfg.groupCount > maxGroupCount {
		return fmt.Errorf("group count %d exceeded limit", cfg.groupCount)
	}
	if cfg.members == "" {
		return errors.New("empty members")
	}
	if cfg.followerMode && cfg.followNode == "" {
		return errors.New("in follower mode but not follow a node")
	}
	if cfg.listenPeerIP == "" {
		return errors.New("empty listen peer ip")
	}
	if cfg.listenPeerPort <= 0 {
		return fmt.Errorf("invalid listen peer port: %d", cfg.listenPeerPort)
	}

	var ip string
	if cfg.advertiseIP != "" {
		ip = cfg.advertiseIP
	} else {
		ip = cfg.listenPeerIP
	}
	id, err := network.AddrToUint64(ip, cfg.listenPeerPort)
	if err != nil {
		return err
	}
	cfg.id = id

	if cfg.followerMode {
		addr := strings.SplitN(cfg.followNode, ":", 2)
		if len(addr) != 2 {
			return fmt.Errorf("invalid follow node address: %s", cfg.followNode)
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

	cfg.listenPeerAddr = fmt.Sprintf("%s:%d", cfg.listenPeerIP, cfg.listenPeerPort)
	members := strings.Split(cfg.members, ",")
	cfg.membersMap = make(map[uint64]comm.Member, len(members))
	for _, member := range members {
		s := strings.SplitN(member, ":", 2)
		if len(s) != 2 {
			return fmt.Errorf("member %s unrecognize", member)
		}
		if s[0] == "" {
			return fmt.Errorf("member %s name is empty", member)
		}
		addr := strings.SplitN(s[1], ":", 2)
		if len(addr) != 2 {
			return fmt.Errorf("member %s unrecognize", member)
		}
		port, err := strconv.Atoi(addr[1])
		if err != nil {
			return fmt.Errorf("parse member address %s: %v", s[1], err)
		}
		id, err = network.AddrToUint64(addr[0], port)
		if err != nil {
			return err
		}
		if _, ok := cfg.membersMap[id]; ok {
			return fmt.Errorf("member address %s duplicated", member)
		}
		cfg.membersMap[id] = comm.Member{
			NodeID: proto.Uint64(id),
			Name:   proto.String(s[0]),
			Addr:   proto.String(s[1]),
		}
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
	cfg      *NodeConfig
	done     chan struct{}
	stopped  chan struct{}
	errChan  chan error
	ready    bool
	stopping bool
	storage  storage.MultiGroupStorage
	network  comm.NetWork
	httpSvc  http.HttpService
	leader   election.Leadership
	groups   []comm.Group
	sms      [][]comm.StateMachine
	kvstore  storage.KeyValueStore
}

func NewNode(cfg *comm.Config) (n *node, err error) {
	log.Debugf("Using config:\n%v.", cfg)
	nodeCfg := &NodeConfig{
		name:           cfg.Name,
		groupCount:     cfg.GroupCount,
		members:        cfg.Members,
		followerMode:   cfg.FollowerMode,
		followNode:     cfg.FollowNode,
		proposeTimeout: cfg.ProposeTimeout,
		advertiseIP:    cfg.AdvertiseIP,
		listenPeerIP:   cfg.ListenPeerIP,
		listenPeerPort: cfg.ListenPeerPort,
	}
	if err = nodeCfg.validate(); err != nil {
		return
	}

	n = &node{
		cfg: nodeCfg,
		//ready:   make(chan struct{}),
		done:    make(chan struct{}),
		stopped: make(chan struct{}),
		//error channel size is storage add group count of group and leader
		errChan: make(chan error, 1+cfg.GroupCount*(1+1)),
	}

	var t storage.StorageType
	t, err = storage.ParseStorageType(cfg.StorageType)
	if err != nil {
		return
	}
	storageCfg := storage.StorageConfig{
		GroupCount: cfg.GroupCount,
		DataDir:    cfg.DataDir,
		Type:       t,
		Sync:       cfg.Sync,
		SyncPeriod: cfg.SyncPeriod,
		SyncCount:  cfg.SyncCount,
		DisableWAL: cfg.DisableWAL,
	}
	n.storage, err = storage.NewDiskStorage(storageCfg)
	if err != nil {
		log.Errorf("Node create storage error: %v.", err)
		return
	}

	networkCfg := network.NetWorkConfig{
		NetWork:       cfg.ListenMode,
		Token:         cfg.Token,
		ListenAddr:    n.cfg.listenPeerAddr,
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

	if n.kvstore, err = storage.NewKeyValueStore(n, path.Join(cfg.DataDir, "kvstore")); err != nil {
		return
	}

	httpSvcCfg := http.HttpServiceConfig{
		ServiceUrl: cfg.ServiceUrl,
	}
	n.httpSvc, err = http.NewHttpService(httpSvcCfg, n, n.kvstore)
	if err != nil {
		log.Errorf("Node %d create http service error: %v.", n.cfg.id, err)
		return
	}

	if !n.cfg.followerMode {
		leaderCfg := election.LeaderConfig{
			NodeID:          n.cfg.id,
			Name:            n.cfg.name,
			Addr:            n.cfg.listenPeerAddr,
			ServiceUrl:      cfg.ServiceUrl,
			GroupID:         0,
			ElectionTimeout: cfg.ElectionTimeout,
		}
		if n.leader, err = election.NewLeadership(leaderCfg, n, n.storage.GetStorage(0)); err != nil {
			log.Errorf("Node %d create leader error: %v.", n.cfg.id, err)
			return
		}
	}

	var cluster *clusterStateMachine
	cluster, err = newClusterStateMachine(n.cfg.id, cfg.ForceNewMembers, n.storage.GetStorage(0))
	if err != nil {
		log.Errorf("Node %d create cluster error: %v.", n.cfg.id, err)
		return nil, err
	}

	n.groups = make([]comm.Group, n.cfg.groupCount)
	for i := uint16(0); i < uint16(n.cfg.groupCount); i++ {
		groupCfg := groupConfig{
			nodeID:          n.cfg.id,
			groupID:         i,
			followerMode:    n.cfg.followerMode,
			followNodeID:    n.cfg.followNodeID,
			forceNewMembers: cfg.ForceNewMembers,
			clusterSM:       cluster,
		}
		if !n.cfg.followerMode {
			groupCfg.leaderSM = n.leader.GetStateMachine()
		}
		if n.groups[i], err = newGroup(groupCfg, n.network, n.storage.GetStorage(i)); err != nil {
			log.Errorf("Node %d create group[%d] error: %v.", n.cfg.id, i, err)
			return nil, err
		}
	}
	n.SetMembers()
	n.SetMaxLogCount(cfg.MaxLogCount)

	n.AddStateMachine(0, n.kvstore)

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

		if !n.cfg.followerMode {
			if err := n.leader.Start(n.NotifyStop()); err != nil {
				log.Errorf("Node %d start leader error: %v.", n.cfg.id, err)
				n.errChan <- err
				return
			}
		}
		log.Debug("Node leader started.")
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
	util.AddHook(n.Stop)

	n.network.Start(n.NotifyStop())
	log.Debug("Node peer network started.")

	n.httpSvc.Start(n.errChan)
	log.Debugf("Node %d ready to serve.", n.cfg.id)
	n.ready = true

	select {
	case <-n.stopped:
	case err := <-n.errChan:
		log.Errorf("Node receive error: %v, going to stop.", err)
		close(n.stopped)
	}
}

func (n *node) stop() {
	n.stopping = true
	n.ready = false

	if n.httpSvc != nil {
		n.httpSvc.Stop()
	}
	log.Debug("node http service stopped.")

	if n.leader != nil {
		n.leader.Stop()
	}
	log.Debug("Node leader stopped.")

	if n.network != nil {
		n.network.StopServer()
	}
	log.Debug("Node peer network server stopped.")

	for _, group := range n.groups {
		group.Stop()
	}
	log.Debug("Node groups stopped.")

	if n.network != nil {
		n.network.StopClient()
	}
	log.Debug("Node peer network client stopped.")

	if n.storage != nil {
		n.storage.Close()
	}
	log.Debug("Node storage stopped.")

	if n.kvstore != nil {
		n.kvstore.Close()
	}
	log.Debug("Node kvstore stopped.")

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
	if err := util.BytesToObject(b[:comm.GroupIDLen], &groupID); err != nil {
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
	if !n.ready {
		return errNodeNotReady
	}
	if !n.checkGroupID(groupID) {
		return errGroupOutRange
	}
	if smid != comm.LeaderStateMachineID && !n.IsLeader() {
		return errNotLeader
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

func (n *node) GetCurrentInstanceID(groupID uint16) (uint64, error) {
	if !n.checkGroupID(groupID) {
		return 0, errGroupOutRange
	}
	return n.groups[groupID].GetCurrentInstanceID(), nil
}

func (n *node) GetNodeID() uint64 {
	return n.cfg.id
}

func (n *node) GetMemberCount() int {
	return n.groups[0].GetMemberCount()
}

func (n *node) GetGroupCount() int {
	return n.cfg.groupCount
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

func (n *node) AddMember(member comm.Member) error {
	if n.stopping {
		return errNodeStopping
	}
	if !n.ready {
		return errNodeNotReady
	}
	if !n.IsLeader() {
		return errNotLeader
	}

	ctx, cancel := context.WithTimeout(context.Background(), n.cfg.proposeTimeout)
	defer cancel()

	result, err := n.groups[0].AddMember(ctx, member)
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

func (n *node) RemoveMember(member comm.Member) error {
	if n.stopping {
		return errNodeStopping
	}
	if !n.ready {
		return errNodeNotReady
	}
	if !n.IsLeader() {
		return errNotLeader
	}
	ctx, cancel := context.WithTimeout(context.Background(), n.cfg.proposeTimeout)
	defer cancel()

	result, err := n.groups[0].RemoveMember(ctx, member)
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

func (n *node) UpdateMember(dst, src comm.Member) error {
	if n.stopping {
		return errNodeStopping
	}
	if !n.ready {
		return errNodeNotReady
	}
	if !n.IsLeader() {
		return errNotLeader
	}
	ctx, cancel := context.WithTimeout(context.Background(), n.cfg.proposeTimeout)
	defer cancel()

	result, err := n.groups[0].UpdateMember(ctx, dst, src)
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

func (n *node) GetMembers() (map[uint64]comm.Member, error) {
	return n.groups[0].GetMembers()
}

func (n *node) GetLeader() comm.Member {
	return n.leader.GetLeader()
}

func (n *node) IsLeader() bool {
	return n.leader.IsLeader()
}

func (n *node) SetElectionTimeout(d time.Duration) {
	n.leader.SetElectionTimeout(d)
}

func (n *node) GiveUpElection() {
	n.leader.GiveUp()
}

func (n *node) SetSync(groupID uint16, sync bool) error {
	if !n.checkGroupID(groupID) {
		return errGroupOutRange
	}
	return nil
}

func (n *node) AddStateMachine(groupID uint16, sms ...comm.StateMachine) error {
	if !n.checkGroupID(groupID) {
		return errGroupOutRange
	}
	n.groups[groupID].AddStateMachine(sms...)
	return nil
}
