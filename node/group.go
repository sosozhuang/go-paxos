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
	"github.com/sosozhuang/go-paxos/comm"
	"github.com/sosozhuang/go-paxos/network"
	"github.com/sosozhuang/go-paxos/paxos"
	"github.com/sosozhuang/go-paxos/storage"
	"github.com/sosozhuang/go-paxos/util"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	maxValueLength = 10 * 1024 * 1024
	learnTimeout   = time.Minute
	followTimeout  = time.Second * 10
)

type groupConfig struct {
	nodeID           uint64
	groupID          uint16
	followerMode     bool
	followNodeID     uint64
	forceNewMembers  bool
	enableReplayer   bool
	sms              []comm.StateMachine
	leaderSM         comm.StateMachine
	clusterSM        *clusterStateMachine
	lmu              sync.Mutex
	learnNodes       map[uint64]time.Time
	fmu              sync.Mutex
	followers        map[uint64]time.Time
}

func (cfg *groupConfig) init() {
	cfg.sms = make([]comm.StateMachine, 0)
	cfg.learnNodes = make(map[uint64]time.Time)
	cfg.followers = make(map[uint64]time.Time)
}

func (cfg *groupConfig) FollowerMode() bool {
	return cfg.followerMode
}

func (cfg *groupConfig) GetFollowNodeID() uint64 {
	return cfg.followNodeID
}

func (cfg *groupConfig) GetNodeID() uint64 {
	return cfg.nodeID
}

func (cfg *groupConfig) GetGroupID() uint16 {
	return cfg.groupID
}

func (cfg *groupConfig) GetClusterID() uint64 {
	return cfg.clusterSM.getClusterID()
}

func (cfg *groupConfig) isClusterInitialized() bool {
	return cfg.clusterSM.isInitialized()
}

func (cfg *groupConfig) GetMajorityCount() int {
	return cfg.clusterSM.getMajorityCount()
}

func (cfg *groupConfig) isValidNodeID(nodeID uint64) bool {
	return cfg.clusterSM.isValidNodeID(nodeID)
}

func (cfg *groupConfig) GetMemberCount() int {
	return cfg.clusterSM.getMemberCount()
}

func (cfg *groupConfig) getMembersWithVersion() (map[uint64]comm.Member, uint64) {
	return cfg.clusterSM.getMembersWithVersion()
}

func (cfg *groupConfig) isInMembership() bool {
	return cfg.clusterSM.isInMembership()
}

func (cfg *groupConfig) IsEnableReplayer() bool {
	return cfg.enableReplayer
}

func (cfg *groupConfig) addLearnNode(id uint64) {
	if _, ok := cfg.clusterSM.getMembers()[id]; ok {
		cfg.lmu.Lock()
		cfg.learnNodes[id] = time.Now().Add(learnTimeout)
		cfg.lmu.Unlock()
	}
}

func (cfg *groupConfig) GetLearnNodes() map[uint64]string {
	now := time.Now()
	m := make(map[uint64]string, len(cfg.learnNodes))
	cfg.lmu.Lock()
	defer cfg.lmu.Unlock()
	for id, t := range cfg.learnNodes {
		if t.Before(now) {
			delete(cfg.learnNodes, id)
		} else {
			m[id] = network.Uint64ToAddr(id)
		}
	}
	return m
}

func (cfg *groupConfig) AddFollower(nodeID uint64) {
	cfg.fmu.Lock()
	cfg.followers[nodeID] = time.Now().Add(followTimeout)
	cfg.fmu.Unlock()
}

func (cfg *groupConfig) GetFollowers() map[uint64]string {
	now := time.Now()
	m := make(map[uint64]string, len(cfg.followers))
	cfg.fmu.Lock()
	defer cfg.fmu.Unlock()
	for id, t := range cfg.followers {
		if t.Before(now) {
			delete(cfg.followers, id)
		} else {
			m[id] = network.Uint64ToAddr(id)
		}
	}
	return m
}

func (cfg *groupConfig) setMembers(members map[uint64]comm.Member) {
	cfg.clusterSM.setMembers(members)
}

func (cfg *groupConfig) GetMembers() map[uint64]comm.Member {
	return cfg.clusterSM.getMembers()
}

func (cfg *groupConfig) GetClusterCheckpoint() ([]byte, error) {
	return cfg.clusterSM.GetCheckpoint()
}

func (cfg *groupConfig) UpdateClusterByCheckpoint(b []byte) error {
	return cfg.clusterSM.UpdateByCheckpoint(b)
}

func (cfg *groupConfig) GetLeaderCheckpoint() ([]byte, error) {
	return cfg.leaderSM.GetCheckpoint()
}

func (cfg *groupConfig) UpdateLeaderByCheckpoint(b []byte) error {
	return cfg.leaderSM.UpdateByCheckpoint(b)
}

var (
	errExceededLimit         = errors.New("group: proposal value length exceeded limit")
	errInFollowerMode        = errors.New("group: follower mode can't accept proposal")
	errNotInMemberShip       = errors.New("group: not in membership")
	errChanFull              = errors.New("group: proposal channel full")
	errClusterIDNotMatched   = errors.New("group: message cluster id not matched")
	errChecksumNotMatched    = errors.New("group: message checksum not matched")
	//ErrClusterUninitialized  = errors.New("group: cluster uninitialized")
	//ErrMemberExists          = errors.New("group: member already exists")
	//ErrMemberNotExists       = errors.New("group: member not exists")
	//ErrMemberNotModified     = errors.New("group: member not modified")
	//ErrMemberNameEmpty       = errors.New("group: empty member name")
	//ErrMemberAddrEmpty       = errors.New("group: empty member address")
	//ErrMemberServiceUrlEmpty = errors.New("group: empty member service url")
)

type group struct {
	cfg       *groupConfig
	instance  comm.Instance
	proposeCh chan context.Context
	msgCh     chan []byte
	wg        sync.WaitGroup
}

func newGroup(cfg groupConfig, sender comm.Sender, st storage.Storage) (comm.Group, error) {
	cfg.init()
	group := &group{
		cfg:       &cfg,
		proposeCh: make(chan context.Context, 100),
		msgCh:     make(chan []byte, 100),
	}

	var err error
	group.instance, err = paxos.NewInstance(group.cfg, sender, st)
	if err != nil {
		return nil, err
	}

	return group, nil
}

func (g *group) Start(ctx context.Context, stopped <-chan struct{}) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		if g.cfg.leaderSM != nil {
			g.addStateMachine(g.cfg.leaderSM)
		}
		g.addStateMachine(g.cfg.clusterSM)
		g.addStateMachine(g.cfg.sms...)
		go g.handleNewValue(stopped)
		go g.handleMessage(stopped)
		if err := g.instance.Start(ctx, stopped); err != nil {
			return err
		}
		log.Debugf("Group %d started.", g.cfg.GetGroupID())
		return nil
	}
}

func (g *group) Stop() {
	close(g.proposeCh)
	close(g.msgCh)
	g.instance.Stop()
	g.wg.Wait()
	log.Debugf("Group %d stopped.", g.cfg.GetGroupID())
}

func (g *group) Propose(ctx context.Context, smid uint32, value []byte) (<-chan comm.Result, error) {
	if len(value) >= maxValueLength {
		return nil, errExceededLimit
	}
	if g.cfg.FollowerMode() {
		return nil, errInFollowerMode
	}
	if !g.cfg.isInMembership() {
		return nil, errNotInMemberShip
	}

	b, err := util.ObjectToBytes(smid)
	if err != nil {
		return nil, err
	}
	ctx = context.WithValue(ctx, "value", append(b, value...))
	result := make(chan comm.Result)
	ctx = context.WithValue(ctx, "result", result)
	for i := 0; i < 3; i++ {
		select {
		case g.proposeCh <- ctx:
			return result, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			time.Sleep(time.Millisecond * 10)
			continue
		}
	}
	return nil, errChanFull
}

func (g *group) BatchPropose(context.Context, uint32, []byte, uint32) (<-chan comm.Result, error) {
	return nil, nil
}

func (g *group) handleNewValue(stopped <-chan struct{}) {
	g.wg.Add(1)
	defer func() {
		g.wg.Done()
		log.Debugf("Group %d handle new value stopped.", g.cfg.GetGroupID())
	}()
	for {
		select {
		case <-stopped:
			return
		default:
		}
		if !g.instance.IsReadyForNewValue() {
			time.Sleep(time.Millisecond * 10)
			continue
		}

		ctx, ok := <-g.proposeCh
		if !ok {
			return
		}
		if d, ok := ctx.Deadline(); ok && d.Before(time.Now()) {
			continue
		}
		if g.instance.GetProposerInstanceID() == 0 ||
			!g.cfg.isClusterInitialized() {
			g.initCluster()
		}
		g.instance.NewValue(ctx)

	}
}

func (g *group) initCluster() {
	members, version := g.cfg.getMembersWithVersion()
	svar := &comm.ClusterInfo{
		ClusterID: proto.Uint64(g.cfg.clusterSM.newClusterID()),
		Version:   proto.Uint64(version),
	}
	svar.Members = make([]*comm.Member, len(members))
	i := 0
	for _, member := range members {
		svar.Members[i] = &comm.Member{
			NodeID: proto.Uint64(member.GetNodeID()),
			Name:   proto.String(member.GetName()),
			Addr:   proto.String(member.GetAddr()),
		}
		i++
	}

	value, err := proto.Marshal(svar)
	if err != nil {
		log.Errorf("Group %d marshal cluster info error: %v.", g.cfg.GetGroupID(), err)
		return
	}

	b, err := util.ObjectToBytes(comm.ClusterStateMachineID)
	if err != nil {
		log.Errorf("Group %d convert cluster smid to bytes error: %v.", g.cfg.GetGroupID(), err)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), maxProposeTimeout)
	defer cancel()
	ctx = context.WithValue(ctx, "value", append(b, value...))
	result := make(chan comm.Result)
	ctx = context.WithValue(ctx, "result", result)

	go g.instance.NewValue(ctx)
	select {
	case <-ctx.Done():
		log.Errorf("Group %d propose new cluster info error: %v.", g.cfg.GetGroupID(), ctx.Err())
	case x := <-result:
		if x.Err != nil {
			log.Errorf("Group %d propose new cluster info error: %v.", g.cfg.GetGroupID(), x.Err)
		} else {
			log.Infof("Group %d propose new cluster info result: %v.", g.cfg.GetGroupID(), x)
		}
	}
}

func (g *group) AddStateMachine(sms ...comm.StateMachine) {
	g.cfg.sms = append(g.cfg.sms, sms...)
}

func (g *group) addStateMachine(sms ...comm.StateMachine) {
	g.instance.AddStateMachine(sms...)
}

func (g *group) ReceiveMessage(b []byte) {
	g.msgCh <- b
}

func (g *group) handleMessage(stopped <-chan struct{}) {
	g.wg.Add(1)
	defer func() {
		g.wg.Done()
		log.Debugf("Group %d handle message stopped.", g.cfg.GetGroupID())
	}()
	for b := range g.msgCh {
		select {
		case <-stopped:
			return
		default:
		}
		header, b, err := g.unpack(b)
		if err != nil {
			log.Errorf("Group %d unpack message error: %v.", g.cfg.GetGroupID(), err)
			continue
		}

		switch header.GetType() {
		case comm.MsgType_Paxos:
			msg := &comm.PaxosMsg{}
			if err := proto.Unmarshal(b, msg); err != nil {
				log.Errorf("Group %d unmarshal paxos message error: %v.", g.cfg.GetGroupID(), err)
				continue
			}
			if !g.checkPaxosMessage(msg) {
				continue
			}
			g.instance.ReceivePaxosMessage(msg)
		case comm.MsgType_Checkpoint:
			msg := &comm.CheckpointMsg{}
			if err := proto.Unmarshal(b, msg); err != nil {
				log.Errorf("Group %d unmarshal checkpoint message error: %v.", g.cfg.GetGroupID(), err)
				continue
			}
			g.instance.ReceiveCheckpointMessage(msg)
		}
	}
}

func (g *group) unpack(b []byte) (*comm.Header, []byte, error) {
	//skip group id
	start := comm.GroupIDLen
	offset := start + comm.Int32Len
	headerLen, err := util.BytesToInt(b[start:offset])
	if err != nil {
		return nil, nil, fmt.Errorf("convert bytes to header length: %v", err)
	}

	header := &comm.Header{}
	start = offset
	offset = start + headerLen
	if err = proto.Unmarshal(b[start:offset], header); err != nil {
		return nil, nil, fmt.Errorf("unmarshal header: %v", err)
	}

	if !g.checkClusterID(header.GetClusterID()) {
		return nil, nil, errClusterIDNotMatched
	}

	start = offset
	offset = start + comm.Int32Len
	msgLen, err := util.BytesToInt(b[start:offset])
	if err != nil {
		return nil, nil, fmt.Errorf("convert bytes to message length: %v", err)
	}

	start = offset
	offset = start + msgLen

	var checksum uint32
	if err = util.BytesToObject(b[offset:], &checksum); err != nil {
		return nil, b[start:offset], fmt.Errorf("convert bytes to checksum: %v", err)
	}

	if checksum != util.Checksum(b[:offset]) {
		return nil, b[start:offset], errChecksumNotMatched
	}
	return header, b[start:offset], nil
}

func (g *group) checkClusterID(clusterID uint64) bool {
	if g.cfg.forceNewMembers {
		return true
	}
	if clusterID == 0 || !g.cfg.isClusterInitialized() {
		return true
	}
	if clusterID != g.cfg.GetClusterID() {
		return false
	}
	return true
}

func (g *group) checkPaxosMessage(msg *comm.PaxosMsg) bool {
	t := msg.GetType()
	if t == comm.PaxosMsgType_NewValue ||
		t == comm.PaxosMsgType_PrepareReply ||
		t == comm.PaxosMsgType_AcceptReply {
		if !g.cfg.isValidNodeID(msg.GetNodeID()) {
			return false
		}
		if g.cfg.FollowerMode() {
			return false
		}
	} else if t == comm.PaxosMsgType_Prepare ||
		t == comm.PaxosMsgType_Accept {
		if !g.cfg.isClusterInitialized() {
			g.cfg.addLearnNode(msg.GetNodeID())
		}
		if !g.cfg.isValidNodeID(msg.GetNodeID()) {
			g.cfg.addLearnNode(msg.GetNodeID())
			return false
		}
		if g.cfg.FollowerMode() {
			return false
		}
	}
	return true
}

func (g *group) GetCurrentInstanceID() uint64 {
	return g.instance.GetInstanceID()
}

func (g *group) PauseReplayer() {
	g.instance.PauseReplayer()
}

func (g *group) ContinueReplayer() {
	g.instance.ContinueReplayer()
}

func (g *group) PauseCleaner() {
	g.instance.PauseCleaner()
}

func (g *group) ContinueCleaner() {
	g.instance.ContinueCleaner()
}

func (g *group) SetMaxLogCount(c int64) {
	g.instance.SetMaxLogCount(c)
}

func (g *group) GetMembers() (map[uint64]comm.Member, error) {
	if !g.cfg.isClusterInitialized() {
		return nil, comm.ErrClusterUninitialized
	}
	return g.cfg.GetMembers(), nil
}

func (g *group) SetMembers(members map[uint64]comm.Member) {
	g.cfg.setMembers(members)
}

func checkMember(member *comm.Member) error {
	if member.GetName() == "" {
		return comm.ErrMemberNameEmpty
	}
	if member.GetAddr() == "" {
		return comm.ErrMemberAddrEmpty
	}
	addr := strings.SplitN(member.GetAddr(), ":", 2)
	if len(addr) != 2 {
		return fmt.Errorf("invalid member address: %s", member.GetAddr())
	}
	port, err := strconv.Atoi(addr[1])
	if err != nil {
		return fmt.Errorf("parse member address %s: %v", member.GetAddr(), err)
	}
	id, err := network.AddrToUint64(addr[0], port)
	if err != nil {
		return err
	}
	member.NodeID = proto.Uint64(id)
	return nil
}

func (g *group) AddMember(ctx context.Context, member comm.Member) (<-chan comm.Result, error) {
	if !g.cfg.isClusterInitialized() {
		return nil, comm.ErrClusterUninitialized
	}
	if err := checkMember(&member); err != nil {
		return nil, err
	}
	members, version := g.cfg.getMembersWithVersion()
	if _, ok := members[member.GetNodeID()]; ok {
		return nil, comm.ErrMemberExists
	}
	svar := &comm.ClusterInfo{
		ClusterID: proto.Uint64(g.cfg.GetClusterID()),
		Version:   proto.Uint64(version),
	}
	svar.Members = make([]*comm.Member, len(members)+1)
	i := 0
	for _, m := range members {
		svar.Members[i] = &m
		i++
	}
	svar.Members[i] = &member
	value, err := proto.Marshal(svar)
	if err != nil {
		return nil, err
	}

	return g.Propose(ctx, comm.ClusterStateMachineID, value)
}

func (g *group) RemoveMember(ctx context.Context, member comm.Member) (<-chan comm.Result, error) {
	if !g.cfg.isClusterInitialized() {
		return nil, comm.ErrClusterUninitialized
	}
	members, version := g.cfg.getMembersWithVersion()
	if _, ok := members[member.GetNodeID()]; !ok {
		return nil, comm.ErrMemberNotExists
	}
	svar := &comm.ClusterInfo{
		ClusterID: proto.Uint64(g.cfg.GetClusterID()),
		Version:   proto.Uint64(version),
	}
	svar.Members = make([]*comm.Member, len(members)-1)
	i := 0
	for id, m := range members {
		if id != member.GetNodeID() {
			svar.Members[i] = &m
			i++
		}
	}
	value, err := proto.Marshal(svar)
	if err != nil {
		return nil, err
	}

	return g.Propose(ctx, comm.ClusterStateMachineID, value)
}

func (g *group) UpdateMember(ctx context.Context, dst, src comm.Member) (<-chan comm.Result, error) {
	if !g.cfg.isClusterInitialized() {
		return nil, comm.ErrClusterUninitialized
	}
	if err := checkMember(&dst); err != nil {
		return nil, err
	}
	members, version := g.cfg.getMembersWithVersion()
	if _, ok := members[dst.GetNodeID()]; ok {
		return nil, comm.ErrMemberExists
	}
	if m, ok := members[src.GetNodeID()]; !ok {
		return nil, comm.ErrMemberNotExists
	} else if m.GetName() == dst.GetName() &&
		m.GetAddr() == dst.GetAddr() {
		return nil, comm.ErrMemberNotModified
	}

	svar := &comm.ClusterInfo{
		ClusterID: proto.Uint64(g.cfg.GetClusterID()),
		Version:   proto.Uint64(version),
	}
	svar.Members = make([]*comm.Member, len(members))
	i := 0
	for id, member := range members {
		if id != src.GetNodeID() {
			svar.Members[i] = &member
		} else {
			svar.Members[i] = &dst
		}
		i++
	}
	value, err := proto.Marshal(svar)
	if err != nil {
		return nil, err
	}

	return g.Propose(ctx, comm.ClusterStateMachineID, value)
}

func (g *group) GetMemberCount() int {
	return g.cfg.GetMemberCount()
}
