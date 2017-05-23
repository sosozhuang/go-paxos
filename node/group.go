package node

import (
	"context"
	"errors"
	"github.com/gogo/protobuf/proto"
	"github.com/sosozhuang/paxos/comm"
	"github.com/sosozhuang/paxos/network"
	"github.com/sosozhuang/paxos/paxos"
	"github.com/sosozhuang/paxos/store"
	"hash/crc32"
	"math/rand"
	"sync"
	"time"
	"fmt"
)

const (
	maxValueLength = 10 * 1024 * 1024
	learnTimeout   = time.Minute
	followTimeout  = time.Second * 10
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

type groupConfig struct {
	nodeID           uint64
	groupID          uint16
	members          map[uint64]string
	followerMode     bool
	followNodeID     uint64
	enableMemberShip bool
	enableReplayer   bool
	sms              []comm.StateMachine
	masterSM         comm.StateMachine
	systemSM         *systemStateMachine
	lmu              sync.Mutex
	learnNodes       map[uint64]time.Time
	fmu              sync.Mutex
	followers        map[uint64]time.Time
}

func (cfg *groupConfig) validate(st store.Storage) error {
	if len(cfg.members) <= 0 {
		return fmt.Errorf("group %d member list is empty", cfg.groupID)
	}

	if len(cfg.sms) <= 0 {
		return fmt.Errorf("group %d state machine list is empty", cfg.groupID)
	}
	var err error
	cfg.systemSM, err = newSystemStateMachine(cfg.nodeID, st)
	if err != nil {
		return err
	}
	cfg.systemSM.addNodes(cfg.members)

	cfg.learnNodes = make(map[uint64]time.Time)
	cfg.followers = make(map[uint64]time.Time)
	return nil
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
	return cfg.systemSM.getClusterID()
}

func (cfg *groupConfig) GetMajorityCount() int {
	return cfg.systemSM.getMajorityCount()
}

func (cfg *groupConfig) isValidNodeID(nodeID uint64) bool {
	return cfg.systemSM.isValidNodeID(nodeID)
}

func (cfg *groupConfig) getNodeCount() int {
	return cfg.systemSM.getNodeCount()
}

func (cfg *groupConfig) getMembersWithVersion() (map[uint64]string, uint64) {
	return cfg.systemSM.getMembersWithVersion()
}

func (cfg *groupConfig) isInMemberShip() bool {
	return cfg.systemSM.isInMemberShip()
}

func (cfg *groupConfig) IsEnableReplayer() bool {
	return cfg.enableReplayer
}

func (cfg *groupConfig) isEnableMemberShip() bool {
	return cfg.enableMemberShip
}

func (cfg *groupConfig) addLearnNode(id uint64) {
	if _, ok := cfg.systemSM.getMembers()[id]; ok {
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

func (cfg *groupConfig) GetMembers() map[uint64]string {
	return cfg.systemSM.getMembers()
}

func (cfg *groupConfig) GetSystemCheckpoint() ([]byte, error) {
	return cfg.systemSM.GetCheckpoint()
}

func (cfg *groupConfig) UpdateSystemByCheckpoint(b []byte) error {
	return cfg.systemSM.UpdateByCheckpoint(b)
}

func (cfg *groupConfig) GetMasterCheckpoint() ([]byte, error) {
	return cfg.masterSM.GetCheckpoint()
}

func (cfg *groupConfig) UpdateMasterByCheckpoint(b []byte) error {
	return cfg.masterSM.UpdateByCheckpoint(b)
}

type group struct {
	cfg       *groupConfig
	instance  comm.Instance
	proposeCh chan context.Context
	msgCh     chan []byte
	wg        sync.WaitGroup
}

func newGroup(cfg groupConfig, sender comm.Sender, st store.Storage) (comm.Group, error) {
	var err error
	if err = cfg.validate(st); err != nil {
		return nil, err
	}

	group := &group{
		cfg:       &cfg,
		proposeCh: make(chan context.Context, 100),
		msgCh:     make(chan []byte, 100),
	}

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
		g.addStateMachine(g.cfg.masterSM, g.cfg.systemSM)
		g.addStateMachine(g.cfg.sms...)
		go g.handleNewValue(stopped)
		go g.handleMessage(stopped)
		return g.instance.Start(ctx, stopped)
	}
}

func (g *group) Stop() {
	close(g.proposeCh)
	close(g.msgCh)
	g.instance.Stop()
	g.wg.Wait()
}

func (g *group) Propose(ctx context.Context, smid uint32, value []byte) (<-chan comm.Result, error) {
	if len(value) >= maxValueLength {
		return nil, errors.New("value too large")
	}
	if g.cfg.FollowerMode() {
		return nil, errors.New("in follower mode")
	}
	if !g.cfg.isInMemberShip() {
		return nil, errors.New("not in members")
	}
	b, err := comm.ObjectToBytes(smid)
	if err != nil {
		return nil, err
	}
	ctx = context.WithValue(ctx, "value", append(b, value...))
	result := make(chan comm.Result)
	ctx = context.WithValue(ctx, "result", result)
	select {
	case g.proposeCh <- ctx:
		return result, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (g *group) BatchPropose(context.Context, uint32, []byte, uint32) (<-chan comm.Result, error) {
	return nil, nil
}

func (g *group) handleNewValue(stopped <-chan struct{}) {
	g.wg.Add(1)
	defer g.wg.Done()
	for {
		select {
		case <-stopped:
			return
		default:
		}
		if !g.instance.IsReadyForNewValue() {
			time.Sleep(time.Millisecond * 50)
			continue
		}

		ctx, ok := <-g.proposeCh
		if !ok {
			return
		}
		if d, ok := ctx.Deadline(); ok && d.Before(time.Now()) {
			continue
		}
		// todo: dont put in for loop, check when start
		if g.cfg.isEnableMemberShip() &&
			(g.instance.GetProposerInstanceID() == 0 || g.cfg.GetClusterID() == 0) {
			g.newSystemValue()
		}
		g.instance.NewValue(ctx)

	}
}

func (g *group) newSystemValue() {
	members, version := g.cfg.getMembersWithVersion()
	svar := &comm.SystemVar{
		ClusterID: proto.Uint64(g.cfg.GetNodeID() ^ uint64(rand.Uint32()) + uint64(rand.Uint32())),
		Version:   proto.Uint64(version),
	}
	svar.Nodes = make([]*comm.PaxosNodeInfo, g.cfg.getNodeCount())
	i := 0
	for k, v := range members {
		svar.Nodes[i] = &comm.PaxosNodeInfo{NodeID: proto.Uint64(uint64(k)), Addr: proto.String(v)}
		i++
	}

	value, err := proto.Marshal(svar)
	if err != nil {
		log.Error(err)
		return
	}

	b, err := comm.ObjectToBytes(comm.SystemStateMachineID)
	if err != nil {
		log.Error(err)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), proposeTimeout)
	defer cancel()
	ctx = context.WithValue(ctx, "value", append(b, value...))
	result := make(chan comm.Result)
	ctx = context.WithValue(ctx, "result", result)

	g.instance.NewValue(ctx)
}

func (g *group) addStateMachine(sms ...comm.StateMachine) {
	g.instance.AddStateMachine(sms...)
}

func (g *group) ReceiveMessage(b []byte) {
	g.msgCh <- b
}

func (g *group) handleMessage(stopped <-chan struct{}) {
	g.wg.Add(1)
	defer g.wg.Done()
	for b := range g.msgCh {
		select {
		case <-stopped:
			return
		default:
		}
		header, err := g.unpack(b)
		if err != nil {
			log.Error(err)
			continue
		}

		switch header.GetType() {
		case comm.MsgType_Paxos:
			msg := &comm.PaxosMsg{}
			if err := proto.Unmarshal(b, msg); err != nil {
				log.Error(err)
				continue
			}
			if !g.checkPaxosMessage(msg) {
				continue
			}
			g.instance.ReceivePaxosMessage(msg)
		case comm.MsgType_Checkpoint:
			msg := &comm.CheckpointMsg{}
			if err := proto.Unmarshal(b, msg); err != nil {
				log.Error(err)
				continue
			}
			g.instance.ReceiveCheckpointMessage(msg)
		}
	}
}

func (g *group) unpack(b []byte) (*comm.Header, error) {
	headerLen, err := comm.BytesToInt(b[:comm.Int32Len])
	if err != nil {
		return nil, err
	}

	header := &comm.Header{}
	start := comm.Int32Len
	offset := start + headerLen
	if err = proto.Unmarshal(b[start:offset], header); err != nil {
		return nil, err
	}

	if !g.checkHeader(header) {
		return nil, errors.New("check header error")
	}

	start = offset
	offset = start + comm.Int32Len
	msgLen, err := comm.BytesToInt(b[start:offset])
	if err != nil {
		return nil, err
	}

	start = offset
	offset = start + msgLen

	var checksum uint32
	if err = comm.BytesToObject(b[offset:], &checksum); err != nil {
		return nil, err
	}

	if checksum != crc32.Checksum(b[:offset], crcTable) {
		return nil, errors.New("checksum error")
	}
	return header, nil
}

func (g *group) checkHeader(h *comm.Header) bool {
	if h.GetClusterID() == 0 || g.cfg.GetClusterID() == 0 {
		return true
	}
	if h.GetClusterID() != g.cfg.GetClusterID() {
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
		if !g.cfg.isValidNodeID(msg.GetNodeID()) ||
			g.cfg.GetClusterID() == 0 {
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

func (g *group) SetMaxLogCount(c int) {
	g.instance.SetMaxLogCount(c)
}

func (g *group) AddMember(ctx context.Context, nodeID uint64, addr string) (<-chan comm.Result, error) {
	if g.cfg.GetClusterID() == 0 {
		return nil, errors.New("cluster id zero")
	}
	members, version := g.cfg.getMembersWithVersion()
	if _, ok := members[nodeID]; ok {
		return nil, errors.New("node exist")
	}
	svar := &comm.SystemVar{
		ClusterID: proto.Uint64(g.cfg.GetClusterID()),
		Version:   proto.Uint64(version),
	}
	svar.Nodes = make([]*comm.PaxosNodeInfo, g.cfg.getNodeCount()+1)
	i := 0
	for k, v := range members {
		svar.Nodes[i] = &comm.PaxosNodeInfo{NodeID: proto.Uint64(uint64(k)), Addr: proto.String(v)}
		i++
	}
	svar.Nodes[i] = &comm.PaxosNodeInfo{NodeID: proto.Uint64(uint64(nodeID)), Addr: proto.String(addr)}
	value, err := proto.Marshal(svar)
	if err != nil {
		return nil, err
	}

	return g.Propose(ctx, comm.SystemStateMachineID, value)
}

func (g *group) RemoveMember(ctx context.Context, nodeID uint64) (<-chan comm.Result, error) {
	if g.cfg.GetClusterID() == 0 {
		return nil, errors.New("cluster id zero")
	}
	members, version := g.cfg.getMembersWithVersion()
	if _, ok := members[nodeID]; !ok {
		return nil, errors.New("node not exist")
	}
	svar := &comm.SystemVar{
		ClusterID: proto.Uint64(g.cfg.GetClusterID()),
		Version:   proto.Uint64(version),
	}
	svar.Nodes = make([]*comm.PaxosNodeInfo, g.cfg.getNodeCount()-1)
	i := 0
	for k, v := range members {
		if k != nodeID {
			svar.Nodes[i] = &comm.PaxosNodeInfo{NodeID: proto.Uint64(uint64(k)), Addr: proto.String(v)}
			i++
		}
	}
	value, err := proto.Marshal(svar)
	if err != nil {
		return nil, err
	}

	return g.Propose(ctx, comm.SystemStateMachineID, value)
}

func (g *group) ChangeMember(ctx context.Context, dst uint64, addr string, src uint64) (<-chan comm.Result, error) {
	if g.cfg.GetClusterID() == 0 {
		return nil, errors.New("cluster id zero")
	}
	members, version := g.cfg.getMembersWithVersion()
	if _, ok := members[dst]; ok {
		return nil, errors.New("node exist")
	}
	if _, ok := members[src]; !ok {
		return nil, errors.New("node not exist")
	}
	svar := &comm.SystemVar{
		ClusterID: proto.Uint64(g.cfg.GetClusterID()),
		Version:   proto.Uint64(version),
	}
	svar.Nodes = make([]*comm.PaxosNodeInfo, g.cfg.getNodeCount())
	i := 0
	for k, v := range members {
		if k != src {
			svar.Nodes[i] = &comm.PaxosNodeInfo{NodeID: proto.Uint64(uint64(k)), Addr: proto.String(v)}
		} else {
			svar.Nodes[i] = &comm.PaxosNodeInfo{NodeID: proto.Uint64(uint64(dst)), Addr: proto.String(addr)}
		}
		i++
	}
	value, err := proto.Marshal(svar)
	if err != nil {
		return nil, err
	}

	return g.Propose(ctx, comm.SystemStateMachineID, value)
}
