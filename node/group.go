package node

import (
	"context"
	"errors"
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/sosozhuang/paxos/comm"
	"github.com/sosozhuang/paxos/network"
	"github.com/sosozhuang/paxos/paxos"
	"github.com/sosozhuang/paxos/store"
	"hash/crc32"
	"math/rand"
	"sync"
	"time"
)

const (
	maxValueLength = 10 * 1024 * 1024
	learnTimeout   = time.Minute
	followTimeout  = time.Second * 10
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

type groupConfig struct {
	nodeID  uint64
	groupID uint16
	//members          map[uint64]string
	followerMode     bool
	followNodeID     uint64
	forceNewMembers  bool
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
	var err error
	cfg.systemSM, err = newSystemStateMachine(cfg.nodeID, cfg.forceNewMembers, st)
	if err != nil {
		return err
	}
	//if cfg.GetClusterID() == 0 && len(cfg.members) <= 0 {
	//	return errors.New("group: members are empty")
	//}

	//cfg.systemSM.setMembers(cfg.members)

	cfg.sms = make([]comm.StateMachine, 0)
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

func (cfg *groupConfig) isClusterInitialized() bool {
	return cfg.systemSM.isInitialized()
}

func (cfg *groupConfig) GetMajorityCount() int {
	return cfg.systemSM.getMajorityCount()
}

func (cfg *groupConfig) isValidNodeID(nodeID uint64) bool {
	return cfg.systemSM.isValidNodeID(nodeID)
}

func (cfg *groupConfig) GetMemberCount() int {
	return cfg.systemSM.getMemberCount()
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

func (cfg *groupConfig) setMembers(members map[uint64]string) {
	cfg.systemSM.setMembers(members)
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
		if g.cfg.masterSM != nil {
			g.addStateMachine(g.cfg.masterSM)
		}
		g.addStateMachine(g.cfg.systemSM)
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
		return nil, errors.New("group: proposal value length exceeded limit")
	}
	if g.cfg.FollowerMode() {
		return nil, errors.New("group: follower mode can't accept proposal")
	}
	if !g.cfg.isInMemberShip() {
		return nil, errors.New("group: not in membership")
	}

	b, err := comm.ObjectToBytes(smid)
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
	return nil, errors.New("proposal channel full")
}

func (g *group) BatchPropose(context.Context, uint32, []byte, uint32) (<-chan comm.Result, error) {
	return nil, nil
}

func (g *group) handleNewValue(stopped <-chan struct{}) {
	g.wg.Add(1)
	defer func() {
		g.wg.Done()
		log.Debugf("Group %d handleNewValue stopped.", g.cfg.GetGroupID())
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
		if g.cfg.isEnableMemberShip() &&
			(g.instance.GetProposerInstanceID() == 0 || !g.cfg.isClusterInitialized()) {
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
	//if g.cfg.forceNewMembers {
	//	svar.ClusterID = proto.Uint64(g.cfg.GetClusterID())
	//} else {
	//	svar.ClusterID = proto.Uint64(g.cfg.GetNodeID() ^ uint64(rand.Uint32()))
	//}
	svar.Nodes = make([]*comm.PaxosNodeInfo, g.cfg.GetMemberCount())
	i := 0
	for k, v := range members {
		svar.Nodes[i] = &comm.PaxosNodeInfo{NodeID: proto.Uint64(uint64(k)), Addr: proto.String(v)}
		i++
	}

	value, err := proto.Marshal(svar)
	if err != nil {
		log.Errorf("Group %d marshal system var error: %v.", g.cfg.GetGroupID(), err)
		return
	}

	b, err := comm.ObjectToBytes(comm.SystemStateMachineID)
	if err != nil {
		log.Errorf("Group %d convert system smid to bytes error: %v.", g.cfg.GetGroupID(), err)
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
		log.Errorf("Group %d propose new system var error: %v.", g.cfg.GetGroupID(), ctx.Err())
	case x := <-result:
		if x.Err != nil {
			log.Errorf("Group %d propose new system var error: %v.", g.cfg.GetGroupID(), x.Err)
		} else {
			log.Infof("Group %d propose new system var result: %v.", g.cfg.GetGroupID(), x)
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
		log.Debugf("Group %d handleMessage stopped.", g.cfg.GetGroupID())
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
	headerLen, err := comm.BytesToInt(b[start:offset])
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
		return nil, nil, errors.New("cluster id error")
	}

	start = offset
	offset = start + comm.Int32Len
	msgLen, err := comm.BytesToInt(b[start:offset])
	if err != nil {
		return nil, nil, fmt.Errorf("convert bytes to message length: %v", err)
	}

	start = offset
	offset = start + msgLen

	var checksum uint32
	if err = comm.BytesToObject(b[offset:], &checksum); err != nil {
		return nil, b[start:offset], fmt.Errorf("convert bytes to checksum: %v", err)
	}

	if checksum != crc32.Checksum(b[:offset], crcTable) {
		return nil, b[start:offset], errors.New("verify message checksum error")
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

func (g *group) SetMembers(members map[uint64]string) {
	g.cfg.setMembers(members)
}

func (g *group) AddMember(ctx context.Context, nodeID uint64, addr string) (<-chan comm.Result, error) {
	if !g.cfg.isClusterInitialized() {
		return nil, errors.New("group: membership uninitialized")
	}
	members, version := g.cfg.getMembersWithVersion()
	if _, ok := members[nodeID]; ok {
		return nil, errors.New("group: member exist")
	}
	svar := &comm.SystemVar{
		ClusterID: proto.Uint64(g.cfg.GetClusterID()),
		Version:   proto.Uint64(version),
	}
	svar.Nodes = make([]*comm.PaxosNodeInfo, g.cfg.GetMemberCount()+1)
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
	if !g.cfg.isClusterInitialized() {
		return nil, errors.New("group: membership uninitialized")
	}
	members, version := g.cfg.getMembersWithVersion()
	if _, ok := members[nodeID]; !ok {
		return nil, errors.New("group: member not exist")
	}
	svar := &comm.SystemVar{
		ClusterID: proto.Uint64(g.cfg.GetClusterID()),
		Version:   proto.Uint64(version),
	}
	svar.Nodes = make([]*comm.PaxosNodeInfo, g.cfg.GetMemberCount()-1)
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
	if !g.cfg.isClusterInitialized() {
		return nil, errors.New("group: membership uninitialized")
	}
	members, version := g.cfg.getMembersWithVersion()
	if _, ok := members[dst]; ok {
		return nil, errors.New("group: member exist")
	}
	if _, ok := members[src]; !ok {
		return nil, errors.New("group: member not exist")
	}
	svar := &comm.SystemVar{
		ClusterID: proto.Uint64(g.cfg.GetClusterID()),
		Version:   proto.Uint64(version),
	}
	svar.Nodes = make([]*comm.PaxosNodeInfo, g.cfg.GetMemberCount())
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

func (g *group) GetNodeCount() int {
	return g.cfg.GetMemberCount()
}
