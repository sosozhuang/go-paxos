package node

import (
	"context"
	"fmt"
	"github.com/sosozhuang/paxos/comm"
	"github.com/sosozhuang/paxos/config"
	"github.com/sosozhuang/paxos/election"
	"github.com/sosozhuang/paxos/logger"
	"github.com/sosozhuang/paxos/network"
	"github.com/sosozhuang/paxos/paxos"
	"github.com/sosozhuang/paxos/store"
	"sync"
	"time"
	"github.com/sosozhuang/paxos/util"
)

var log = logger.PaxosLogger

type NodeConfig struct {
	Sync bool
	SyncDuration time.Duration
	SyncInterval uint32

	EnableMemberShip bool
	GroupCount uint32
	ListenAddr string
	ListenPort int
	Peers []string
	id comm.NodeID

}

func (cfg *NodeConfig) Validate() error {
	return nil
}

type node struct {
	cfg           *config.Config
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
	id            comm.NodeID
}

func NewNode(cfg config.Config) (n *node, err error) {
	if err = cfg.Validate(); err != nil {
		return nil, err
	}
	n = &node{
		ready: make(chan struct{}),
		done: make(chan struct{}),
		stopped: make(chan struct{}),
		errChan: make(chan error, 10),
		id: comm.NodeID(util.AddrToInt64(cfg.ListenAddr, cfg.ListenPort)),
	}

	defer func() {
		if err != nil && n != nil {
			n.stop()
			n = nil
		}
	}()

	storageType, err := store.ParseStorageType(n.cfg.DataStore)
	if err != nil {
		return
	}
	storageCfg := store.StorageConfig{
		DataDir: n.cfg.DataDir,
		Type:    storageType,
	}
	n.storage, err = store.NewDiskStorage(storageCfg)
	if err != nil {
		log.Error(err)
		return
	}

	networkCfg := network.NetWorkConfig{}
	n.network, err = network.NewPeerNetWork(networkCfg, n)
	if err != nil {
		log.Error(err)
		return
	}

	if n.cfg.EnableMater {
		n.masters = make([]election.Master, n.cfg.GroupCount)
		var master election.Master
		masterCfg := election.MasterConfig{}
		for i := 0; i < n.cfg.GroupCount; i++ {
			if master, err = election.NewMaster(masterCfg, n, i, n.storage); err != nil {
				log.Error(err)
				return err
			}
			n.masters[i] = master
		}
	}

	n.groups = make([]Group, n.cfg.GroupCount)
	var group Group
	groupCfg := GroupConfig{}
	for i := 0; i < n.cfg.GroupCount; i++ {
		if group, err = newGroup(groupCfg, n); err != nil {
			log.Error(err)
			return err
		}
		n.groups[i] = group
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

//func (n *node) Start1() error {
//	defer func() {
//		// todo: stop things have started if return error
//	}()
//	var err error
//	storageType, err := store.ParseStorageType(n.DataStore)
//	if err != nil {
//		return err
//	}
//	storageCfg := store.StorageConfig{
//		DataDir: n.DataDir,
//		Type:    storageType,
//	}
//
//	n.storage, err = store.NewDiskStorage(storageCfg)
//	if err != nil {
//		log.Error(err)
//		return err
//	}
//	log.Debug("Disk storage.")
//
//	networkCfg := network.NetWorkConfig{}
//	n.network, err = network.NewPeerNetWork(networkCfg, n)
//	if err != nil {
//		log.Error(err)
//		return err
//	}
//
//	if n.EnableMater {
//		n.masters = make([]election.Master, n.cfg.GroupCount)
//		var master election.Master
//		masterCfg := election.MasterConfig{}
//		for i := 0; i < n.cfg.GroupCount; i++ {
//			if master, err = election.NewMaster(masterCfg, n, i, n.storage); err != nil {
//				log.Error(err)
//				return err
//			}
//			n.masters[i] = master
//		}
//	}
//
//	var group Group
//	groupCfg := GroupConfig{}
//	for i := 0; i < n.cfg.GroupCount; i++ {
//		if group, err = newGroup(groupCfg); err != nil {
//			log.Error(err)
//			return err
//		}
//		n.groups = append(n.groups, group)
//	}
//
//	if n.EnableBatch {
//		var batch Batch
//		batchCfg := BatchConfig{}
//		for i := 0; i < n.cfg.GroupCount; i++ {
//			if batch, err = newBatch(batchCfg); err != nil {
//				log.Error(err)
//				return err
//			}
//			n.batches = append(n.batches, batch)
//		}
//	}
//
//	if err = n.initStateMachine(); err != nil {
//		log.Error(err)
//		return err
//	}
//
//	for _, group := range n.groups {
//		if err := group.Init(); err != nil {
//			log.Error(err)
//			return err
//		}
//	}
//
//	if err = n.network.Start(); err != nil {
//		log.Error(err)
//		return err
//	}
//
//	return nil
//}

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
	if n.EnableMater && !n.FollowerMode {
		for _, master := range n.masters {
			master.Start()
		}
	}
	return nil
}

func (n *node) checkGroup(groupID comm.GroupID) bool {
	return groupID < 0 || groupID >= len(n.groups)
}

func (n *node) Propose(groupID comm.GroupID, value []byte, instanceID comm.InstanceID) error {
	return n.ProposeWithCtx(context.Background(), groupID, value, instanceID)
}

func (n *node) ProposeWithCtx(ctx context.Context, groupID comm.GroupID, value []byte, instanceID comm.InstanceID) error {
	if !n.checkGroup(groupID) {
		return fmt.Errorf("group %d out of range", groupID)
	}
	return nil
}
func (n *node) BatchPropose(groupID comm.GroupID, value []byte, instanceID comm.InstanceID, batch uint32) error {
	return n.BatchProposeWithCtx(context.Background(), groupID, value, instanceID, batch)
}
func (n *node) BatchProposeWithCtx(ctx context.Context, groupID uint16, value []byte, instanceID comm.InstanceID, batch uint32) error {
	if !n.checkGroup(groupID) {
		return fmt.Errorf("group %d out of range", groupID)
	}
	return nil
}

func (n *node) CurrentInstanceID(groupID comm.GroupID) (comm.InstanceID, error) {
	if !n.checkGroup(groupID) {
		return comm.InstanceID(0), fmt.Errorf("group %d out of range", groupID)
	}
	return n.groups[groupID].GetCurrentInstanceID()
}

func (n *node) GetNodeID() comm.NodeID {
	return n.id
}

func (n *node) handleMessage(msg []byte, length int) error {
	group := uint16(0)
	if !n.checkGroup(group) {
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
	if !n.checkGroup(groupID) {
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
