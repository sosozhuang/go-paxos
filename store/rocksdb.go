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
package store

import (
	"errors"
	"github.com/tecbot/gorocksdb"
	"path"
	"runtime"
	"strconv"
	"github.com/sosozhuang/paxos/comm"
	"context"
	"sync"
	"github.com/gogo/protobuf/proto"
)

type rdbs []*rocksDB

type rocksDB struct {
	*StorageConfig
	*gorocksdb.DB
	*gorocksdb.Options
	*gorocksdb.ReadOptions
	*gorocksdb.WriteOptions
	*logStore
}

type comparator struct{}

func (*comparator) Compare(a, b []byte) int {
	i, err := strconv.ParseUint(string(a), 10, 64)
	if err != nil {
		log.Fatalf("Convert %s to uint error: %s\n", string(a), err)
		return 0
	}
	j, err := strconv.ParseUint(string(b), 10, 64)
	if err != nil {
		log.Fatalf("Convert %s to uint error: %s\n", string(b), err)
		return 0
	}
	if i == j {
		return 0
	} else if i < j {
		return -1
	}
	return 1
}

func (*comparator) Name() string {
	return "paxos.comparator"
}

func newGroupRocksDB(cfg *StorageConfig) (rdbs rdbs, err error) {
	rdbs = make([]rocksDB, cfg.GroupCount)
	if len(rdbs) == 0 || rdbs == nil {
		return nil, errors.New("invalid group count")
	}
	defer func() {
		if err != nil && rdbs != nil {
			rdbs.Close()
			rdbs = nil
		}
	}()

	c := &comparator{}
	for i := range rdbs {
		dir := path.Join(cfg.DataDir, strconv.Itoa(i))
		rdbs[i], err = newRocksDB(cfg, dir, c)
		if err != nil {
			return
		}
	}
	return
}

func (r *rdbs) Open(ctx context.Context, stopped <-chan struct{}) error {
	ch := make(chan error)
	var wg sync.WaitGroup
	wg.Add(len(r))
	for _, db := range []*rocksDB(r) {
		go func() {
			defer wg.Done()
			if err := db.Open(ctx, stopped); err != nil {
				ch <- err
			}
		}()
	}
	go func() {
		wg.Wait()
		close(ch)
	}()

	select {
	case <-stopped:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case err, ok := <-ch:
		if !ok {
			return nil
		}
		return err
	}
}

func (r *rdbs) Close() {
	for _, db := range []*rocksDB(r) {
		if db != nil {
			db.Close()
		}
	}
}

func (rs *rdbs) Get(groupID comm.GroupID, instanceID comm.InstanceID) ([]byte, error) {
	return rs[groupID].Get(instanceID)
}
func (rs *rdbs) Set(groupID comm.GroupID, instanceID comm.InstanceID, value []byte, opts *SetOptions) error {
	return rs[groupID].Set(instanceID, value, opts)
}
func (rs *rdbs) Delete(groupID comm.GroupID, instanceID comm.InstanceID, opts *DeleteOptions) error {
	return rs[groupID].Delete(instanceID, opts)
}
func (rs *rdbs) GetMaxInstanceID(groupID comm.GroupID) (comm.InstanceID, error) {
	return rs[groupID].GetMaxInstanceID()
}
func (rs *rdbs) SetMinChosenInstanceID(groupID comm.GroupID, instanceID comm.InstanceID, opts *SetOptions) error {
	return rs[groupID].SetMinChosenInstanceID(instanceID, opts)
}
func (rs *rdbs) GetMinChosenInstanceID(groupID comm.GroupID) (uint64, error) {
	return rs[groupID].GetMinChosenInstanceID()
}
func (rs *rdbs) ClearAllLog(groupID comm.GroupID) error {
	return rs[groupID].ClearAllLog()
}
func (rs *rdbs) SetSystemVariables(g comm.GroupID, v *comm.SystemVariables) error {
	return rs[g].SetSystemVariables(v)
}
func (rs *rdbs) GetSystemVariables(g comm.GroupID) (*comm.SystemVariables, error) {
	return rs[g].GetSystemVariables()
}
func (rs *rdbs) SetMasterVariables(g comm.GroupID, v *comm.MasterVariables) error {
	return rs[g].SetMasterVariables(v)
}
func (rs *rdbs) GetMasterVariables(g comm.GroupID) (*comm.MasterVariables, error) {
	return rs[g].GetMasterVariables()
}

func newRocksDB(cfg *StorageConfig, dir string, comparator gorocksdb.Comparator) (rdb *rocksDB, err error) {
	rdb = new(rocksDB)
	defer func() {
		if err != nil && rdb != nil {
			rdb.Close()
			rdb = nil
		}
	}()

	rdb.Options = gorocksdb.NewDefaultOptions()
	rdb.Options.SetCreateIfMissing(true)
	rdb.Options.IncreaseParallelism(runtime.NumCPU())
	rdb.Options.OptimizeLevelStyleCompaction(0)
	rdb.Options.SetComparator(comparator)
	//todo: create option for every group?
	//rdb.Options.SetWriteBufferSize(1024 * 1024 + group * 10 * 1024)

	rdb.DB, err = gorocksdb.OpenDb(rdb.Options, dir)
	if err != nil {
		return
	}

	rdb.ReadOptions = gorocksdb.NewDefaultReadOptions()
	rdb.WriteOptions = gorocksdb.NewDefaultWriteOptions()
	rdb.WriteOptions.SetSync(!cfg.DisableSync)
	rdb.WriteOptions.DisableWAL(cfg.DisableWAL)

	rdb.logStore, err = newLogStore(dir, rdb)
	return
}

func (r *rocksDB) Open(ctx context.Context, stopped <-chan struct{}) error {
	ch := make(chan error)
	go func() {
		if err := r.logStore.Open(ctx, stopped); err != nil {
			ch <- err
		} else {
			close(ch)
		}

	}()

	select {
	case <-stopped:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case err, ok := <-ch:
		if !ok {
			return nil
		}
		return err
	}
}

func (r *rocksDB) Get(instanceID comm.InstanceID) ([]byte, error) {
	return make([]byte, 0), nil
}

func (r *rocksDB) Set(instanceID comm.InstanceID, value []byte, opts *SetOptions) error {
	return nil
}
func (r *rocksDB) Delete(instanceID comm.InstanceID, opts *DeleteOptions) error {
	return nil
}
func (r *rocksDB) GetMaxInstanceID() (comm.InstanceID, error) {
	it := r.NewIterator(r.ReadOptions)
	defer it.Close()
	for it.SeekToLast(); it.Valid(); it.Prev() {
		id, err := strconv.ParseInt(string(it.Key()), 10, 64)
		if err != nil {
			return comm.InstanceID(0), err
		}
		if id != minChosenKey &&
			id != systemVariablesKey &&
			id != masterVariablesKey {
			return id, nil
		}
	}
	return comm.InstanceID(0), nil
}
func (r *rocksDB) SetMinChosenInstanceID(instanceID comm.InstanceID, opts *SetOptions) error {
	return nil
}
func (r *rocksDB) GetMinChosenInstanceID() (uint64, error) {
	return uint64(0), nil
}
func (r *rocksDB) ClearAllLog() error {
	return nil
}
func (r *rocksDB) SetSystemVariables(v []byte) error {
	key := strconv.Itoa(systemVariablesKey)
	value, err := proto.Marshal(v)
	if err != nil {
		return err
	}
	return r.DB.Put(r.WriteOptions, key, value)
}
func (r *rocksDB) GetSystemVariables() (*comm.SystemVariables, error) {
	key := strconv.Itoa(systemVariablesKey)
	value, err := r.DB.Get(r.ReadOptions, key)
	if err != nil {
		return nil, err
	}
	v := &comm.SystemVariables{}
	if err := proto.Unmarshal(value.Data(), v); err != nil {
		return nil, err
	}
	return v, nil
}
func (r *rocksDB) SetMasterVariables(v *comm.MasterVariables) error {
	key := strconv.Itoa(masterVariablesKey)
	value, err := proto.Marshal(v)
	if err != nil {
		return err
	}
	return r.DB.Put(r.WriteOptions, key, value)
}
func (r *rocksDB) GetMasterVariables() (*comm.MasterVariables, error) {
	key := strconv.Itoa(masterVariablesKey)
	value, err := r.DB.Get(r.ReadOptions, key)
	if err != nil {
		return nil, err
	}
	v := &comm.MasterVariables{}
	if err := proto.Unmarshal(value.Data(), v); err != nil {
		return nil, err
	}
	return v, nil
}

func (r *rocksDB) Close() {
	if r.Options != nil {
		r.Options.Destroy()
		r.Options = nil
	}
	if r.ReadOptions != nil {
		r.ReadOptions.Destroy()
		r.ReadOptions = nil
	}
	if r.WriteOptions != nil {
		r.WriteOptions.Destroy()
		r.WriteOptions = nil
	}
	for r.DB != nil {
		r.DB.Close()
		r.DB = nil
	}
	if r.logStore != nil {
		r.logStore.Close()
	}
}
