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
package storage

import (
	"errors"
	"github.com/tecbot/gorocksdb"
	"path"
	"runtime"
	"strconv"
	"github.com/sosozhuang/go-paxos/comm"
	"context"
	"sync"
	"github.com/gogo/protobuf/proto"
	"os"
	"math"
	"math/rand"
	"fmt"
	"github.com/sosozhuang/go-paxos/util"
)

type rdbs []*rocksDB

type rocksDB struct {
	dir string
	*StorageConfig
	*gorocksdb.DB
	*gorocksdb.Options
	*gorocksdb.ReadOptions
	*gorocksdb.WriteOptions
	*logStore
}

type comparator struct{}

func (*comparator) Compare(a, b []byte) int {
	var i, j uint64
	if err := util.BytesToObject(a, &i); err != nil {
		log.Error("Comparator convert", a, "to uint64:", err)
		return 0
	}
	if err := util.BytesToObject(b, &j); err != nil {
		log.Error("Comparator convert", b, "to uint64:", err)
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

func newRocksDBGroup(cfg *StorageConfig) (rdbs rdbs, err error) {
	rdbs = make([]*rocksDB, cfg.GroupCount)
	if len(rdbs) == 0 || rdbs == nil {
		return nil, errors.New("rocksdb: invalid group count")
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

func (rs rdbs) Open(ctx context.Context, stopped <-chan struct{}) error {
	ch := make(chan error, len(rs))
	var wg sync.WaitGroup
	wg.Add(len(rs))
	for _, db := range rs {
		go func(db *rocksDB) {
			defer wg.Done()
			if err := db.open(stopped); err != nil {
				ch <- err
			}
		}(db)
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
			log.Debug("Rocksdb group storage opened.")
			return nil
		}
		return err
	}
}

func (rs rdbs) Close() {
	for _, db := range rs {
		if db != nil {
			db.close()
		}
	}
	log.Debug("Rocksdb group storage closed.")
}

func (rs rdbs) GetStorage(id uint16) Storage {
	return rs[id]
}

func newRocksDB(cfg *StorageConfig, dir string, comparator gorocksdb.Comparator) (rdb *rocksDB, err error) {
	rdb = new(rocksDB)
	defer func() {
		if err != nil && rdb != nil {
			rdb.close()
			rdb = nil
		}
	}()

	rdb.dir = dir
	rdb.StorageConfig = cfg
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
	rdb.WriteOptions.SetSync(cfg.Sync)
	rdb.WriteOptions.DisableWAL(cfg.DisableWAL)

	rdb.logStore, err = newLogStore(dir, rdb)
	return
}

func (r *rocksDB) open(stopped <-chan struct{}) error {
	return r.logStore.open(stopped)
}

func (r *rocksDB) GetDir() string{
	return r.dir
}

func (r *rocksDB) Get(instanceID uint64) ([]byte, error) {
	key, err := util.ObjectToBytes(instanceID)
	if err != nil {
		return nil, fmt.Errorf("rocksdb: convert %d to bytes: %v", instanceID, err)
	}
	v, err := r.DB.Get(r.ReadOptions, key)
	if err != nil {
		return nil, fmt.Errorf("rocksdb: get value: %v", err)
	}
	defer v.Free()
	if v.Size() == 0 {
		return nil, ErrNotFound
	}
	id, value, err := r.logStore.read(v.Data())
	if err != nil {
		return nil, err
	}
	if id != instanceID {
		return nil, errInstanceIDNotMatched
	}
	return value, nil
}

func (r *rocksDB) Set(instanceID uint64, value []byte) error {
	v, err := r.logStore.append(instanceID, value)
	if err != nil {
		return err
	}
	key, err := util.ObjectToBytes(instanceID)
	if err != nil {
		return fmt.Errorf("rocksdb: convert %d to bytes: %v", instanceID, err)
	}
	if err = r.DB.Put(r.WriteOptions, key, v); err != nil {
		return fmt.Errorf("rocksdb: set value: %v", err)
	}
	return nil
}

func (r *rocksDB) Delete(instanceID uint64) error {
	key, err := util.ObjectToBytes(instanceID)
	if err != nil {
		return fmt.Errorf("rocksdb: convert %d to bytes: %v", instanceID, err)
	}
	value, err := r.DB.Get(r.ReadOptions, key)
	if err != nil {
		return fmt.Errorf("rocksdb: get value: %v", err)
	}
	defer value.Free()
	if value.Size() == 0 {
		return nil
	}

	if rand.Uint32() % 100 < 1 {
		if err = r.logStore.delete(value.Data()); err != nil {
			return err
		}
	}

	if err = r.DB.Delete(r.WriteOptions, key); err != nil {
		return fmt.Errorf("rocksdb: delete value: %v", err)
	}
	return nil
}

func (r *rocksDB) ForceDelete(instanceID uint64) error {
	key, err := util.ObjectToBytes(instanceID)
	if err != nil {
		return fmt.Errorf("rocksdb: convert %d to bytes: %v", instanceID, err)
	}
	value, err := r.DB.Get(r.ReadOptions, key)
	if err != nil {
		return fmt.Errorf("rocksdb: get value: %v", err)
	}
	defer value.Free()
	if value.Size() == 0 {
		return nil
	}

	if err = r.logStore.truncate(value.Data()); err != nil {
		return err
	}

	if err = r.DB.Delete(r.WriteOptions, key); err != nil {
		return fmt.Errorf("rocksdb: delete value: %v", err)
	}
	return nil
}

func (r *rocksDB) GetMaxInstanceID() (instanceID uint64, err error) {
	it := r.NewIterator(r.ReadOptions)
	defer it.Close()
	for it.SeekToLast(); it.Valid(); it.Prev() {
		key := it.Key()
		if err = util.BytesToObject(key.Data(), &instanceID); err != nil {
			key.Free()
			err = fmt.Errorf("rocksdb: convert bytes to instance id: %v", err)
			return
		}
		key.Free()
		if instanceID != math.MaxUint64 &&
			instanceID != math.MaxUint64 -1 &&
			instanceID != math.MaxUint64 -2 {
			return
		}
	}
	instanceID = 0
	err = ErrNotFound
	return
}

func (r *rocksDB) SetMinChosenInstanceID(instanceID uint64) error {
	value, err := util.ObjectToBytes(instanceID)
	if err != nil {
		return fmt.Errorf("rocksdb: convert %d to bytes: %v", instanceID, err)
	}
	if err = r.DB.Put(r.WriteOptions, minChosenKey, value); err != nil {
		return fmt.Errorf("rocksdb: set value: %v", err)
	}
	return nil
}

func (r *rocksDB) GetMinChosenInstanceID() (uint64, error) {
	var instanceID uint64
	value, err := r.DB.Get(r.ReadOptions, minChosenKey)
	if err != nil {
		return instanceID, fmt.Errorf("rocksdb: get value: %v", err)
	}
	defer value.Free()
	if value.Size() == 0 {
		return instanceID, nil
	}
	if err = util.BytesToObject(value.Data(), &instanceID); err != nil {
		return instanceID, fmt.Errorf("rocksdb: convert bytes to instance id: %v", err)
	}
	return instanceID, nil
}

func (r *rocksDB) Recreate() error {
	ci, err := r.GetClusterInfo()
	if err != nil && err != ErrNotFound {
		return err
	}
	li, err := r.GetLeaderInfo()
	if err != nil && err != ErrNotFound {
		return err
	}
	backup := path.Clean(r.dir) + ".bak"
	if err = os.RemoveAll(backup); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("rocksdb: remove %s: %v", backup, err)
	}
	if err = os.Rename(r.dir, backup); err != nil {
		return fmt.Errorf("rocksdb: rename %s to %s: %v", r.dir, backup, err)
	}

	r.closeDB()
	r.DB, err = gorocksdb.OpenDb(r.Options, r.dir)
	if err != nil {
		return fmt.Errorf("rocksdb: open db: %v", err)
	}
	r.logStore, err = newLogStore(r.dir, r)
	if err != nil {
		return err
	}

	if ci != nil {
		if err = r.SetClusterInfo(ci); err != nil {
			return err
		}
	}
	if li != nil {
		if err = r.SetLeaderInfo(li); err != nil {
			return err
		}
	}
	return nil
}

func (r *rocksDB) SetClusterInfo(v *comm.ClusterInfo) error {
	value, err := proto.Marshal(v)
	if err != nil {
		return fmt.Errorf("rocksdb: marshal cluster info: %v", err)
	}
	if err = r.DB.Put(r.WriteOptions, clusterInfoKey, value); err != nil {
		return fmt.Errorf("rocksdb: set value: %v", err)
	}
	return nil
}

func (r *rocksDB) GetClusterInfo() (*comm.ClusterInfo, error) {
	value, err := r.DB.Get(r.ReadOptions, clusterInfoKey)
	if err != nil {
		return nil, fmt.Errorf("rocksdb: get value: %v", err)
	}
	defer value.Free()
	if value.Size() == 0 {
		return nil, ErrNotFound
	}
	v := &comm.ClusterInfo{}
	if err := proto.Unmarshal(value.Data(), v); err != nil {
		return nil, fmt.Errorf("rocksdb: unmarshal cluster info: %v", err)
	}
	return v, nil
}
func (r *rocksDB) SetLeaderInfo(v *comm.LeaderInfo) error {
	value, err := proto.Marshal(v)
	if err != nil {
		return fmt.Errorf("rocksdb: marshal leader info: %v", err)
	}
	if err = r.DB.Put(r.WriteOptions, leaderInfoKey, value); err != nil {
		return fmt.Errorf("rocksdb: set value: %v", err)
	}
	return nil
}
func (r *rocksDB) GetLeaderInfo() (*comm.LeaderInfo, error) {
	value, err := r.DB.Get(r.ReadOptions, leaderInfoKey)
	if err != nil {
		return nil, fmt.Errorf("rocksdb: get value: %v", err)
	}
	defer value.Free()
	if value.Size() == 0 {
		return nil, ErrNotFound
	}
	v := &comm.LeaderInfo{}
	if err := proto.Unmarshal(value.Data(), v); err != nil {
		return nil, fmt.Errorf("rocksdb: unmarshal leader info: %v", err)
	}
	return v, nil
}

func (r *rocksDB) close() {
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
	r.closeDB()
}

func (r *rocksDB) GetMaxInstanceIDFileID() (instanceID uint64, value []byte, err error) {
	instanceID, err = r.GetMaxInstanceID()
	if err == ErrNotFound {
		err = nil
		return
	}
	if err != nil {
		return
	}

	var key []byte
	key, err = util.ObjectToBytes(instanceID)
	if err != nil {
		err = fmt.Errorf("rocksdb: convert %d to bytes: %v", instanceID, err)
		return
	}
	v, err := r.DB.Get(r.ReadOptions, key)
	if err != nil {
		err = fmt.Errorf("rocksdb: get value: %v", err)
		return
	}
	defer v.Free()
	if v.Size() == 0 {
		err = ErrNotFound
		return
	}
	value = v.Data()

	return
}

func (r *rocksDB) RebuildOneIndex(instanceID uint64, value []byte) error {
	key, err := util.ObjectToBytes(instanceID)
	if err != nil {
		return fmt.Errorf("rocksdb: convert %d to bytes: %v", instanceID, err)
	}
	if err = r.DB.Put(r.WriteOptions, key, value); err != nil {
		return fmt.Errorf("rocksdb: set value: %v", err)
	}
	return nil
}

func (r *rocksDB) closeDB() {
	if r.DB != nil {
		r.DB.Close()
		r.DB = nil
	}
	if r.logStore != nil {
		r.logStore.close()
		r.logStore = nil
	}
}