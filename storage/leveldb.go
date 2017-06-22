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
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"errors"
	"github.com/syndtr/goleveldb/leveldb/comparer"
	"strconv"
	"path"
	"context"
	"github.com/sosozhuang/go-paxos/comm"
	"github.com/gogo/protobuf/proto"
	"os"
	"math"
	"sync"
	"math/rand"
	"fmt"
	"github.com/sosozhuang/go-paxos/util"
)

type ldbs []*levelDB

type levelDB struct {
	dir string
	*StorageConfig
	*leveldb.DB
	*opt.Options
	*opt.ReadOptions
	*opt.WriteOptions
	*logStore
}

func (*comparator) Separator(dst, a, b []byte) []byte {
	return nil
}

func (*comparator) Successor(dst, b []byte) []byte {
	return nil
}

func newLevelDBGroup(cfg *StorageConfig) (ldbs ldbs, err error) {
	ldbs = make([]*levelDB, cfg.GroupCount)
	if len(ldbs) == 0 || ldbs == nil {
		return nil, errors.New("leveldb: invalid group count")
	}
	defer func() {
		if err != nil {
			ldbs.Close()
			ldbs = nil
		}
	}()

	c := &comparator{}
	for i := range ldbs {
		dir := path.Join(cfg.DataDir, strconv.Itoa(i))
		ldbs[i], err = newLevelDB(cfg, dir, c)
		if err != nil {
			return
		}
	}
	return
}

func (ls ldbs) Open(ctx context.Context, stopped <-chan struct{}) error {
	ch := make(chan error, len(ls))
	var wg sync.WaitGroup
	wg.Add(len(ls))
	for _, db := range ls {
		go func(db *levelDB) {
			defer wg.Done()
			if err := db.open(stopped); err != nil {
				ch <- err
			}
		}(db)
	}
	go func() {
		wg.Wait()
		close(ch)
		log.Debug("Leveldb group storage opened.")
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

func (ls ldbs) Close() {
	for _, db := range ls {
		if db != nil {
			db.close()
		}
	}
	log.Debug("Leveldb group storage closed.")
}

func (ls ldbs) GetStorage(id uint16) Storage {
	return ls[id]
}

func newLevelDB(cfg *StorageConfig, dir string, cpr comparer.Comparer) (ldb *levelDB, err error) {
	ldb = new(levelDB)
	defer func() {
		if err != nil && ldb != nil {
			ldb.close()
			ldb = nil
		}
	}()

	ldb.dir = dir
	ldb.StorageConfig = cfg
	ldb.Options = &opt.Options{
		ErrorIfMissing: false,
		//todo:  create option for every group?
		//WriteBuffer: 1024 * 1024 + group * 10 * 1024,
		Comparer: cpr,
	}
	ldb.DB, err = leveldb.OpenFile(dir, ldb.Options)
	if err != nil {
		return
	}

	ldb.ReadOptions = &opt.ReadOptions{}
	ldb.WriteOptions = &opt.WriteOptions{}
	ldb.WriteOptions.Sync = cfg.Sync
	ldb.DisableWAL = cfg.DisableWAL

	ldb.logStore, err = newLogStore(dir, ldb)
	return
}

func (l *levelDB) open(stopped <-chan struct{}) error {
	return l.logStore.open(stopped)
}

func (l *levelDB) GetDir() string{
	return l.dir
}

func (l *levelDB) Get(instanceID uint64) ([]byte, error) {
	key, err := util.ObjectToBytes(instanceID)
	if err != nil {
		return nil, fmt.Errorf("leveldb: convert %d to bytes: %v", instanceID, err)
	}
	value, err := l.DB.Get(key, l.ReadOptions)
	if err == leveldb.ErrNotFound {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("leveldb: get value: %v", err)
	}
	id, value, err := l.logStore.read(value)
	if err != nil {
		return nil, err
	}
	if id != instanceID {
		return nil, errInstanceIDNotMatched
	}
	return value, nil
}

func (l *levelDB) Set(instanceID uint64, value []byte) error {
	v, err := l.logStore.append(instanceID, value)
	if err != nil {
		return err
	}
	key, err := util.ObjectToBytes(instanceID)
	if err != nil {
		return fmt.Errorf("leveldb: convert %d to bytes: %v", instanceID, err)
	}
	if err = l.DB.Put(key, v, l.WriteOptions); err != nil {
		return fmt.Errorf("leveldb: set value: %v", err)
	}
	return nil
}

func (l *levelDB) Delete(instanceID uint64) error {
	key, err := util.ObjectToBytes(instanceID)
	if err != nil {
		return fmt.Errorf("leveldb: convert %d to bytes: %v", instanceID, err)
	}
	value, err := l.DB.Get(key, l.ReadOptions)
	if err == leveldb.ErrNotFound {
		return nil
	}
	if err != nil {
		return fmt.Errorf("leveldb: get value: %v", err)
	}

	if rand.Uint32() % 100 < 1 {
		if err = l.logStore.delete(value); err != nil {
			return err
		}
	}

	if err = l.DB.Delete(key, l.WriteOptions); err != nil {
		return fmt.Errorf("leveldb: delete value: %v", err)
	}
	return nil
}

func (l *levelDB) ForceDelete(instanceID uint64) error {
	key, err := util.ObjectToBytes(instanceID)
	if err != nil {
		return err
	}
	value, err := l.DB.Get(key, l.ReadOptions)
	if err == leveldb.ErrNotFound {
		return nil
	}
	if err != nil {
		return fmt.Errorf("leveldb: get value: %v", err)
	}

	if err = l.logStore.truncate(value); err != nil {
		return err
	}

	if err = l.DB.Delete(key, l.WriteOptions); err != nil {
		return fmt.Errorf("leveldb: delete value: %v", err)
	}
	return nil
}

func (l *levelDB) GetMaxInstanceID() (instanceID uint64, err error) {
	it := l.NewIterator(nil, l.ReadOptions)
	defer it.Release()
	for it.Last(); it.Valid(); it.Prev() {
		if err = util.BytesToObject(it.Key(), &instanceID); err != nil {
			err = fmt.Errorf("leveldb: convert bytes to instance id: %v", err)
			return
		}
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
func (l *levelDB) SetMinChosenInstanceID(instanceID uint64) error {
	value, err := util.ObjectToBytes(instanceID)
	if err != nil {
		return fmt.Errorf("leveldb: convert %d to bytes: %v", instanceID, err)
	}
	if err = l.DB.Put(minChosenKey, value, l.WriteOptions); err != nil {
		return fmt.Errorf("leveldb: set value: %v", err)
	}
	return nil
}

func (l *levelDB) GetMinChosenInstanceID() (uint64, error) {
	var instanceID uint64
	value, err := l.DB.Get(minChosenKey, l.ReadOptions)
	if err == leveldb.ErrNotFound {
		return instanceID, nil
	}
	if err != nil {
		return instanceID, fmt.Errorf("leveldb: get value: %v", err)
	}
	if err = util.BytesToObject(value, &instanceID); err != nil {
		return instanceID, fmt.Errorf("level: convert bytes to instance id: %v", err)
	}
	return instanceID, nil
}

func (l *levelDB) Recreate() error {
	ci, err := l.GetClusterInfo()
	if err != nil && err != ErrNotFound {
		return err
	}
	li, err := l.GetLeaderInfo()
	if err != nil && err != ErrNotFound {
		return err
	}
	backup := path.Clean(l.dir) + ".bak"
	if err = os.RemoveAll(backup); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("leveldb: remove %s: %v", backup, err)
	}
	if err = os.Rename(l.dir, backup); err != nil {
		return fmt.Errorf("leveldb: rename %s to %s: %v", l.dir, backup, err)
	}

	l.close()
	l.DB, err = leveldb.OpenFile(l.dir, l.Options)
	if err != nil {
		return fmt.Errorf("leveldb: open db: %v", err)
	}
	l.logStore, err = newLogStore(l.dir, l)
	if err != nil {
		return err
	}

	if ci != nil {
		if err = l.SetClusterInfo(ci); err != nil {
			return err
		}
	}
	if li != nil {
		if err = l.SetLeaderInfo(li); err != nil {
			return err
		}
	}
	return nil
}

func (l *levelDB) SetClusterInfo(v *comm.ClusterInfo) error {
	value, err := proto.Marshal(v)
	if err != nil {
		return fmt.Errorf("leveldb: marshal cluster info: %v", err)
	}
	if err = l.DB.Put(clusterInfoKey, value, l.WriteOptions); err != nil {
		return fmt.Errorf("leveldb: set value: %v", err)
	}
	return nil
}

func (l *levelDB) GetClusterInfo() (*comm.ClusterInfo, error) {
	value, err := l.DB.Get(clusterInfoKey, l.ReadOptions)
	if err == leveldb.ErrNotFound {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("leveldb: get value: %v", err)
	}
	v := &comm.ClusterInfo{}
	if err := proto.Unmarshal(value, v); err != nil {
		return nil, fmt.Errorf("leveldb: unmarshal cluster info: %v", err)
	}
	return v, nil
}

func (l *levelDB) SetLeaderInfo(v *comm.LeaderInfo) error {
	value, err := proto.Marshal(v)
	if err != nil {
		return fmt.Errorf("leveldb: marshal leader info: %v", err)
	}
	if err = l.DB.Put(leaderInfoKey, value, l.WriteOptions); err != nil {
		return fmt.Errorf("leveldb: set value: %v", err)
	}
	return nil
}

func (l *levelDB) GetLeaderInfo() (*comm.LeaderInfo, error) {
	value, err := l.DB.Get(leaderInfoKey, l.ReadOptions)
	if err == leveldb.ErrNotFound {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("leveldb: get value: %v", err)
	}
	v := &comm.LeaderInfo{}
	if err := proto.Unmarshal(value, v); err != nil {
		return nil, fmt.Errorf("leveldb: unmarshal leader info: %v", err)
	}
	return v, nil
}

func (l *levelDB) GetMaxInstanceIDFileID() (instanceID uint64, value []byte, err error) {
	instanceID, err = l.GetMaxInstanceID()
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
		err = fmt.Errorf("leveldb: convert %d to bytes: %v", instanceID, err)
		return
	}
	value, err = l.DB.Get(key, l.ReadOptions)
	if err == leveldb.ErrNotFound {
		err = ErrNotFound
		return
	}
	if err != nil {
		err = fmt.Errorf("leveldb: get value: %v", err)
	}

	return
}

func (l *levelDB) RebuildOneIndex(instanceID uint64, value []byte) error {
	key, err := util.ObjectToBytes(instanceID)
	if err != nil {
		return err
	}
	return l.DB.Put(key, value, l.WriteOptions)
}

func (l *levelDB) close() {
	if l.DB != nil {
		l.DB.Close()
		l.DB = nil
	}
	if l.logStore != nil {
		l.logStore.close()
		l.logStore = nil
	}
}
