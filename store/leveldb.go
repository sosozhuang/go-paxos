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
	"github.com/sosozhuang/paxos/logger"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"errors"
	"github.com/syndtr/goleveldb/leveldb/comparer"
	"strconv"
	"path"
	"context"
	"github.com/sosozhuang/paxos/comm"
	"github.com/gogo/protobuf/proto"
	"os"
	"math"
	"sync"
	"math/rand"
)

var log = logger.PaxosLogger

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
		return nil, errors.New("invalid group count")
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
	ch := make(chan error)
	var wg sync.WaitGroup
	wg.Add(len(ls))
	for _, db := range ls {
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

func (ls ldbs) Close() {
	for _, db := range ls {
		if db != nil {
			db.Close()
		}
	}
}

func (ls ldbs) GetStorage(id uint16) Storage {
	return ls[id]
}

func (ls ldbs) Get(id uint16, instanceID uint64) ([]byte, error) {
	return ls[id].Get(instanceID)
}
func (ls ldbs) Set(id uint16, instanceID uint64, value []byte) error {
	return ls[id].Set(instanceID, value)
}
func (ls ldbs) Delete(id uint16, instanceID uint64) error {
	return ls[id].Delete(instanceID)
}
func (ls ldbs) GetMaxInstanceID(id uint16) (uint64, error) {
	return ls[id].GetMaxInstanceID()
}
func (ls ldbs) SetMinChosenInstanceID(id uint16, instanceID uint64) error {
	return ls[id].SetMinChosenInstanceID(instanceID)
}
func (ls ldbs) GetMinChosenInstanceID(id uint16) (uint64, error) {
	return ls[id].GetMinChosenInstanceID()
}
func (ls ldbs) ClearAllLog(id uint16) error {
	return ls[id].Recreate()
}
func (ls ldbs) SetSystemVar(id uint16, v *comm.SystemVar) error {
	return ls[id].SetSystemVar(v)
}
func (ls ldbs) GetSystemVar(id uint16) (*comm.SystemVar, error) {
	return ls[id].GetSystemVar()
}
func (ls ldbs) SetMasterVar(id uint16, v *comm.MasterVar) error {
	return ls[id].SetMasterVar(v)
}
func (ls ldbs) GetMasterVar(id uint16) (*comm.MasterVar, error) {
	return ls[id].GetMasterVar()
}

func newLevelDB(cfg *StorageConfig, dir string, cpr comparer.Comparer) (ldb *levelDB, err error) {
	ldb = new(levelDB)
	defer func() {
		if err != nil && ldb != nil {
			ldb.Close()
			ldb = nil
		}
	}()

	ldb.dir = dir
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
	ldb.WriteOptions.Sync = !cfg.DisableSync
	ldb.DisableWAL = cfg.DisableWAL

	ldb.logStore, err = newLogStore(dir, ldb)
	return
}

func (l *levelDB) Open(ctx context.Context, stopped <-chan struct{}) error {
	ch := make(chan error)
	go func() {
		if err := l.logStore.Open(ctx, stopped); err != nil {
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

func (l *levelDB) GetDir() string{
	return l.dir
}

func (l *levelDB) Get(instanceID uint64) ([]byte, error) {
	key, err := comm.ObjectToBytes(instanceID)
	if err != nil {
		return nil, err
	}
	value, err := l.DB.Get(key, l.ReadOptions)
	if err == leveldb.ErrNotFound {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	id, value, err := l.logStore.read(value)
	if err != nil {
		return nil, err
	}
	if id != instanceID {
		return nil, errors.New("instance id not equal")
	}
	return value, nil
}

func (l *levelDB) Set(instanceID uint64, value []byte) error {
	v, err := l.logStore.append(instanceID, value)
	if err != nil {
		return err
	}
	key, err := comm.ObjectToBytes(instanceID)
	if err != nil {
		return err
	}
	return l.DB.Put(key, v, l.WriteOptions)
}

func (l *levelDB) Delete(instanceID uint64) error {
	key, err := comm.ObjectToBytes(instanceID)
	if err != nil {
		return err
	}
	value, err := l.DB.Get(key, l.ReadOptions)
	if err == leveldb.ErrNotFound {
		return nil
	}
	if err != nil {
		return err
	}

	if rand.Uint32() % 100 < 1 {
		if err = l.logStore.delete(value); err != nil {
			return err
		}
	}

	return l.DB.Delete(key, l.WriteOptions)
}

func (l *levelDB) ForceDelete(instanceID uint64) error {
	key, err := comm.ObjectToBytes(instanceID)
	if err != nil {
		return err
	}
	value, err := l.DB.Get(key, l.ReadOptions)
	if err == leveldb.ErrNotFound {
		return nil
	}
	if err != nil {
		return err
	}

	if err = l.logStore.delete(value); err != nil {
		return err
	}

	return l.DB.Delete(key, l.WriteOptions)
}

func (l *levelDB) GetMaxInstanceID() (instanceID uint64, err error) {
	it := l.NewIterator(nil, l.ReadOptions)
	defer it.Release()
	for it.Last(); it.Valid(); it.Prev() {
		if err = comm.BytesToObject(it.Key(), &instanceID); err != nil {
			return
		}
		if instanceID != math.MaxUint64 &&
			instanceID != math.MaxUint64 -1 &&
			instanceID != math.MaxUint64 -2 {
			return
		}
	}
	err = ErrNotFound
	return
}
func (l *levelDB) SetMinChosenInstanceID(instanceID uint64) error {
	value, err := comm.ObjectToBytes(instanceID)
	if err != nil {
		return err
	}
	return l.DB.Put(minChosenKey, value, l.WriteOptions)
}

func (l *levelDB) GetMinChosenInstanceID() (uint64, error) {
	var instanceID uint64
	value, err := l.DB.Get(minChosenKey, l.ReadOptions)
	if err == leveldb.ErrNotFound {
		return instanceID, nil
	}
	if err != nil {
		return instanceID, err
	}
	if err = comm.BytesToObject(value, &instanceID); err != nil {
		return instanceID, err
	}
	return instanceID, nil
}

func (l *levelDB) Recreate() error {
	sv, err := l.GetSystemVar()
	if err != nil && err != ErrNotFound {
		return err
	}
	mv, err := l.GetMasterVar()
	if err != nil && err != ErrNotFound {
		return err
	}
	backup := path.Clean(l.dir) + ".bak"
	if err = os.RemoveAll(backup); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err = os.Rename(l.dir, backup); err != nil {
		return err
	}

	l.Close()
	l.DB, err = leveldb.OpenFile(l.dir, l.Options)
	if err != nil {
		return err
	}
	l.logStore, err = newLogStore(l.dir, l)
	if err != nil {
		return err
	}

	if sv != nil {
		if err = l.SetSystemVar(sv); err != nil {
			return err
		}
	}
	if mv != nil {
		if err = l.SetMasterVar(mv); err != nil {
			return err
		}
	}
	return nil
}

func (l *levelDB) SetSystemVar(v *comm.SystemVar) error {
	value, err := proto.Marshal(v)
	if err != nil {
		return err
	}
	return l.DB.Put(systemVarKey, value, l.WriteOptions)
}

func (l *levelDB) GetSystemVar() (*comm.SystemVar, error) {
	value, err := l.DB.Get(systemVarKey, l.ReadOptions)
	if err == leveldb.ErrNotFound {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	v := &comm.SystemVar{}
	if err := proto.Unmarshal(value, v); err != nil {
		return nil, err
	}
	return v, nil
}

func (l *levelDB) SetMasterVar(v *comm.MasterVar) error {
	value, err := proto.Marshal(v)
	if err != nil {
		return err
	}
	return l.DB.Put(masterVarKey, value, l.WriteOptions)
}

func (l *levelDB) GetMasterVar() (*comm.MasterVar, error) {
	value, err := l.DB.Get(masterVarKey, l.ReadOptions)
	if err == leveldb.ErrNotFound {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	v := &comm.MasterVar{}
	if err := proto.Unmarshal(value, v); err != nil {
		return nil, err
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
	key, err = comm.ObjectToBytes(instanceID)
	if err != nil {
		return
	}
	value, err = l.DB.Get(key, l.ReadOptions)
	if err == leveldb.ErrNotFound {
		err = ErrNotFound
		return
	}

	return
}

func (l *levelDB) RebuildOneIndex(instanceID uint64, value []byte) error {
	key, err := comm.ObjectToBytes(instanceID)
	if err != nil {
		return err
	}
	return l.DB.Put(key, value, l.WriteOptions)
}

func (l *levelDB) Close() {
	if l.DB != nil {
		l.DB.Close()
		l.DB = nil
	}
	if l.logStore != nil {
		l.logStore.Close()
		l.logStore = nil
	}
}
