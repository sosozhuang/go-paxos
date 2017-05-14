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

func newGroupLevelDB(cfg *StorageConfig) (ldbs ldbs, err error) {
	ldbs = make([]levelDB, cfg.GroupCount)
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

func (ls *ldbs) Close() {
	for _, db := range []*levelDB(ls) {
		if db != nil {
			db.Close()
		}
	}
}

func (ls *ldbs) GetStorage(groupID comm.GroupID) (Storage, error) {
	if groupID < 0 || groupID >= len(ls) {
		return nil, errors.New("")
	}
	return ls[groupID]
}

func (ls *ldbs) Get(groupID comm.GroupID, instanceID comm.InstanceID) ([]byte, error) {
	return ls[groupID].Get(instanceID)
}
func (ls *ldbs) Set(groupID comm.GroupID, instanceID comm.InstanceID, value []byte, opts *SetOptions) error {
	return ls[groupID].Set(instanceID, value, opts)
}
func (ls *ldbs) Delete(groupID comm.GroupID, instanceID comm.InstanceID, opts *DeleteOptions) error {
	return ls[groupID].Delete(instanceID, opts)
}
func (ls *ldbs) GetMaxInstanceID(groupID comm.GroupID) (comm.InstanceID, error) {
	return ls[groupID].GetMaxInstanceID()
}
func (ls *ldbs) SetMinChosenInstanceID(groupID comm.GroupID, instanceID comm.InstanceID, opts *SetOptions) error {
	return ls[groupID].SetMinChosenInstanceID(instanceID, opts)
}
func (ls *ldbs) GetMinChosenInstanceID(groupID comm.GroupID) (uint64, error) {
	return ls[groupID].GetMinChosenInstanceID()
}
func (ls *ldbs) ClearAllLog(groupID comm.GroupID) error {
	return ls[groupID].Recreate()
}
func (ls *ldbs) SetSystemVariables(g comm.GroupID, v *comm.SystemVariables) error {
	return ls[g].SetSystemVariables(v)
}
func (ls *ldbs) GetSystemVariables(g comm.GroupID) (*comm.SystemVariables, error) {
	return ls[g].GetSystemVariables()
}
func (ls *ldbs) SetMasterVariables(g comm.GroupID, v *comm.MasterVariables) error {
	return ls[g].SetMasterVariables(v)
}
func (ls *ldbs) GetMasterVariables(g comm.GroupID) (*comm.MasterVariables, error) {
	return ls[g].GetMasterVariables()
}

func newLevelDB(cfg *StorageConfig, dir string, comparer comparer.Comparer) (ldb *levelDB, err error) {
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
		Comparer: comparer,
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

func (l *levelDB) Get(instanceID comm.InstanceID) ([]byte, error) {
	return nil, nil
}

func (l *levelDB) Set(instanceID comm.InstanceID, value []byte, opts *SetOptions) error {
	return nil
}

func (l *levelDB) Delete(instanceID comm.InstanceID, opts *DeleteOptions) error {
	return nil
}
func (l *levelDB) GetMaxInstanceID() (instanceID comm.InstanceID, err error) {
	it := l.NewIterator(nil, l.ReadOptions)
	defer it.Release()
	var key uint64
	for it.Last(); it.Valid(); it.Prev() {
		if err = comm.BytesToObject(it.Key(), &key); err != nil {
			return
		}
		if key != minChosenKey &&
			key != systemVariablesKey &&
			key != masterVariablesKey {
			return
		}
	}
	return
}
func (l *levelDB) SetMinChosenInstanceID(instanceID comm.InstanceID, opts *SetOptions) error {
	value, err := comm.ObjectToBytes(instanceID)
	if err != nil {
		return err
	}
	return l.DB.Put(minChosenKey, value, l.WriteOptions)
}
func (l *levelDB) GetMinChosenInstanceID() (comm.InstanceID, error) {
	var instanceID comm.InstanceID
	value, err := l.DB.Get(minChosenKey, l.ReadOptions)
	if err == leveldb.ErrNotFound {
		return instanceID, nil
	}
	if err != nil {
		return instanceID, err
	}
	if len(value) == 12 {
		value, err = l.Get(minChosenKey)
		if err == ErrNotFound {
			return instanceID, nil
		}
		if err != nil {
			return instanceID, err
		}
	}
	if err = comm.BytesToObject(value, &instanceID); err != nil {
		return instanceID, err
	}
	return instanceID, nil
}
func (l *levelDB) Recreate() error {
	sv, err := l.GetSystemVariables()
	if err != nil && err != ErrNotFound {
		return err
	}
	mv, err := l.GetMasterVariables()
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
		if err = l.SetSystemVariables(sv); err != nil {
			return err
		}
	}
	if mv != nil {
		if err = l.SetMasterVariables(mv); err != nil {
			return err
		}
	}
	return nil
}
func (l *levelDB) SetSystemVariables(v *comm.SystemVariables) error {
	value, err := proto.Marshal(v)
	if err != nil {
		return err
	}
	return l.DB.Put(systemVariablesKey, value, l.WriteOptions)
}
func (l *levelDB) GetSystemVariables() (*comm.SystemVariables, error) {
	value, err := l.DB.Get(systemVariablesKey, l.ReadOptions)
	if err == leveldb.ErrNotFound {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	v := &comm.SystemVariables{}
	if err := proto.Unmarshal(value, v); err != nil {
		return nil, err
	}
	return v, nil
}
func (l *levelDB) SetMasterVariables(v *comm.MasterVariables) error {
	value, err := proto.Marshal(v)
	if err != nil {
		return err
	}
	return l.DB.Put(masterVariablesKey, value, l.WriteOptions)
}
func (l *levelDB) GetMasterVariables() (*comm.MasterVariables, error) {
	value, err := l.DB.Get(masterVariablesKey, l.ReadOptions)
	if err == leveldb.ErrNotFound {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	v := &comm.MasterVariables{}
	if err := proto.Unmarshal(value, v); err != nil {
		return nil, err
	}
	return v, nil
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
