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
	"github.com/sosozhuang/paxos/logger"
	"github.com/sosozhuang/paxos/comm"
	"github.com/gogo/protobuf/proto"
	"fmt"
	"context"
	"errors"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

var (
	kvlog = logger.GetLogger("kvstore")
)

type KeyValueStore interface {
	Get(string) (string, error)
	Set(string, string) error
	Delete(string) error
	Close()
	comm.StateMachine
}

type keyValueStore struct {
	proposer comm.Proposer
	dir string
	*leveldb.DB
	*opt.Options
	*opt.ReadOptions
	*opt.WriteOptions
}

func NewKeyValueStore(proposer comm.Proposer, dir string) (KeyValueStore, error) {
	var err error
	kvs := &keyValueStore{
		proposer: proposer,
		dir: dir,
	}
	defer func() {
		if err != nil {
			kvs.Close()
			kvs = nil
		}
	}()

	kvs.Options = &opt.Options{
		ErrorIfMissing: false,
	}
	kvs.DB, err = leveldb.OpenFile(dir, kvs.Options)
	if err != nil {
		return kvs, err
	}

	kvs.ReadOptions = &opt.ReadOptions{}
	kvs.WriteOptions = &opt.WriteOptions{}
	return kvs, nil
}

func (s *keyValueStore) Close() {
	if s.DB != nil {
		s.DB.Close()
		s.DB = nil
	}
}

func (s *keyValueStore) Get(key string) (string, error) {
	if len(key) <= 0 {
		return "", errors.New("empty key")
	}
	value, err := s.DB.Get([]byte(key), s.ReadOptions)
	if err == leveldb.ErrNotFound {
		err = ErrNotFound
	}
	return string(value), err
}

func (s *keyValueStore) Set(key, value string) error {
	if len(key) <= 0 {
		return errors.New("empty key")
	}
	kv := &comm.KeyValue{
		Action: comm.Action_Set.Enum(),
		Key: []byte(key),
		Value: []byte(value),
	}
	v, err := proto.Marshal(kv)
	if err != nil {
		return err
	}
	return s.proposer.Propose(0, comm.KVStoreStateMachineID, v)
}

func (s *keyValueStore) Delete(key string) error {
	kv := &comm.KeyValue{
		Action: comm.Action_Delete.Enum(),
		Key: []byte(key),
	}
	v, err := proto.Marshal(kv)
	if err != nil {
		return err
	}
	return s.proposer.Propose(0, comm.KVStoreStateMachineID, v)
}

func (s *keyValueStore) GetStateMachineID() uint32 {
	return comm.KVStoreStateMachineID
}
func (s *keyValueStore) Execute(ctx context.Context, instanceID uint64, b []byte) (interface{}, error) {
	kv := &comm.KeyValue{}
	if err := proto.Unmarshal(b, kv); err != nil {
		return nil, fmt.Errorf("kvstore: unmarshal data: %v", err)
	}

	switch kv.GetAction() {
	case comm.Action_Set:
		kvlog.Debugf("Key value store set key: %s, value: %s.", kv.GetKey(), kv.GetValue())
		return nil, s.DB.Put(kv.GetKey(), kv.GetValue(), s.WriteOptions)
	case comm.Action_Delete:
		kvlog.Debugf("Key value store delete key: %s.", kv.GetKey())
		return nil, s.DB.Delete(kv.GetKey(), s.WriteOptions)
	}
	return nil, errors.New("unknown action")
}

func (*keyValueStore) ExecuteForCheckpoint(uint64, []byte) error {
	return nil
}
func (*keyValueStore) GetCheckpointInstanceID() uint64 {
	return comm.UnknownInstanceID
}
func (*keyValueStore) GetCheckpoint() ([]byte, error) {
	return nil, nil
}
func (*keyValueStore) UpdateByCheckpoint(b []byte) error {
	return nil
}
func (*keyValueStore) LockCheckpointState() error {
	return nil
}
func (*keyValueStore) UnlockCheckpointState() {
}
func (*keyValueStore) GetCheckpointState() (string, []string) {
	return "", nil
}