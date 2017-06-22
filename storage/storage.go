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
	"context"
	"errors"
	"fmt"
	"github.com/sosozhuang/go-paxos/comm"
	"github.com/sosozhuang/go-paxos/logger"
	"github.com/sosozhuang/go-paxos/util"
	"math"
	"os"
	"strings"
	"time"
)

type StorageConfig struct {
	GroupCount int
	DataDir    string
	Type       StorageType
	Sync       bool
	SyncPeriod time.Duration
	SyncCount  int
	DisableWAL bool
}

type StorageType uint

const (
	RocksDB StorageType = iota
	LevelDB
)

var (
	log                     = logger.GetLogger("storage")
	ErrNotFound             = errors.New("storage: value not found")
	errInstanceIDNotMatched = errors.New("storage: instance id not matched")
	leaderInfoKey           []byte
	clusterInfoKey          []byte
	minChosenKey            []byte
)

func init() {
	leaderInfoKey, _ = util.ObjectToBytes(uint64(math.MaxUint64))
	clusterInfoKey, _ = util.ObjectToBytes(uint64(math.MaxUint64) - 1)
	minChosenKey, _ = util.ObjectToBytes(uint64(math.MaxUint64) - 2)
}

func (cfg *StorageConfig) validate() error {
	if cfg.Type != RocksDB && cfg.Type != LevelDB {
		return errors.New("storage: unsupport storage type")
	}
	if cfg.DataDir == "" {
		return errors.New("storage: empty data dir")
	}

	if f, err := os.Stat(cfg.DataDir); err != nil && !os.IsNotExist(err) {
		return err
	} else if err == nil && !f.IsDir() {
		return fmt.Errorf("storage: data dir %s not dir", cfg.DataDir)
	} else if os.IsNotExist(err) {
		if err = os.MkdirAll(cfg.DataDir, os.FileMode(0775)); err != nil {
			return fmt.Errorf("storage: make directory %s error: %v", cfg.DataDir, err)
		}
		log.Infof("Storage data directory %s created.", cfg.DataDir)
	}

	if !cfg.Sync && cfg.DisableWAL {
		log.Warning("Storage disable sync and wal.")
	}
	if cfg.Sync {
		if cfg.SyncPeriod > time.Minute {
			cfg.SyncPeriod = time.Second * 10
			log.Warningf("Storage sync duration too long, fix to %v.", cfg.SyncPeriod)
		}
		if cfg.SyncCount > 100 {
			cfg.SyncCount = 100
			log.Warningf("Storage sync interval too large, fix to %d.", cfg.SyncCount)
		}
	}
	return nil
}

func ParseStorageType(s string) (StorageType, error) {
	switch strings.ToLower(s) {
	case "leveldb", "":
		return LevelDB, nil
	case "rocksdb":
		return RocksDB, nil
	}

	return StorageType(0), fmt.Errorf("storage: unsupport type: %s", s)
}

type MultiGroupStorage interface {
	Open(context.Context, <-chan struct{}) error
	Close()
	GetStorage(uint16) Storage
}

type Storage interface {
	open(<-chan struct{}) error
	close()
	GetDir() string
	Recreate() error
	Get(uint64) ([]byte, error)
	Set(uint64, []byte) error
	Delete(uint64) error
	ForceDelete(uint64) error
	GetMaxInstanceID() (uint64, error)
	SetMinChosenInstanceID(uint64) error
	GetMinChosenInstanceID() (uint64, error)
	SetClusterInfo(*comm.ClusterInfo) error
	GetClusterInfo() (*comm.ClusterInfo, error)
	SetLeaderInfo(*comm.LeaderInfo) error
	GetLeaderInfo() (*comm.LeaderInfo, error)
	GetMaxInstanceIDFileID() (uint64, []byte, error)
	RebuildOneIndex(uint64, []byte) error
}

func NewDiskStorage(cfg StorageConfig) (MultiGroupStorage, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	switch cfg.Type {
	case RocksDB:
		return newRocksDBGroup(&cfg)
	case LevelDB:
		return newLevelDBGroup(&cfg)
	}
	return nil, nil
}
