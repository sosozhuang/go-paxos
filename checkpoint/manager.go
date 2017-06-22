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
package checkpoint

import (
	"errors"
	"github.com/sosozhuang/go-paxos/comm"
	"github.com/sosozhuang/go-paxos/storage"
	"time"
	"github.com/sosozhuang/go-paxos/logger"
)

var log = logger.GetLogger("checkpoint")

type CheckpointManager interface {
	Start(<-chan struct{})
	Stop()
	GetReplayer() Replayer
	GetCleaner() Cleaner
	GetMinChosenInstanceID() uint64
	setMinChosenInstanceID(uint64)
	getMaxChosenInstanceID() uint64
	SetMaxChosenInstanceID(uint64)
	IsAskForCheckpoint() bool
	ExitAskForCheckpoint()
	PrepareAskForCheckpoint(uint64) error
	SaveMinChosenInstanceID(uint64) error
}

type cpm struct {
	askForCheckpoint    bool
	minChosenInstanceID uint64
	maxChosenInstanceID uint64
	lastCheckpointTime  time.Time
	groupCfg            comm.GroupConfig
	st                  storage.Storage
	cleaner             Cleaner
	replayer            Replayer
	askNodes            map[uint64]struct{}
}

func NewCheckpointManager(groupCfg comm.GroupConfig,
	instance comm.CheckpointInstance,
	st storage.Storage) (CheckpointManager, error) {
	instanceID, err := st.GetMinChosenInstanceID()
	if err != nil {
		return nil, err
	}
	cpm := &cpm{
		groupCfg:            groupCfg,
		st:                  st,
		minChosenInstanceID: instanceID,
		askNodes: make(map[uint64]struct{}),
	}
	cpm.cleaner = &cleaner{
		st: st,
		instance: instance,
		cpm: cpm,
		speed: 100000,
	}
	if err = cpm.cleaner.fixMinChosenInstanceID(instanceID); err != nil {
		return nil, err
	}
	cpm.replayer = &replayer{
		st: st,
		instance: instance,
		cpm: cpm,
	}
	return cpm, nil
}

func (c *cpm) Start(stopped <-chan struct{}) {
	if c.groupCfg.IsEnableReplayer() {
		c.replayer.start(stopped)
	}
	c.cleaner.start(stopped)
}

func (c *cpm) Stop() {
	if c.groupCfg.IsEnableReplayer() {
		c.replayer.stop()
	}
	c.cleaner.stop()
}

func (c *cpm) GetReplayer() Replayer {
	return c.replayer
}

func (c *cpm) GetCleaner() Cleaner {
	return c.cleaner
}

func (c *cpm) PrepareAskForCheckpoint(nodeID uint64) error {
	c.askNodes[nodeID] = struct{}{}
	if c.lastCheckpointTime.IsZero() {
		c.lastCheckpointTime = time.Now()
	}
	if time.Now().Sub(c.lastCheckpointTime) < time.Minute {
		if len(c.askNodes) < c.groupCfg.GetMajorityCount() {
			return errors.New("checkpoint: less than majority")
		}
	}
	c.lastCheckpointTime = time.Time{}
	c.askForCheckpoint = true
	return nil
}

func (c *cpm) IsAskForCheckpoint() bool {
	return c.askForCheckpoint
}

func (c *cpm) ExitAskForCheckpoint() {
	c.askForCheckpoint = false
}

func (c *cpm) GetMinChosenInstanceID() uint64 {
	return c.minChosenInstanceID
}

func (c *cpm) SaveMinChosenInstanceID(id uint64) error {
	if err := c.st.SetMinChosenInstanceID(id); err != nil {
		return err
	}
	c.minChosenInstanceID = id
	return nil
}

func (c *cpm) setMinChosenInstanceID(id uint64) {
	c.minChosenInstanceID = id
}

func (c *cpm) SetMaxChosenInstanceID(id uint64) {
	c.maxChosenInstanceID = id
}

func (c *cpm) getMaxChosenInstanceID() uint64 {
	return c.maxChosenInstanceID
}
