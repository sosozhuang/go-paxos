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
	"github.com/sosozhuang/go-paxos/comm"
	"github.com/sosozhuang/go-paxos/storage"
	"time"
)

type Cleaner interface {
	start(<-chan struct{})
	stop()
	Pause()
	Continue()
	IsPaused() bool
	SetMaxLogCount(int64)
	fixMinChosenInstanceID(uint64) error
}

type cleaner struct {
	st             storage.Storage
	instance       comm.CheckpointInstance
	cpm            CheckpointManager
	pause          bool
	running        bool
	saveInstanceID uint64
	speed          int64
	count          uint64
	done           chan struct{}
}

func (c *cleaner) fixMinChosenInstanceID(instanceID uint64) error {
	cpInstanceID := c.instance.GetCheckpointInstanceID() + 1
	fix := instanceID
	for i := instanceID; i < instanceID+100 && i < cpInstanceID; i++ {
		if _, err := c.st.Get(i); err == storage.ErrNotFound {
			fix = i + 1
		} else if err != nil {
			return err
		} else {
			break
		}

	}
	if fix > instanceID {
		if err := c.cpm.SaveMinChosenInstanceID(fix); err != nil {
			return err
		}
	}
	return nil
}

func (c *cleaner) start(stopped <-chan struct{}) {
	c.done = make(chan struct{})
	go c.doClean(stopped)
}

func (c *cleaner) stop() {
	if c.done != nil {
		<-c.done
	}
}

func (c *cleaner) Pause() {
	c.pause = true
}

func (c *cleaner) IsPaused() bool {
	return c.pause && !c.running
}

func (c *cleaner) Continue() {
	c.pause = false
}

func (c *cleaner) doClean(stopped <-chan struct{}) {
	defer close(c.done)
	var sleep, interval int64
	if c.speed > 1000 {
		sleep = 10
		interval = c.speed /1000 + 1
	} else {
		sleep = 1000 / c.speed
		interval = 1
	}

	for {
		select {
		case <-stopped:
			return
		default:
		}

		if c.pause {
			c.running = false
			time.Sleep(time.Second)
			continue
		}
		c.running = true

		instanceID := c.cpm.GetMinChosenInstanceID()
		cpInstanceID := c.instance.GetCheckpointInstanceID()
		maxChosenInstanceID := c.cpm.getMaxChosenInstanceID()
		delete := int64(0)
		for ; instanceID+c.count < cpInstanceID && instanceID+c.count < maxChosenInstanceID; instanceID++ {
			if err := c.delete(instanceID); err != nil {
				log.Errorf("Cleaner delete instance id %d error: %v.", instanceID, err)
				break
			}
			delete += 1
			if delete >= interval {
				delete = 0
				time.Sleep(time.Duration(sleep))
			}
		}

		time.Sleep(time.Second)
	}
}

func (c *cleaner) delete(instanceID uint64) error {
	if err := c.st.Delete(instanceID); err != nil {
		return err
	}
	c.cpm.setMinChosenInstanceID(instanceID)
	if instanceID >= c.saveInstanceID+100 {
		if err := c.cpm.SaveMinChosenInstanceID(instanceID + 1); err != nil {
			return err
		}
		c.saveInstanceID = instanceID

	}
	return nil
}

func (c *cleaner) SetMaxLogCount(count int64) {
	if count <= 300 {
		c.count = 300
		return
	}
	c.count = uint64(count)
}
