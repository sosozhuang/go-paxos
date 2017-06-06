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
	"github.com/gogo/protobuf/proto"
	"github.com/sosozhuang/paxos/comm"
	"github.com/sosozhuang/paxos/storage"
	"time"
)

type Replayer interface {
	start(<-chan struct{})
	stop()
	Pause()
	Continue()
	IsPaused() bool
}

type replayer struct {
	st       storage.Storage
	instance comm.CheckpointInstance
	cpm      CheckpointManager
	pause    bool
	running  bool
	done chan struct{}
}

func (r *replayer) start(stopped <-chan struct{}) {
	r.done = make(chan struct{})
	go r.doReplay(stopped)
}

func (r *replayer) stop() {
	if r.done != nil {
		<- r.done
	}
}

func (r *replayer) Pause() {
	r.pause = true
}

func (r *replayer) IsPaused() bool {
	return r.pause && !r.running
}

func (r *replayer) Continue() {
	r.pause = false
}

func (r *replayer) doReplay(stopped <-chan struct{}) {
	defer close(r.done)
	instanceID := r.instance.GetCheckpointInstanceID() + 1
	var err error
	for {
		select {
		case <-stopped:
			return
		default:
		}
		if r.pause {
			r.running = false
			time.Sleep(time.Second)
			continue
		}
		r.running = true

		if instanceID >= r.cpm.getMaxChosenInstanceID() {
			time.Sleep(time.Second)
			continue
		}
		if err = r.replay(instanceID); err != nil {
			log.Error(err)
			time.Sleep(time.Second / 2)
			continue
		}
		instanceID++
	}
}

func (r *replayer) replay(instanceID uint64) error {
	b, err := r.st.Get(instanceID)
	if err != nil {
		return err
	}
	var state comm.AcceptorState
	if err = proto.Unmarshal(b, &state); err != nil {
		return err
	}

	return r.instance.ExecuteForCheckpoint(instanceID, state.GetAcceptedValue())
}
