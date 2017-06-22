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
package paxos

import (
	"errors"
	"github.com/sosozhuang/go-paxos/checkpoint"
	"io"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"github.com/sosozhuang/go-paxos/util"
)

type Sender interface {
	start(uint64)
	stop()
	isFinished() bool
	ack(uint64, uint64, uint64)
}

type checkpointSender struct {
	learner     Learner
	instance    Instance
	cpm         checkpoint.CheckpointManager
	nodeID      uint64
	uuid        uint64
	sequence    uint64
	ackSequence uint64
	lastAckTime time.Time
	files       map[string]struct{}
	ch          chan struct{}
	wg          sync.WaitGroup
}

func newCheckpointSender(learner Learner, instance Instance, cpm checkpoint.CheckpointManager) Sender {
	return &checkpointSender{
		instance: instance,
		learner:  learner,
		cpm:      cpm,
	}
}

func (s *checkpointSender) start(nodeID uint64) {
	s.reset(nodeID)
	s.wg.Add(1)
	go s.doSend()
}

func (s *checkpointSender) stop() {
	if s.ch != nil {
		close(s.ch)
		s.wg.Wait()
	}
}

func (s *checkpointSender) reset(nodeID uint64) {
	s.ch = make(chan struct{})
	s.nodeID = nodeID
	s.uuid = 0
	s.sequence = 0
	s.ackSequence = 0
	s.lastAckTime = time.Time{}
	s.files = make(map[string]struct{})
}

func (s *checkpointSender) isFinished() bool {
	return s.ch == nil
}

func (s *checkpointSender) doSend() {
	defer func() {
		s.ch = nil
		s.wg.Done()
	}()
	s.lastAckTime = time.Now()
	for !s.cpm.GetReplayer().IsPaused() {
		s.cpm.GetReplayer().Pause()
		select {
		case <-s.ch:
			return
		case <-time.After(time.Millisecond * 100):
		}
	}

	if err := s.lock(); err == nil {
		s.send()
		s.unlock()
	}
	s.cpm.GetReplayer().Continue()

}

func (s *checkpointSender) lock() error {
	return s.instance.lockCheckpointState()
}

func (s *checkpointSender) unlock() {
	s.instance.unlockCheckpointState()
}

func (s *checkpointSender) send() {
	if err := s.learner.sendCheckpointBegin(s.nodeID, s.uuid, s.sequence, 0); err != nil {
		log.Error(err)
		return
	}
	atomic.AddUint64(&s.sequence, 1)
	smids, iids, dirs, files := s.instance.getAllCheckpoint()
	for i, smid := range smids {
		if err := s.sendCheckpoint(smid, iids[i], dirs[i], files[i]); err != nil {
			log.Error(err)
			return
		}
	}

	if err := s.learner.sendCheckpointEnd(s.nodeID, s.uuid, s.sequence, 0); err != nil {
		log.Error(err)
	}
}

func (s *checkpointSender) sendCheckpoint(smid uint32, instanceID uint64, dir string, files []string) error {
	if dir == "" || len(files) <= 0 {
		return nil
	}
	for _, filename := range files {
		name := path.Join(dir, filename)
		if _, ok := s.files[name]; ok {
			continue
		}
		if err := s.sendFile(smid, instanceID, name); err != nil {
			return err
		}
		s.files[name] = struct{}{}
	}
	return nil
}

func (s *checkpointSender) sendFile(smid uint32, instanceID uint64, name string) error {
	f, err := os.OpenFile(name, syscall.O_RDWR, syscall.S_IREAD)
	if err != nil {
		return err
	}
	defer f.Close()
	offset := int64(0)
	for {
		b := make([]byte, 1024*1024)
		n, err := f.Read(b)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if err = s.sendBytes(smid, instanceID, name, offset, b[:n]); err != nil {
			return err
		}
		if n < 1024*1024 {
			break
		}
		offset += int64(n)
	}

	return nil
}

func (s *checkpointSender) sendBytes(smid uint32, checkpointInstanceID uint64, path string,
	offset int64, b []byte) error {
	checksum := util.Checksum(b)
	for {
		if !s.checkAck(s.sequence) {
			return errors.New("ack timeout")
		}
		if err := s.learner.sendCheckpoint(s.nodeID, s.uuid, s.sequence, checkpointInstanceID,
			checksum, path, smid, offset, b); err != nil {
			time.Sleep(time.Second * 30)
		} else {
			atomic.AddUint64(&s.sequence, 1)
			break
		}
	}
	return nil
}

func (s *checkpointSender) ack(nodeID, uuid, seq uint64) {
	if s.nodeID != nodeID ||
		s.uuid != uuid ||
		s.ackSequence != seq {
		return
	}
	atomic.AddUint64(&s.ackSequence, 1)
	s.lastAckTime = time.Now()
}

func (s *checkpointSender) checkAck(seq uint64) bool {
	for s.ackSequence+10 < seq {
		select {
		case <-s.ch:
			return false
		case <-time.After(time.Millisecond * 20):
			if time.Now().Sub(s.lastAckTime) >= time.Minute*2 {
				return false
			}
		}
	}
	return true
}
