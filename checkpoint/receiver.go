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
	"fmt"
	"github.com/sosozhuang/paxos/comm"
	"github.com/sosozhuang/paxos/store"
	"os"
	"path"
	"strings"
	"syscall"
	"sync/atomic"
)

//type Receiver interface {
//	Prepare(comm.NodeID, uint64) error
//	Reset()
//	GetTmpDir(comm.StateMachineID) string
//	Receive(*comm.CheckpointMsg) error
//	IsFinished(comm.NodeID, uint64, uint64) bool
//}

type Receiver struct {
	nodeID   comm.NodeID
	uuid
	sequence
	dirs     map[string]struct{}
	store.Storage
}

func (r *Receiver) Prepare(nodeID comm.NodeID, uuid uuid) error {
	if err := r.removeTmpDir(); err != nil {
		return err
	}
	if err := r.Storage.Recreate(); err != nil {
		return err
	}

	r.nodeID = nodeID
	r.uuid = uuid
	r.sequence = 0
	r.dirs = make(map[string]struct{})
	return nil
}

func (r *Receiver) Reset() {
	r.nodeID = comm.UnknownNodeID
	r.uuid = 0
	r.sequence = 0
	r.dirs = make(map[string]struct{})
}

func (r *Receiver) removeTmpDir() error {
	d, err := os.Open(r.Storage.GetDir())
	if err != nil {
		return err
	}
	defer d.Close()
	children, err := d.Readdir(0)
	if err != nil {
		return err
	}
	for _, child := range children {
		if strings.Contains(child.Name(), "cp_tmp_") {
			if err = os.Remove(child.Name()); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *Receiver) IsFinished(nodeID comm.NodeID, uuid uuid, seq sequence) bool {
	return r.nodeID == nodeID && r.uuid == uuid && r.sequence+1 == seq
}

func (r *Receiver) GetTmpDir(id comm.StateMachineID) string {
	return path.Join(r.Storage.GetDir(), fmt.Sprintf("cp_tmp_%d", id))
}

func (r *Receiver) initFilePath(filename string) error {
	dir := path.Dir(filename)
	if _, ok := r.dirs[dir]; ok {
		return nil
	}
	if err := os.MkdirAll(dir, syscall.S_IRWXU|syscall.S_IRWXG|syscall.S_IROTH|syscall.S_IXOTH); err != nil {
		return err
	}
	r.dirs[dir] = struct{}{}
	return nil
}

func (r *Receiver) Receive(msg *comm.CheckpointMsg) error {
	if msg.GetNodeID() != r.nodeID || msg.GetUUID() != r.uuid {
		return errors.New("invalid checkpoint msg")
	}
	if msg.GetSequence() == r.sequence {
		return nil
	}
	if msg.GetSequence() != r.sequence+1 {
		return errors.New("invalid checkpoint msg sequence")
	}
	f := path.Join(r.GetTmpDir(msg.GetSMID()), msg.GetFilePath())
	if err := r.initFilePath(f); err != nil {
		return err
	}
	fileInfo, err := os.OpenFile(f, syscall.O_CREAT|syscall.O_RDWR|syscall.O_APPEND, syscall.S_IWRITE|syscall.S_IREAD)
	if err != nil {
		return err
	}
	defer fileInfo.Close()
	offset, err := fileInfo.Seek(0, os.SEEK_END)
	if err != nil {
		return err
	}
	if offset != msg.GetOffset() {
		return errors.New("invalid offset")
	}
	n, err := fileInfo.Write(msg.GetBuffer())
	if err != nil {
		return err
	}
	if n != len(msg.GetBuffer()) {
		return errors.New("write failed")
	}
	atomic.AddUint64(&r.sequence, 1)
	return nil
}
