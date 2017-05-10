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
	"github.com/sosozhuang/paxos/checkpoint"
	"github.com/sosozhuang/paxos/comm"
	"time"
	"errors"
	"github.com/sosozhuang/paxos/node"
)

const (
	maxValueLength = 1024 * 1024 * 1024
)
type Instance interface {
	NewInstance()
	GetCurrentInstanceID() comm.InstanceID
	HandleMessage([]byte, int) error
	Cleaner() checkpoint.Cleaner
	Replayer() checkpoint.Replayer
}

func NewInstance() (Instance, error) {
	return &instance{
		ch: make(chan []byte, 100),
	}, nil
}

type instance struct {
	group node.Group
	proposer Proposer
	acceptor Acceptor
	learner Learner
	ch chan []byte
}

func (i *instance) NewInstance() {
	i.proposer.NewInstance()
	i.acceptor.NewInstance()
	i.learner.NewInstance()
}

func (i *instance) OnReceive(message []byte) error {
	select {
	case i.ch <- message:
		return nil
	case time.After(time.Second * 3):
		return errors.New("time out")
	}
}

func (i *instance) CheckNewValue() {
	for value := range i.ch {
		if !i.learner.isIMLast() {
			break
		}

		if !i.group.CheckMemberShip() {
			break
		}

		if len(value) > maxValueLength &&
			(i.proposer.GetInstanceID() == 0 || ) {
			break
		}

		if i.group.IsUseMembership() {

		} else {
			msg := i.proposer.NewValue(value)
			// todo: send to acceptor and remote
		}

	}
}