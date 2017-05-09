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
)

type Instance interface {
	GetCurrentInstanceID() comm.InstanceID
	HandleMessage([]byte, int) error
	Committer
	Cleaner() checkpoint.Cleaner
	Replayer() checkpoint.Replayer
}

func NewInstance() (Instance, error) {
	return &instance{}, nil
}

type instance struct {
	proposer Proposer
	acceptor Acceptor
	learner Learner
}