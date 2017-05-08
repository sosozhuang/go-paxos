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
package comm

import "context"

type Receiver interface {
	ReceiveMessage(msg []byte) error
}

type Sender interface {
	SendMessage(ip string, port int, message []byte) error
}

type NetWork interface {
	Start(context.Context, <-chan struct{}) error
	Stop()
	Receiver
	Sender
}

type Node interface {
	NotifyStop() <-chan struct{}
	NotifyError(err error)
	Propose(GroupID, []byte, InstanceID) error
	ProposeWithCtx(context.Context, GroupID, []byte, InstanceID) error
	BatchPropose(GroupID, []byte, InstanceID, uint32) error
	BatchProposeWithCtx(context.Context, GroupID, []byte, InstanceID, uint32) error
	CurrentInstanceID(GroupID) InstanceID
	GetNodeID() NodeID

	ReceiveMessage([]byte) error
}


type NodeID uint64
type GroupID uint16
type InstanceID uint64
type StateMachineID uint64

const (
	UnknownNodeID = NodeID(0)
)