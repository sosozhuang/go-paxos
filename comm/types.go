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

import (
	"bytes"
	"context"
	"encoding/binary"
	"unsafe"
	"time"
)

type Receiver interface {
	ReceiveMessage([]byte)
}

type Sender interface {
	SendMessage(string, []byte) error
}

type NetWork interface {
	Start(context.Context, <-chan struct{}) error
	Stop()
	Sender
}

type Node interface {
	NotifyStop() <-chan struct{}
	NotifyError(err error)
	Propose(GroupID, StateMachineID, []byte) error
	ProposeWithTimeout(GroupID, StateMachineID, []byte, time.Duration) error
	ProposeWithCtx(context.Context, GroupID, StateMachineID, []byte) error
	ProposeWithCtxTimeout(context.Context, GroupID, StateMachineID, []byte, time.Duration) error
	GetNodeID() NodeID

	ReceiveMessage([]byte)
}

type StateMachine interface {
	Execute(context.Context, InstanceID, []byte) (interface{}, error)
	GetStateMachineID() StateMachineID
	ExecuteForCheckpoint(InstanceID, []byte) error
	GetCheckpointInstanceID() InstanceID
	//LockCheckpointState() error
	//UnLockCheckpointState()
	//CheckpointState() error
	//LoadCheckpointState() error
}

type NodeID uint64
type GroupID uint16
type InstanceID uint64
type StateMachineID uint64
type Result struct {
	Ret interface{}
	Err error
}

const (
	UnknownNodeID = 0
	GroupIDLen = int(unsafe.Sizeof(GroupID(0)))
	SMIDLen = int(unsafe.Sizeof(StateMachineID(0)))
	Int32Len = int(unsafe.Sizeof(int32(0)))
)

func ObjectToBytes(i interface{}) ([]byte, error) {
	bytesBuffer := bytes.NewBuffer([]byte{})
	if err := binary.Write(bytesBuffer, binary.BigEndian, i); err != nil {
		return nil, err
	}
	return bytesBuffer.Bytes(), nil
}

func BytesToObject(b []byte, i interface{}) error {
	bytesBuffer := bytes.NewBuffer(b)
	return binary.Read(bytesBuffer, binary.BigEndian, i)
}

func IntToBytes(n int) ([]byte, error) {
	d := int32(n)
	bytesBuffer := bytes.NewBuffer([]byte{})
	if err := binary.Write(bytesBuffer, binary.BigEndian, d); err != nil {
		return nil, err
	}
	return bytesBuffer.Bytes(), nil
}

func BytesToInt(b []byte) (int, error) {
	bytesBuffer := bytes.NewBuffer(b)
	var n int32
	if err := binary.Read(bytesBuffer, binary.BigEndian, &n); err != nil {
		return 0, err
	}
	return int(n), nil
}

type Group interface {
	Start(context.Context, <-chan struct{}) error
	Stop()
	Propose(context.Context, StateMachineID, []byte) (<-chan Result, error)
	BatchPropose(context.Context, StateMachineID, []byte, uint32) (<-chan Result, error)
	FollowerMode() bool
	FollowNodeID() NodeID
	EnableMemberShip() bool
	AddStateMachines(...StateMachine)
	ReceiveMessage([]byte)
	GetCurrentInstanceID() InstanceID
	PauseReplayer()
	ContinueReplayer()
	PauseCleaner()
	ContinueCleaner()
	SetMaxLogCount(int)
	GetGroupID() GroupID
	GetClusterID() uint64
	GetMajorityCount() int
	GetLearnNodes() map[NodeID]string
	GetMembers() map[NodeID]string
	AddMember(context.Context, NodeID, string) (<-chan Result, error)
	RemoveMember(context.Context, NodeID) (<-chan Result, error)
	ChangeMember(context.Context, NodeID, string, NodeID) (<-chan Result, error)
	GetFollowers() map[NodeID]string
	AddFollower(NodeID)
}

type Checksum uint32

type Instance interface {
	Start(context.Context, <-chan struct{}) error
	Stop()
	IsReadyForNewValue() bool
	GetInstanceID() InstanceID
	GetChecksum() Checksum
	NewValue(context.Context)
	AddStateMachine(...StateMachine)
	ReceivePaxosMessage(*PaxosMsg)
	ReceiveCheckpointMessage(*CheckpointMsg)
	PauseReplayer()
	ContinueReplayer()
	PauseCleaner()
	ContinueCleaner()
	SetMaxLogCount(int)
	GetMajorityCount() int
	GetProposerInstanceID() InstanceID
	GetCheckpointInstanceID() InstanceID
	ExecuteForCheckpoint(InstanceID, []byte) error
}

type Learner interface {
	SendCheckpointBegin(NodeID, uint64, uint64, InstanceID)
	SendCheckpointEnd(NodeID, uint64, uint64, InstanceID)
}