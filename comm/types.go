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
	"math"
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
	Propose(uint16, uint32, []byte) error
	ProposeWithTimeout(uint16, uint32, []byte, time.Duration) error
	ProposeWithCtx(context.Context, uint16, uint32, []byte) error
	ProposeWithCtxTimeout(context.Context, uint16, uint32, []byte, time.Duration) error
	GetNodeID() uint64
	ReceiveMessage([]byte)
}

type Proposer interface {
	Propose(uint16, uint32, []byte) error
	ProposeWithTimeout(uint16, uint32, []byte, time.Duration) error
	ProposeWithCtx(context.Context, uint16, uint32, []byte) error
	ProposeWithCtxTimeout(context.Context, uint16, uint32, []byte, time.Duration) error
}

type StateMachine interface {
	GetStateMachineID() uint32
	Execute(context.Context, uint64, []byte) (interface{}, error)
	ExecuteForCheckpoint(uint64, []byte) error
	GetCheckpointInstanceID() uint64
	//LockCheckpointState() error
	//UnLockCheckpointState()
	//CheckpointState() error
	//LoadCheckpointState() error
}

//type NodeID uint64
//type InstanceID uint64
type Result struct {
	Ret interface{}
	Err error
}

const (
	UnknownNodeID = 0
	UnknownVersion = uint64(math.MaxUint64)
	GroupIDLen = int(unsafe.Sizeof(uint16(0)))
	SMIDLen = int(unsafe.Sizeof(uint32(0)))
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

type GroupConfig interface {
	FollowerMode() bool
	GetFollowNodeID() uint64
	GetGroupID() uint16
	GetClusterID() uint64
	GetMajorityCount() int
	GetLearnNodes() map[uint64]string
	GetMembers() map[uint64]string
	GetFollowers() map[uint64]string
	AddFollower(uint64)
}

type Group interface {
	Start(context.Context, <-chan struct{}) error
	Stop()
	Propose(context.Context, uint32, []byte) (<-chan Result, error)
	BatchPropose(context.Context, uint32, []byte, uint32) (<-chan Result, error)
	AddStateMachine(...StateMachine)
	ReceiveMessage([]byte)
	GetCurrentInstanceID() uint64
	PauseReplayer()
	ContinueReplayer()
	PauseCleaner()
	ContinueCleaner()
	SetMaxLogCount(int)
	AddMember(context.Context, uint64, string) (<-chan Result, error)
	RemoveMember(context.Context, uint64) (<-chan Result, error)
	ChangeMember(context.Context, uint64, string, uint64) (<-chan Result, error)
}

type Instance interface {
	Start(context.Context, <-chan struct{}) error
	Stop()
	IsReadyForNewValue() bool
	GetInstanceID() uint64
	GetChecksum() uint32
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
	GetProposerInstanceID() uint64
	GetCheckpointInstanceID() uint64
	ExecuteForCheckpoint(uint64, []byte) error
}

type Learner interface {
	SendCheckpointBegin(uint64, uint64, uint64, uint64)
	SendCheckpointEnd(uint64, uint64, uint64, uint64)
}