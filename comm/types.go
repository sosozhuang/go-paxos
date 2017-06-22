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
	"context"
	"math"
	"time"
	"unsafe"
	"errors"
)

const (
	UnknownNodeID     = 0
	UnknownClusterID  = 0
	UnknownVersion    = uint64(math.MaxUint64)
	UnknownInstanceID = uint64(math.MaxUint64)
	GroupIDLen        = int(unsafe.Sizeof(uint16(0)))
	SMIDLen           = int(unsafe.Sizeof(uint32(0)))
	Int32Len          = int(unsafe.Sizeof(int32(0)))
)

const (
	ClusterStateMachineID uint32 = 100000000 + iota
	LeaderStateMachineID
	KVStoreStateMachineID
)

var (
	ErrClusterUninitialized  = errors.New("group: cluster uninitialized")
	ErrMemberExists          = errors.New("group: member already exists")
	ErrMemberNotExists       = errors.New("group: member not exists")
	ErrMemberNotModified     = errors.New("group: member not modified")
	ErrMemberNameEmpty       = errors.New("group: empty member name")
	ErrMemberAddrEmpty       = errors.New("group: empty member address")
	ErrMemberServiceUrlEmpty = errors.New("group: empty member service url")

)

type Receiver interface {
	ReceiveMessage([]byte)
}

type Sender interface {
	SendMessage(string, []byte) error
}

type NetWork interface {
	Start(<-chan struct{}) error
	StopServer()
	StopClient()
	Sender
}

type Node interface {
	Receiver
	Proposer
	GetNodeID() uint64
	GetLeader() Member
	IsLeader() bool
	GetMembers() (map[uint64]Member, error)
	AddMember(Member) error
	UpdateMember(Member, Member) error
	RemoveMember(Member) error
}

type Membership interface {
	GetMembers() (map[uint64]Member, error)
	AddMember(Member) error
	UpdateMember(Member, Member) error
	RemoveMember(Member) error
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
	GetCheckpoint() ([]byte, error)
	UpdateByCheckpoint(b []byte) error
	LockCheckpointState() error
	UnlockCheckpointState()
	GetCheckpointState() (string, []string)
}

type Result struct {
	Ret interface{}
	Err error
}

type GroupConfig interface {
	GetNodeID() uint64
	GetGroupID() uint16
	GetClusterID() uint64
	FollowerMode() bool
	GetFollowNodeID() uint64
	GetMemberCount() int
	GetMajorityCount() int
	GetLearnNodes() map[uint64]string
	GetMembers() map[uint64]Member
	GetFollowers() map[uint64]string
	AddFollower(uint64)
	IsEnableReplayer() bool
	GetClusterCheckpoint() ([]byte, error)
	UpdateClusterByCheckpoint([]byte) error
	GetLeaderCheckpoint() ([]byte, error)
	UpdateLeaderByCheckpoint([]byte) error
}

type Group interface {
	Receiver
	Start(context.Context, <-chan struct{}) error
	Stop()
	AddStateMachine(...StateMachine)
	Propose(context.Context, uint32, []byte) (<-chan Result, error)
	BatchPropose(context.Context, uint32, []byte, uint32) (<-chan Result, error)
	GetCurrentInstanceID() uint64
	PauseReplayer()
	ContinueReplayer()
	PauseCleaner()
	ContinueCleaner()
	SetMaxLogCount(int64)
	GetMemberCount() int
	GetMembers() (map[uint64]Member, error)
	SetMembers(map[uint64]Member)
	AddMember(context.Context, Member) (<-chan Result, error)
	RemoveMember(context.Context, Member) (<-chan Result, error)
	UpdateMember(context.Context, Member, Member) (<-chan Result, error)
}

type Instance interface {
	Start(context.Context, <-chan struct{}) error
	Stop()
	AddStateMachine(...StateMachine)
	IsReadyForNewValue() bool
	NewValue(context.Context)
	GetProposerInstanceID() uint64
	GetInstanceID() uint64
	ReceivePaxosMessage(*PaxosMsg)
	ReceiveCheckpointMessage(*CheckpointMsg)
	PauseReplayer()
	ContinueReplayer()
	PauseCleaner()
	ContinueCleaner()
	SetMaxLogCount(int64)
}

type CheckpointInstance interface {
	GetCheckpointInstanceID() uint64
	ExecuteForCheckpoint(uint64, []byte) error
}