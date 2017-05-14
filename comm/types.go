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
	"net"
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
	Propose(GroupID, []byte, InstanceID) error
	ProposeWithCtx(context.Context, GroupID, []byte, InstanceID) error
	BatchPropose(GroupID, []byte, InstanceID, uint32) error
	BatchProposeWithCtx(context.Context, GroupID, []byte, InstanceID, uint32) error
	CurrentInstanceID(GroupID) InstanceID
	GetNodeID() NodeID
	GetNodeCount() int
	GetListenAddr() *net.TCPAddr

	ReceiveMessage([]byte)
}

type NodeID uint64
type GroupID uint16
type InstanceID uint64
type StateMachineID uint64

const (
	UnknownNodeID = NodeID(0)
	GroupIDLen = int(unsafe.Sizeof(GroupID(0)))
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
