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
	"github.com/sosozhuang/paxos/comm"
	"github.com/gogo/protobuf/proto"
	"hash/crc32"
)
type broadcastType int
const (
	localFirst broadcastType = iota
	remoteFirst
	remoteOnly
)
var crcTable = crc32.MakeTable(crc32.Castagnoli)
type Transporter interface {
	Send()
	Broadcast(comm.PaxosMsg, broadcastType)
	BroadcastToFollower()
}

func NewTransport() (Transporter, error) {
	return nil, nil
}

type transporter struct {
	nodeID comm.NodeID
	groupID comm.GroupID
	sender comm.Sender
}

func (t *transporter) Send() {
	t.sender.SendMessage()
}

func (t *transporter) pack(msgType comm.MsgType, pb proto.Message) ([]byte, error) {
	header := &comm.Header{
		ClusterID: proto.Uint64(0),
		Type: msgType.Enum(),
		Version: proto.Int32(1),
	}
	b, err := comm.ObjectToBytes(t.groupID)
	if err != nil {
		return nil, err
	}
	h, err := proto.Marshal(header)
	if err != nil {
		return nil, err
	}
	l, err := comm.IntToBytes(len(h))
	if err != nil {
		return nil, err
	}
	b = append(b, l...)
	b = append(b, h...)
	m, err := proto.Marshal(pb)
	if err != nil {
		return nil, err
	}
	l, err = comm.IntToBytes(len(m))
	if err != nil {
		return nil, err
	}
	b = append(b, l...)
	b = append(b, m...)
	checksum := crc32.Update(0, crcTable, b)
	l, err = comm.ObjectToBytes(checksum)
	if err != nil {
		return nil, err
	}
	b = append(b, l...)
	return b, nil
}

func (t *transporter) Broadcast() {
	t.sender.SendMessage()
}

func (t *transporter) BoradcastToFollower() {

}