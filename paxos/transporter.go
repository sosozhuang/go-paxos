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
	"github.com/gogo/protobuf/proto"
	"github.com/sosozhuang/paxos/comm"
	"hash/crc32"
)

type Transporter interface {
	send(uint64, comm.MsgType, proto.Message)
	broadcast(comm.MsgType, proto.Message)
	broadcastToFollowers(comm.MsgType, proto.Message)
	broadcastToLearnNodes(comm.MsgType, proto.Message)
}

type transporter struct {
	nodeID   uint64
	groupCfg comm.GroupConfig
	sender   comm.Sender
}

func NewTransporter(nodeID uint64, groupCfg comm.GroupConfig, sender comm.Sender) Transporter {
	return &transporter{
		nodeID:   nodeID,
		groupCfg: groupCfg,
		sender:   sender,
	}
}

func (t *transporter) send(nodeID uint64, msgType comm.MsgType, msg proto.Message) {
	b, err := t.pack(msgType, msg)
	if err != nil {
		log.Error(err)
		return
	}

	addr, ok := t.groupCfg.GetMembers()[nodeID]
	if !ok {
		log.Error("can't find node")
		return
	}
	t.sender.SendMessage(addr, b)
}

func (t *transporter) pack(msgType comm.MsgType, pb proto.Message) ([]byte, error) {
	header := &comm.Header{
		ClusterID: proto.Uint64(t.groupCfg.GetClusterID()),
		Type:      msgType.Enum(),
		Version:   proto.Int32(1),
	}
	b, err := comm.ObjectToBytes(t.groupCfg.GetGroupID())
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

func (t *transporter) broadcast(msgType comm.MsgType, msg proto.Message) {
	b, err := t.pack(msgType, msg)
	if err != nil {
		log.Error(err)
		return
	}

	members := t.groupCfg.GetMembers()
	for id, addr := range members {
		if id != t.nodeID {
			if err = t.sender.SendMessage(addr, b); err != nil {
				log.Error(err)
			}
		}
	}
}

func (t *transporter) broadcastToFollowers(msgType comm.MsgType, msg proto.Message) {
	b, err := t.pack(msgType, msg)
	if err != nil {
		log.Error(err)
		return
	}

	followers := t.groupCfg.GetFollowers()
	for _, addr := range followers {
		if err = t.sender.SendMessage(addr, b); err != nil {
			log.Error(err)
		}
	}
}

func (t *transporter) broadcastToLearnNodes(msgType comm.MsgType, msg proto.Message) {
	b, err := t.pack(msgType, msg)
	if err != nil {
		log.Error(err)
		return
	}

	nodes := t.groupCfg.GetLearnNodes()
	for _, addr := range nodes {
		if err = t.sender.SendMessage(addr, b); err != nil {
			log.Error(err)
		}
	}
}
