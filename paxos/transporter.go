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
	"fmt"
	"github.com/sosozhuang/paxos/network"
	"github.com/sosozhuang/paxos/util"
)

type Transporter interface {
	send(uint64, comm.MsgType, proto.Message) error
	broadcast(comm.MsgType, proto.Message) error
	broadcastToFollowers(comm.MsgType, proto.Message) error
	broadcastToLearnNodes(comm.MsgType, proto.Message) error
}

type transporter struct {
	groupCfg comm.GroupConfig
	sender   comm.Sender
}

func newTransporter(groupCfg comm.GroupConfig, sender comm.Sender) Transporter {
	return &transporter{
		groupCfg: groupCfg,
		sender:   sender,
	}
}

func (t *transporter) send(nodeID uint64, msgType comm.MsgType, msg proto.Message) error {
	b, err := t.pack(msgType, msg)
	if err != nil {
		log.Error(err)
		return err
	}

	member, ok := t.groupCfg.GetMembers()[nodeID]
	if !ok {
		return t.sender.SendMessage(network.Uint64ToAddr(nodeID), b)
		//return fmt.Errorf("transporter: can't find node id: %d", nodeID)
	}
	return t.sender.SendMessage(member.GetAddr(), b)
}

func (t *transporter) pack(msgType comm.MsgType, pb proto.Message) ([]byte, error) {
	header := &comm.Header{
		ClusterID: proto.Uint64(t.groupCfg.GetClusterID()),
		Type:      msgType.Enum(),
		Version:   proto.Int32(1),
	}
	b, err := util.ObjectToBytes(t.groupCfg.GetGroupID())
	if err != nil {
		return nil, fmt.Errorf("transporter: convert group id %d to bytes: %v",
			t.groupCfg.GetGroupID(), err)
	}
	h, err := proto.Marshal(header)
	if err != nil {
		return nil, fmt.Errorf("transporter: marshal message header: %v", err)
	}
	l, err := util.IntToBytes(len(h))
	if err != nil {
		return nil, fmt.Errorf("transporter: convert message header length to bytes: %v", err)
	}
	b = append(b, l...)
	b = append(b, h...)
	m, err := proto.Marshal(pb)
	if err != nil {
		return nil, fmt.Errorf("transporter: marshal message: %v", err)
	}
	l, err = util.IntToBytes(len(m))
	if err != nil {
		return nil, fmt.Errorf("transporter: convert message length to bytes: %v", err)
	}
	b = append(b, l...)
	b = append(b, m...)
	checksum := util.Checksum(b)
	l, err = util.ObjectToBytes(checksum)
	if err != nil {
		return nil, fmt.Errorf("transporter: convert checksum to bytes: %v", err)
	}
	b = append(b, l...)
	return b, nil
}

func (t *transporter) broadcast(msgType comm.MsgType, msg proto.Message) error {
	b, err := t.pack(msgType, msg)
	if err != nil {
		return err
	}

	errs := make([]error, 0)
	members := t.groupCfg.GetMembers()
	for id, member := range members {
		if id != t.groupCfg.GetNodeID() {
			if err = t.sender.SendMessage(member.GetAddr(), b); err != nil {
				errs = append(errs, fmt.Errorf("send message to %s: %v", member.GetAddr(), err))
			}
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("transporter: %v", errs)
	}
	return nil
}

func (t *transporter) broadcastToFollowers(msgType comm.MsgType, msg proto.Message) error {
	b, err := t.pack(msgType, msg)
	if err != nil {
		return err
	}

	errs := make([]error, 0)
	followers := t.groupCfg.GetFollowers()
	for _, addr := range followers {
		if err = t.sender.SendMessage(addr, b); err != nil {
			errs = append(errs, fmt.Errorf("send message to %s: %v", addr, err))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("transporter: %v", errs)
	}
	return nil
}

func (t *transporter) broadcastToLearnNodes(msgType comm.MsgType, msg proto.Message) error {
	b, err := t.pack(msgType, msg)
	if err != nil {
		return err
	}

	errs := make([]error, 0)
	nodes := t.groupCfg.GetLearnNodes()
	for _, addr := range nodes {
		if err = t.sender.SendMessage(addr, b); err != nil {
			errs = append(errs, fmt.Errorf("send message to %s: %v", addr, err))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("transporter: %v", errs)
	}
	return nil
}
