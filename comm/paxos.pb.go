// Code generated by protoc-gen-go.
// source: paxos.proto
// DO NOT EDIT!

/*
Package comm is a generated protocol buffer package.

It is generated from these files:
	paxos.proto

It has these top-level messages:
	Header
	PaxosMsg
	CheckpointMsg
	AcceptorState
	Member
	ClusterInfo
	LeaderInfo
	KeyValue
*/
package comm

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

// protoc --go_out=. *.proto
type MsgType int32

const (
	MsgType_Paxos      MsgType = 0
	MsgType_Checkpoint MsgType = 1
)

var MsgType_name = map[int32]string{
	0: "Paxos",
	1: "Checkpoint",
}
var MsgType_value = map[string]int32{
	"Paxos":      0,
	"Checkpoint": 1,
}

func (x MsgType) Enum() *MsgType {
	p := new(MsgType)
	*p = x
	return p
}
func (x MsgType) String() string {
	return proto.EnumName(MsgType_name, int32(x))
}
func (x *MsgType) UnmarshalJSON(data []byte) error {
	value, err := proto.UnmarshalJSONEnum(MsgType_value, data, "MsgType")
	if err != nil {
		return err
	}
	*x = MsgType(value)
	return nil
}
func (MsgType) EnumDescriptor() ([]byte, []int) { return fileDescriptor0, []int{0} }

type PaxosMsgType int32

const (
	// for proposer
	PaxosMsgType_NewValue     PaxosMsgType = 1
	PaxosMsgType_PrepareReply PaxosMsgType = 2
	PaxosMsgType_AcceptReply  PaxosMsgType = 3
	// for acceptor
	PaxosMsgType_Prepare PaxosMsgType = 4
	PaxosMsgType_Accept  PaxosMsgType = 5
	// for learner
	PaxosMsgType_ProposalFinished   PaxosMsgType = 6
	PaxosMsgType_AskForLearn        PaxosMsgType = 7
	PaxosMsgType_ConfirmAskForLearn PaxosMsgType = 8
	PaxosMsgType_SendValue          PaxosMsgType = 9
	PaxosMsgType_AckSendValue       PaxosMsgType = 10
	PaxosMsgType_SendInstanceID     PaxosMsgType = 11
	PaxosMsgType_AskForCheckpoint   PaxosMsgType = 12
)

var PaxosMsgType_name = map[int32]string{
	1:  "NewValue",
	2:  "PrepareReply",
	3:  "AcceptReply",
	4:  "Prepare",
	5:  "Accept",
	6:  "ProposalFinished",
	7:  "AskForLearn",
	8:  "ConfirmAskForLearn",
	9:  "SendValue",
	10: "AckSendValue",
	11: "SendInstanceID",
	12: "AskForCheckpoint",
}
var PaxosMsgType_value = map[string]int32{
	"NewValue":           1,
	"PrepareReply":       2,
	"AcceptReply":        3,
	"Prepare":            4,
	"Accept":             5,
	"ProposalFinished":   6,
	"AskForLearn":        7,
	"ConfirmAskForLearn": 8,
	"SendValue":          9,
	"AckSendValue":       10,
	"SendInstanceID":     11,
	"AskForCheckpoint":   12,
}

func (x PaxosMsgType) Enum() *PaxosMsgType {
	p := new(PaxosMsgType)
	*p = x
	return p
}
func (x PaxosMsgType) String() string {
	return proto.EnumName(PaxosMsgType_name, int32(x))
}
func (x *PaxosMsgType) UnmarshalJSON(data []byte) error {
	value, err := proto.UnmarshalJSONEnum(PaxosMsgType_value, data, "PaxosMsgType")
	if err != nil {
		return err
	}
	*x = PaxosMsgType(value)
	return nil
}
func (PaxosMsgType) EnumDescriptor() ([]byte, []int) { return fileDescriptor0, []int{1} }

type CheckpointMsgType int32

const (
	CheckpointMsgType_SendFile    CheckpointMsgType = 0
	CheckpointMsgType_AckSendFile CheckpointMsgType = 1
)

var CheckpointMsgType_name = map[int32]string{
	0: "SendFile",
	1: "AckSendFile",
}
var CheckpointMsgType_value = map[string]int32{
	"SendFile":    0,
	"AckSendFile": 1,
}

func (x CheckpointMsgType) Enum() *CheckpointMsgType {
	p := new(CheckpointMsgType)
	*p = x
	return p
}
func (x CheckpointMsgType) String() string {
	return proto.EnumName(CheckpointMsgType_name, int32(x))
}
func (x *CheckpointMsgType) UnmarshalJSON(data []byte) error {
	value, err := proto.UnmarshalJSONEnum(CheckpointMsgType_value, data, "CheckpointMsgType")
	if err != nil {
		return err
	}
	*x = CheckpointMsgType(value)
	return nil
}
func (CheckpointMsgType) EnumDescriptor() ([]byte, []int) { return fileDescriptor0, []int{2} }

type CheckPointMsgFlag int32

const (
	CheckPointMsgFlag_Begin       CheckPointMsgFlag = 0
	CheckPointMsgFlag_Progressing CheckPointMsgFlag = 1
	CheckPointMsgFlag_End         CheckPointMsgFlag = 2
	CheckPointMsgFlag_Successful  CheckPointMsgFlag = 3
	CheckPointMsgFlag_Failed      CheckPointMsgFlag = 4
)

var CheckPointMsgFlag_name = map[int32]string{
	0: "Begin",
	1: "Progressing",
	2: "End",
	3: "Successful",
	4: "Failed",
}
var CheckPointMsgFlag_value = map[string]int32{
	"Begin":       0,
	"Progressing": 1,
	"End":         2,
	"Successful":  3,
	"Failed":      4,
}

func (x CheckPointMsgFlag) Enum() *CheckPointMsgFlag {
	p := new(CheckPointMsgFlag)
	*p = x
	return p
}
func (x CheckPointMsgFlag) String() string {
	return proto.EnumName(CheckPointMsgFlag_name, int32(x))
}
func (x *CheckPointMsgFlag) UnmarshalJSON(data []byte) error {
	value, err := proto.UnmarshalJSONEnum(CheckPointMsgFlag_value, data, "CheckPointMsgFlag")
	if err != nil {
		return err
	}
	*x = CheckPointMsgFlag(value)
	return nil
}
func (CheckPointMsgFlag) EnumDescriptor() ([]byte, []int) { return fileDescriptor0, []int{3} }

// message LeaderElection {
//    required uint64 NodeID = 1;
//    required uint64 Version = 2;
//    required int64 ElectionTimeout = 3;
// };
type Action int32

const (
	Action_Set    Action = 0
	Action_Get    Action = 1
	Action_Update Action = 2
	Action_Delete Action = 3
)

var Action_name = map[int32]string{
	0: "Set",
	1: "Get",
	2: "Update",
	3: "Delete",
}
var Action_value = map[string]int32{
	"Set":    0,
	"Get":    1,
	"Update": 2,
	"Delete": 3,
}

func (x Action) Enum() *Action {
	p := new(Action)
	*p = x
	return p
}
func (x Action) String() string {
	return proto.EnumName(Action_name, int32(x))
}
func (x *Action) UnmarshalJSON(data []byte) error {
	value, err := proto.UnmarshalJSONEnum(Action_value, data, "Action")
	if err != nil {
		return err
	}
	*x = Action(value)
	return nil
}
func (Action) EnumDescriptor() ([]byte, []int) { return fileDescriptor0, []int{4} }

type Header struct {
	ClusterID        *uint64  `protobuf:"varint,1,req,name=ClusterID" json:"ClusterID,omitempty"`
	Type             *MsgType `protobuf:"varint,2,req,name=Type,enum=comm.MsgType" json:"Type,omitempty"`
	Version          *int32   `protobuf:"varint,3,opt,name=Version" json:"Version,omitempty"`
	XXX_unrecognized []byte   `json:"-"`
}

func (m *Header) Reset()                    { *m = Header{} }
func (m *Header) String() string            { return proto.CompactTextString(m) }
func (*Header) ProtoMessage()               {}
func (*Header) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{0} }

func (m *Header) GetClusterID() uint64 {
	if m != nil && m.ClusterID != nil {
		return *m.ClusterID
	}
	return 0
}

func (m *Header) GetType() MsgType {
	if m != nil && m.Type != nil {
		return *m.Type
	}
	return MsgType_Paxos
}

func (m *Header) GetVersion() int32 {
	if m != nil && m.Version != nil {
		return *m.Version
	}
	return 0
}

type PaxosMsg struct {
	Type                *PaxosMsgType `protobuf:"varint,1,req,name=Type,enum=comm.PaxosMsgType" json:"Type,omitempty"`
	InstanceID          *uint64       `protobuf:"varint,2,opt,name=InstanceID" json:"InstanceID,omitempty"`
	NodeID              *uint64       `protobuf:"varint,3,opt,name=NodeID" json:"NodeID,omitempty"`
	ProposalID          *uint64       `protobuf:"varint,4,opt,name=ProposalID" json:"ProposalID,omitempty"`
	ProposalNodeID      *uint64       `protobuf:"varint,5,opt,name=ProposalNodeID" json:"ProposalNodeID,omitempty"`
	Value               []byte        `protobuf:"bytes,6,opt,name=Value" json:"Value,omitempty"`
	PreAcceptID         *uint64       `protobuf:"varint,7,opt,name=PreAcceptID" json:"PreAcceptID,omitempty"`
	PreAcceptNodeID     *uint64       `protobuf:"varint,8,opt,name=PreAcceptNodeID" json:"PreAcceptNodeID,omitempty"`
	RejectByPromiseID   *uint64       `protobuf:"varint,9,opt,name=RejectByPromiseID" json:"RejectByPromiseID,omitempty"`
	CurInstanceID       *uint64       `protobuf:"varint,10,opt,name=CurInstanceID" json:"CurInstanceID,omitempty"`
	MinChosenInstanceID *uint64       `protobuf:"varint,11,opt,name=MinChosenInstanceID" json:"MinChosenInstanceID,omitempty"`
	Checksum            *uint32       `protobuf:"varint,12,opt,name=Checksum" json:"Checksum,omitempty"`
	AckFlag             *bool         `protobuf:"varint,13,opt,name=AckFlag" json:"AckFlag,omitempty"`
	ClusterInfo         []byte        `protobuf:"bytes,14,opt,name=ClusterInfo" json:"ClusterInfo,omitempty"`
	LeaderInfo          []byte        `protobuf:"bytes,15,opt,name=LeaderInfo" json:"LeaderInfo,omitempty"`
	XXX_unrecognized    []byte        `json:"-"`
}

func (m *PaxosMsg) Reset()                    { *m = PaxosMsg{} }
func (m *PaxosMsg) String() string            { return proto.CompactTextString(m) }
func (*PaxosMsg) ProtoMessage()               {}
func (*PaxosMsg) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{1} }

func (m *PaxosMsg) GetType() PaxosMsgType {
	if m != nil && m.Type != nil {
		return *m.Type
	}
	return PaxosMsgType_NewValue
}

func (m *PaxosMsg) GetInstanceID() uint64 {
	if m != nil && m.InstanceID != nil {
		return *m.InstanceID
	}
	return 0
}

func (m *PaxosMsg) GetNodeID() uint64 {
	if m != nil && m.NodeID != nil {
		return *m.NodeID
	}
	return 0
}

func (m *PaxosMsg) GetProposalID() uint64 {
	if m != nil && m.ProposalID != nil {
		return *m.ProposalID
	}
	return 0
}

func (m *PaxosMsg) GetProposalNodeID() uint64 {
	if m != nil && m.ProposalNodeID != nil {
		return *m.ProposalNodeID
	}
	return 0
}

func (m *PaxosMsg) GetValue() []byte {
	if m != nil {
		return m.Value
	}
	return nil
}

func (m *PaxosMsg) GetPreAcceptID() uint64 {
	if m != nil && m.PreAcceptID != nil {
		return *m.PreAcceptID
	}
	return 0
}

func (m *PaxosMsg) GetPreAcceptNodeID() uint64 {
	if m != nil && m.PreAcceptNodeID != nil {
		return *m.PreAcceptNodeID
	}
	return 0
}

func (m *PaxosMsg) GetRejectByPromiseID() uint64 {
	if m != nil && m.RejectByPromiseID != nil {
		return *m.RejectByPromiseID
	}
	return 0
}

func (m *PaxosMsg) GetCurInstanceID() uint64 {
	if m != nil && m.CurInstanceID != nil {
		return *m.CurInstanceID
	}
	return 0
}

func (m *PaxosMsg) GetMinChosenInstanceID() uint64 {
	if m != nil && m.MinChosenInstanceID != nil {
		return *m.MinChosenInstanceID
	}
	return 0
}

func (m *PaxosMsg) GetChecksum() uint32 {
	if m != nil && m.Checksum != nil {
		return *m.Checksum
	}
	return 0
}

func (m *PaxosMsg) GetAckFlag() bool {
	if m != nil && m.AckFlag != nil {
		return *m.AckFlag
	}
	return false
}

func (m *PaxosMsg) GetClusterInfo() []byte {
	if m != nil {
		return m.ClusterInfo
	}
	return nil
}

func (m *PaxosMsg) GetLeaderInfo() []byte {
	if m != nil {
		return m.LeaderInfo
	}
	return nil
}

type CheckpointMsg struct {
	Type                 *CheckpointMsgType `protobuf:"varint,1,req,name=Type,enum=comm.CheckpointMsgType" json:"Type,omitempty"`
	NodeID               *uint64            `protobuf:"varint,2,req,name=NodeID" json:"NodeID,omitempty"`
	Flag                 *CheckPointMsgFlag `protobuf:"varint,3,opt,name=Flag,enum=comm.CheckPointMsgFlag" json:"Flag,omitempty"`
	UUID                 *uint64            `protobuf:"varint,4,req,name=UUID" json:"UUID,omitempty"`
	Sequence             *uint64            `protobuf:"varint,5,req,name=Sequence" json:"Sequence,omitempty"`
	CheckpointInstanceID *uint64            `protobuf:"varint,6,opt,name=CheckpointInstanceID" json:"CheckpointInstanceID,omitempty"`
	Checksum             *uint32            `protobuf:"varint,7,opt,name=Checksum" json:"Checksum,omitempty"`
	FilePath             *string            `protobuf:"bytes,8,opt,name=FilePath" json:"FilePath,omitempty"`
	SMID                 *uint32            `protobuf:"varint,9,opt,name=SMID" json:"SMID,omitempty"`
	Offset               *int64             `protobuf:"varint,10,opt,name=Offset" json:"Offset,omitempty"`
	Bytes                []byte             `protobuf:"bytes,11,opt,name=Bytes" json:"Bytes,omitempty"`
	XXX_unrecognized     []byte             `json:"-"`
}

func (m *CheckpointMsg) Reset()                    { *m = CheckpointMsg{} }
func (m *CheckpointMsg) String() string            { return proto.CompactTextString(m) }
func (*CheckpointMsg) ProtoMessage()               {}
func (*CheckpointMsg) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{2} }

func (m *CheckpointMsg) GetType() CheckpointMsgType {
	if m != nil && m.Type != nil {
		return *m.Type
	}
	return CheckpointMsgType_SendFile
}

func (m *CheckpointMsg) GetNodeID() uint64 {
	if m != nil && m.NodeID != nil {
		return *m.NodeID
	}
	return 0
}

func (m *CheckpointMsg) GetFlag() CheckPointMsgFlag {
	if m != nil && m.Flag != nil {
		return *m.Flag
	}
	return CheckPointMsgFlag_Begin
}

func (m *CheckpointMsg) GetUUID() uint64 {
	if m != nil && m.UUID != nil {
		return *m.UUID
	}
	return 0
}

func (m *CheckpointMsg) GetSequence() uint64 {
	if m != nil && m.Sequence != nil {
		return *m.Sequence
	}
	return 0
}

func (m *CheckpointMsg) GetCheckpointInstanceID() uint64 {
	if m != nil && m.CheckpointInstanceID != nil {
		return *m.CheckpointInstanceID
	}
	return 0
}

func (m *CheckpointMsg) GetChecksum() uint32 {
	if m != nil && m.Checksum != nil {
		return *m.Checksum
	}
	return 0
}

func (m *CheckpointMsg) GetFilePath() string {
	if m != nil && m.FilePath != nil {
		return *m.FilePath
	}
	return ""
}

func (m *CheckpointMsg) GetSMID() uint32 {
	if m != nil && m.SMID != nil {
		return *m.SMID
	}
	return 0
}

func (m *CheckpointMsg) GetOffset() int64 {
	if m != nil && m.Offset != nil {
		return *m.Offset
	}
	return 0
}

func (m *CheckpointMsg) GetBytes() []byte {
	if m != nil {
		return m.Bytes
	}
	return nil
}

type AcceptorState struct {
	InstanceID       *uint64 `protobuf:"varint,1,req,name=InstanceID" json:"InstanceID,omitempty"`
	PromisedID       *uint64 `protobuf:"varint,2,req,name=PromisedID" json:"PromisedID,omitempty"`
	PromisedNodeID   *uint64 `protobuf:"varint,3,req,name=PromisedNodeID" json:"PromisedNodeID,omitempty"`
	AcceptedID       *uint64 `protobuf:"varint,4,req,name=AcceptedID" json:"AcceptedID,omitempty"`
	AcceptedNodeID   *uint64 `protobuf:"varint,5,req,name=AcceptedNodeID" json:"AcceptedNodeID,omitempty"`
	AcceptedValue    []byte  `protobuf:"bytes,6,opt,name=AcceptedValue" json:"AcceptedValue,omitempty"`
	Checksum         *uint32 `protobuf:"varint,7,req,name=Checksum" json:"Checksum,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (m *AcceptorState) Reset()                    { *m = AcceptorState{} }
func (m *AcceptorState) String() string            { return proto.CompactTextString(m) }
func (*AcceptorState) ProtoMessage()               {}
func (*AcceptorState) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{3} }

func (m *AcceptorState) GetInstanceID() uint64 {
	if m != nil && m.InstanceID != nil {
		return *m.InstanceID
	}
	return 0
}

func (m *AcceptorState) GetPromisedID() uint64 {
	if m != nil && m.PromisedID != nil {
		return *m.PromisedID
	}
	return 0
}

func (m *AcceptorState) GetPromisedNodeID() uint64 {
	if m != nil && m.PromisedNodeID != nil {
		return *m.PromisedNodeID
	}
	return 0
}

func (m *AcceptorState) GetAcceptedID() uint64 {
	if m != nil && m.AcceptedID != nil {
		return *m.AcceptedID
	}
	return 0
}

func (m *AcceptorState) GetAcceptedNodeID() uint64 {
	if m != nil && m.AcceptedNodeID != nil {
		return *m.AcceptedNodeID
	}
	return 0
}

func (m *AcceptorState) GetAcceptedValue() []byte {
	if m != nil {
		return m.AcceptedValue
	}
	return nil
}

func (m *AcceptorState) GetChecksum() uint32 {
	if m != nil && m.Checksum != nil {
		return *m.Checksum
	}
	return 0
}

type Member struct {
	NodeID           *uint64 `protobuf:"varint,1,req,name=NodeID" json:"NodeID,omitempty"`
	Name             *string `protobuf:"bytes,2,req,name=Name" json:"Name,omitempty"`
	Addr             *string `protobuf:"bytes,3,req,name=Addr" json:"Addr,omitempty"`
	ServiceUrl       *string `protobuf:"bytes,4,opt,name=ServiceUrl" json:"ServiceUrl,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (m *Member) Reset()                    { *m = Member{} }
func (m *Member) String() string            { return proto.CompactTextString(m) }
func (*Member) ProtoMessage()               {}
func (*Member) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{4} }

func (m *Member) GetNodeID() uint64 {
	if m != nil && m.NodeID != nil {
		return *m.NodeID
	}
	return 0
}

func (m *Member) GetName() string {
	if m != nil && m.Name != nil {
		return *m.Name
	}
	return ""
}

func (m *Member) GetAddr() string {
	if m != nil && m.Addr != nil {
		return *m.Addr
	}
	return ""
}

func (m *Member) GetServiceUrl() string {
	if m != nil && m.ServiceUrl != nil {
		return *m.ServiceUrl
	}
	return ""
}

type ClusterInfo struct {
	ClusterID        *uint64   `protobuf:"varint,1,req,name=ClusterID" json:"ClusterID,omitempty"`
	Members          []*Member `protobuf:"bytes,2,rep,name=Members" json:"Members,omitempty"`
	Version          *uint64   `protobuf:"varint,3,req,name=Version" json:"Version,omitempty"`
	XXX_unrecognized []byte    `json:"-"`
}

func (m *ClusterInfo) Reset()                    { *m = ClusterInfo{} }
func (m *ClusterInfo) String() string            { return proto.CompactTextString(m) }
func (*ClusterInfo) ProtoMessage()               {}
func (*ClusterInfo) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{5} }

func (m *ClusterInfo) GetClusterID() uint64 {
	if m != nil && m.ClusterID != nil {
		return *m.ClusterID
	}
	return 0
}

func (m *ClusterInfo) GetMembers() []*Member {
	if m != nil {
		return m.Members
	}
	return nil
}

func (m *ClusterInfo) GetVersion() uint64 {
	if m != nil && m.Version != nil {
		return *m.Version
	}
	return 0
}

type LeaderInfo struct {
	Leader           *Member `protobuf:"bytes,1,req,name=Leader" json:"Leader,omitempty"`
	Version          *uint64 `protobuf:"varint,2,req,name=Version" json:"Version,omitempty"`
	ElectionTimeout  *int64  `protobuf:"varint,3,req,name=ElectionTimeout" json:"ElectionTimeout,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (m *LeaderInfo) Reset()                    { *m = LeaderInfo{} }
func (m *LeaderInfo) String() string            { return proto.CompactTextString(m) }
func (*LeaderInfo) ProtoMessage()               {}
func (*LeaderInfo) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{6} }

func (m *LeaderInfo) GetLeader() *Member {
	if m != nil {
		return m.Leader
	}
	return nil
}

func (m *LeaderInfo) GetVersion() uint64 {
	if m != nil && m.Version != nil {
		return *m.Version
	}
	return 0
}

func (m *LeaderInfo) GetElectionTimeout() int64 {
	if m != nil && m.ElectionTimeout != nil {
		return *m.ElectionTimeout
	}
	return 0
}

type KeyValue struct {
	Action           *Action `protobuf:"varint,1,req,name=Action,enum=comm.Action" json:"Action,omitempty"`
	Key              []byte  `protobuf:"bytes,2,req,name=Key" json:"Key,omitempty"`
	Value            []byte  `protobuf:"bytes,3,opt,name=Value" json:"Value,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (m *KeyValue) Reset()                    { *m = KeyValue{} }
func (m *KeyValue) String() string            { return proto.CompactTextString(m) }
func (*KeyValue) ProtoMessage()               {}
func (*KeyValue) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{7} }

func (m *KeyValue) GetAction() Action {
	if m != nil && m.Action != nil {
		return *m.Action
	}
	return Action_Set
}

func (m *KeyValue) GetKey() []byte {
	if m != nil {
		return m.Key
	}
	return nil
}

func (m *KeyValue) GetValue() []byte {
	if m != nil {
		return m.Value
	}
	return nil
}

func init() {
	proto.RegisterType((*Header)(nil), "comm.Header")
	proto.RegisterType((*PaxosMsg)(nil), "comm.PaxosMsg")
	proto.RegisterType((*CheckpointMsg)(nil), "comm.CheckpointMsg")
	proto.RegisterType((*AcceptorState)(nil), "comm.AcceptorState")
	proto.RegisterType((*Member)(nil), "comm.Member")
	proto.RegisterType((*ClusterInfo)(nil), "comm.ClusterInfo")
	proto.RegisterType((*LeaderInfo)(nil), "comm.LeaderInfo")
	proto.RegisterType((*KeyValue)(nil), "comm.KeyValue")
	proto.RegisterEnum("comm.MsgType", MsgType_name, MsgType_value)
	proto.RegisterEnum("comm.PaxosMsgType", PaxosMsgType_name, PaxosMsgType_value)
	proto.RegisterEnum("comm.CheckpointMsgType", CheckpointMsgType_name, CheckpointMsgType_value)
	proto.RegisterEnum("comm.CheckPointMsgFlag", CheckPointMsgFlag_name, CheckPointMsgFlag_value)
	proto.RegisterEnum("comm.Action", Action_name, Action_value)
}

func init() { proto.RegisterFile("paxos.proto", fileDescriptor0) }

var fileDescriptor0 = []byte{
	// 960 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x09, 0x6e, 0x88, 0x02, 0xff, 0x84, 0x55, 0xdb, 0x6e, 0xe3, 0x46,
	0x0c, 0x8d, 0x2e, 0xbe, 0xd1, 0x76, 0x32, 0x3b, 0x0d, 0xb6, 0xc2, 0xa2, 0x28, 0x5c, 0x23, 0x08,
	0x8c, 0x6c, 0x11, 0x14, 0xf9, 0x83, 0x5c, 0x36, 0x6d, 0xb0, 0x9b, 0xd4, 0x18, 0x27, 0x8b, 0xbe,
	0xaa, 0x32, 0x6d, 0xab, 0x96, 0x25, 0x77, 0x24, 0x6f, 0xeb, 0xc7, 0xfe, 0x42, 0x7f, 0xb0, 0x40,
	0xbf, 0xa4, 0x20, 0x47, 0xb2, 0x47, 0xde, 0x05, 0xfa, 0x36, 0x3c, 0x24, 0x0f, 0x35, 0x9c, 0x43,
	0x0a, 0xba, 0xeb, 0xf0, 0xcf, 0x2c, 0xbf, 0x5c, 0xeb, 0xac, 0xc8, 0xa4, 0x1f, 0x65, 0xab, 0xd5,
	0x30, 0x82, 0xe6, 0x4f, 0x18, 0x4e, 0x51, 0xcb, 0x6f, 0xa0, 0x73, 0x9b, 0x6c, 0xf2, 0x02, 0xf5,
	0xc3, 0x5d, 0xe0, 0x0c, 0xdc, 0x91, 0xaf, 0xf6, 0x80, 0xfc, 0x0e, 0xfc, 0xe7, 0xed, 0x1a, 0x03,
	0x77, 0xe0, 0x8e, 0x8e, 0xaf, 0xfa, 0x97, 0x94, 0x7c, 0xf9, 0x98, 0xcf, 0x09, 0x54, 0xec, 0x92,
	0x01, 0xb4, 0x3e, 0xa2, 0xce, 0xe3, 0x2c, 0x0d, 0xbc, 0x81, 0x33, 0x6a, 0xa8, 0xca, 0x1c, 0xfe,
	0xed, 0x43, 0x7b, 0x4c, 0xa5, 0x1f, 0xf3, 0xb9, 0x3c, 0x2f, 0x99, 0x1c, 0x66, 0x92, 0x86, 0xa9,
	0xf2, 0x5a, 0x74, 0xdf, 0x02, 0x3c, 0xa4, 0x79, 0x11, 0xa6, 0x11, 0x3e, 0xdc, 0x05, 0xee, 0xc0,
	0x19, 0xf9, 0xca, 0x42, 0xe4, 0x6b, 0x68, 0x3e, 0x65, 0x53, 0xf2, 0x79, 0xec, 0x2b, 0x2d, 0xca,
	0x1b, 0xeb, 0x6c, 0x9d, 0xe5, 0x61, 0xf2, 0x70, 0x17, 0xf8, 0x26, 0x6f, 0x8f, 0xc8, 0x73, 0x38,
	0xae, 0xac, 0x32, 0xbf, 0xc1, 0x31, 0x07, 0xa8, 0x3c, 0x85, 0xc6, 0xc7, 0x30, 0xd9, 0x60, 0xd0,
	0x1c, 0x38, 0xa3, 0x9e, 0x32, 0x86, 0x1c, 0x40, 0x77, 0xac, 0xf1, 0x3a, 0x8a, 0x70, 0x5d, 0x3c,
	0xdc, 0x05, 0x2d, 0x4e, 0xb5, 0x21, 0x39, 0x82, 0x93, 0x9d, 0x59, 0x16, 0x68, 0x73, 0xd4, 0x21,
	0x2c, 0xbf, 0x87, 0x57, 0x0a, 0x7f, 0xc3, 0xa8, 0xb8, 0xd9, 0x8e, 0x75, 0xb6, 0x8a, 0x73, 0x8a,
	0xed, 0x70, 0xec, 0xe7, 0x0e, 0x79, 0x06, 0xfd, 0xdb, 0x8d, 0xb6, 0x5a, 0x02, 0x1c, 0x59, 0x07,
	0xe5, 0x0f, 0xf0, 0xd5, 0x63, 0x9c, 0xde, 0x2e, 0xb2, 0x1c, 0x53, 0x2b, 0xb6, 0xcb, 0xb1, 0x5f,
	0x72, 0xc9, 0x37, 0xd0, 0xbe, 0x5d, 0x60, 0xb4, 0xcc, 0x37, 0xab, 0xa0, 0x37, 0x70, 0x46, 0x7d,
	0xb5, 0xb3, 0xe9, 0x49, 0xaf, 0xa3, 0xe5, 0x7d, 0x12, 0xce, 0x83, 0xfe, 0xc0, 0x19, 0xb5, 0x55,
	0x65, 0x52, 0x1f, 0x2a, 0x71, 0xa4, 0xb3, 0x2c, 0x38, 0xe6, 0x1e, 0xd9, 0x10, 0xbd, 0xc3, 0x07,
	0x56, 0x16, 0x07, 0x9c, 0x70, 0x80, 0x85, 0x0c, 0xff, 0x71, 0xa1, 0xcf, 0x85, 0xd6, 0x59, 0x9c,
	0x16, 0xa4, 0x8c, 0xb7, 0x35, 0x65, 0x7c, 0x6d, 0x94, 0x51, 0x0b, 0xb1, 0xe4, 0xb1, 0x7f, 0x7e,
	0x97, 0xb5, 0x5a, 0x3d, 0xff, 0x5b, 0xf0, 0xf9, 0x7b, 0x49, 0x14, 0x75, 0x92, 0x71, 0x49, 0x42,
	0x6e, 0xc5, 0x41, 0x52, 0x82, 0xff, 0xf2, 0xc2, 0x2a, 0x21, 0x0a, 0x3e, 0x53, 0x3f, 0x26, 0xf8,
	0xfb, 0x06, 0xd3, 0x08, 0x83, 0x06, 0xe3, 0x3b, 0x5b, 0x5e, 0xc1, 0xe9, 0xfe, 0x7b, 0xac, 0xf6,
	0x36, 0xb9, 0xbd, 0x5f, 0xf4, 0xd5, 0xfa, 0xdb, 0x3a, 0xe8, 0xef, 0x1b, 0x68, 0xdf, 0xc7, 0x09,
	0x8e, 0xc3, 0x62, 0xc1, 0x22, 0xe9, 0xa8, 0x9d, 0x4d, 0xdf, 0x36, 0x79, 0x2c, 0x05, 0xd1, 0x57,
	0x7c, 0xa6, 0x4b, 0xff, 0x3c, 0x9b, 0xe5, 0x58, 0xf0, 0xe3, 0x7b, 0xaa, 0xb4, 0x48, 0xab, 0x37,
	0xdb, 0x02, 0x73, 0x7e, 0xe7, 0x9e, 0x32, 0xc6, 0xf0, 0x2f, 0x17, 0xfa, 0x46, 0x70, 0x99, 0x9e,
	0x14, 0x61, 0x71, 0x38, 0x53, 0x66, 0xc8, 0xed, 0x99, 0x32, 0xb3, 0x43, 0x82, 0x9b, 0xee, 0x1a,
	0x6b, 0x21, 0xe5, 0xec, 0xb0, 0xb5, 0x9b, 0x3d, 0xb7, 0x9c, 0x1d, 0x0b, 0x25, 0x1e, 0x53, 0x98,
	0x79, 0x4c, 0x77, 0x2d, 0x84, 0x78, 0x2a, 0x6b, 0x37, 0x83, 0xcc, 0x53, 0x47, 0x49, 0xf3, 0x15,
	0x62, 0xcf, 0x62, 0x1d, 0x3c, 0xe8, 0xb0, 0x6b, 0x77, 0x78, 0xb8, 0x80, 0xe6, 0x23, 0xae, 0x7e,
	0x45, 0x6d, 0x09, 0xc6, 0xa9, 0x09, 0x46, 0x82, 0xff, 0x14, 0xae, 0xcc, 0x66, 0xeb, 0x28, 0x3e,
	0x13, 0x76, 0x3d, 0x9d, 0x6a, 0xbe, 0x5d, 0x47, 0xf1, 0x99, 0xee, 0x34, 0x41, 0xfd, 0x29, 0x8e,
	0xf0, 0x45, 0x27, 0xbc, 0x57, 0x3a, 0xca, 0x42, 0x86, 0xab, 0xda, 0x44, 0xfc, 0xcf, 0x3a, 0x3d,
	0x87, 0x96, 0xf9, 0xac, 0x3c, 0x70, 0x07, 0xde, 0xa8, 0x7b, 0xd5, 0x2b, 0x37, 0x2a, 0x83, 0xaa,
	0x72, 0xd6, 0x77, 0x2a, 0x71, 0xec, 0x76, 0xea, 0x27, 0x7b, 0xbc, 0xe4, 0x19, 0x34, 0x8d, 0xc5,
	0xa5, 0x0e, 0xe9, 0x4a, 0x9f, 0xcd, 0xe6, 0xd6, 0xd8, 0x68, 0x69, 0xbd, 0x4b, 0x30, 0x2a, 0xe2,
	0x2c, 0x7d, 0x8e, 0x57, 0x98, 0x6d, 0x0a, 0xae, 0xe7, 0xa9, 0x43, 0x78, 0xf8, 0x0b, 0xb4, 0xdf,
	0xe3, 0xd6, 0x34, 0xfe, 0x0c, 0x9a, 0xd7, 0xec, 0x2c, 0x47, 0xb6, 0xac, 0x6a, 0x30, 0x55, 0xfa,
	0xa4, 0x00, 0xef, 0x3d, 0x6e, 0xb9, 0x62, 0x4f, 0xd1, 0x71, 0xbf, 0x5a, 0x3d, 0x6b, 0xb5, 0x5e,
	0x9c, 0x41, 0xab, 0x1c, 0x71, 0xd9, 0x81, 0x06, 0xff, 0x11, 0xc4, 0x91, 0x3c, 0x06, 0xd8, 0x8f,
	0x95, 0x70, 0x2e, 0xfe, 0x75, 0xa0, 0x67, 0xff, 0x2d, 0x64, 0x0f, 0xda, 0x4f, 0xf8, 0x07, 0x53,
	0x08, 0x47, 0x0a, 0xe8, 0x8d, 0x35, 0xae, 0x43, 0x8d, 0x0a, 0xd7, 0xc9, 0x56, 0xb8, 0xf2, 0x04,
	0xba, 0x46, 0x2e, 0x06, 0xf0, 0x64, 0x17, 0x5a, 0x65, 0x88, 0xf0, 0x25, 0xd0, 0x15, 0xc8, 0x2b,
	0x1a, 0xf2, 0x14, 0x44, 0xf5, 0x0f, 0xb8, 0x8f, 0xd3, 0x38, 0x5f, 0xe0, 0x54, 0x34, 0x39, 0x3f,
	0x5f, 0xde, 0x67, 0xfa, 0x03, 0x86, 0x3a, 0x15, 0x2d, 0xf9, 0x1a, 0xe4, 0x6d, 0x96, 0xce, 0x62,
	0xbd, 0xb2, 0xf1, 0xb6, 0xec, 0x43, 0x67, 0x82, 0xa9, 0xd1, 0xa4, 0xe8, 0xd0, 0x97, 0x5c, 0x47,
	0xcb, 0x3d, 0x02, 0x52, 0xc2, 0x31, 0x99, 0xfb, 0x79, 0x13, 0x5d, 0xaa, 0x69, 0x58, 0xac, 0x4b,
	0xf6, 0x2e, 0xae, 0xe0, 0xd5, 0x67, 0x7b, 0x8f, 0x2e, 0x4a, 0xe9, 0xb4, 0x20, 0xc4, 0x91, 0xb9,
	0xd6, 0x72, 0x07, 0x38, 0x17, 0xcf, 0x65, 0x8e, 0xbd, 0xe6, 0xa8, 0x91, 0x37, 0x38, 0x8f, 0x53,
	0x93, 0x30, 0xd6, 0xd9, 0x5c, 0x63, 0x9e, 0xc7, 0xe9, 0x5c, 0x38, 0xb2, 0x05, 0xde, 0xbb, 0x74,
	0x2a, 0x5c, 0x6a, 0xf1, 0x64, 0x13, 0x45, 0x98, 0xe7, 0xb3, 0x4d, 0x22, 0x3c, 0xea, 0xc9, 0x7d,
	0x18, 0x27, 0x38, 0x15, 0xfe, 0xc5, 0x55, 0xf5, 0xc4, 0x14, 0x3e, 0xc1, 0x42, 0x1c, 0xd1, 0xe1,
	0x47, 0x2c, 0x84, 0x43, 0x71, 0x2f, 0xeb, 0x69, 0x58, 0xa0, 0x70, 0xe9, 0x7c, 0x87, 0x09, 0x16,
	0x28, 0xbc, 0xff, 0x02, 0x00, 0x00, 0xff, 0xff, 0xdd, 0xa1, 0xb4, 0xbc, 0x67, 0x08, 0x00, 0x00,
}