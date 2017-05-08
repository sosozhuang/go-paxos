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