package protocol

import (
	"time"
)

type MemberStatus string

const(
	StatusAlive MemberStatus = "alive"
	StatusSuspect MemberStatus = "suspect"
	StatusFailed MemberStatus = "failed"
	StatusLeft MemberStatus = "left"
)

type MessageType string

const(
	MessagePing MessageType = "PING"
	MessageAck MessageType = "ACK"
	MessagePingReq MessageType = "PING-REQ"
	MessageAlive MessageType = "ALIVE"
	MessageSuspect MessageType = "SUSPECT"
	MessageConfirm MessageType = "CONFIRM"
	MessageDead MessageType = "DEAD"
)

type Update struct{
	NodeID string
	Status MemberStatus
	Incarnation int64
	ObservedAt time.Time
	ObservedBy string
}

type Message struct{
	Type MessageType
	From string
	To string
	Target string
	CorrelationID string
	SentAt time.Time
	Updates []Update
}