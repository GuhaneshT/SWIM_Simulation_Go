package node

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"time"

	"clusterpulse/membership"
	"clusterpulse/protocol"
)

type Config struct {
	ID            string
	ProbeInterval time.Duration
	AckTimeout    time.Duration
	KnownNodes    []string
}

type Node struct {
	id            string
	peers         []string
	probeInterval time.Duration
	ackTimeout    time.Duration
	rng           *rand.Rand
	table         *membership.Table
	inbox         chan protocol.Message
	outbox        chan protocol.Message
}

func New(cfg Config) *Node {
	if cfg.ProbeInterval <= 0 {
		cfg.ProbeInterval = 750 * time.Millisecond
	}

	if cfg.AckTimeout <= 0 {
		cfg.AckTimeout = 350 * time.Millisecond
	}

	peers := make([]string, 0, len(cfg.KnownNodes))
	seen := make(map[string]struct{}, len(cfg.KnownNodes))

	for _, nodeID := range cfg.KnownNodes {
		if nodeID == "" || nodeID == cfg.ID {
			continue
		}

		if _, exists := seen[nodeID]; exists {
			continue
		}

		seen[nodeID] = struct{}{}
		peers = append(peers, nodeID)
	}

	sort.Strings(peers)

	bufferSize := len(peers)*4+8
	if bufferSize < 16 {
		bufferSize = 16
	}

	return &Node{
		id:            cfg.ID,
		peers:         peers,
		probeInterval: cfg.ProbeInterval,
		ackTimeout:    cfg.AckTimeout,
		rng: rand.New(
			rand.NewSource(
				time.Now().UnixNano() + int64(len(cfg.ID))*7919,
			),
		),
		inbox:  make(chan protocol.Message, bufferSize),
		outbox: make(chan protocol.Message, bufferSize),
		table:  membership.NewTable(cfg.ID, cfg.KnownNodes),
	}
}

func (n *Node) ID() string {
	return n.id
}

func (n *Node) Inbox() chan protocol.Message {
	return n.inbox
}

func (n *Node) Outbox() <-chan protocol.Message {
	return n.outbox
}

func (n *Node) Run(ctx context.Context, logger *log.Logger) {
	ticker := time.NewTicker(n.probeInterval)
	defer ticker.Stop()

	logf(logger, "[%s] started", n.id)

	for {
		select {
		case <-ctx.Done():
			logf(logger, "[%s] stopped", n.id)
			return
		case <-ticker.C:
			n.probeRandomPeer(ctx, logger)
		case msg, ok := <-n.inbox:
			if !ok {
				logf(logger, "[%s] inbox closed", n.id)
				return
			}
			n.handleMessage(ctx, msg, logger)
		}
	}
}

func (n *Node) probeRandomPeer(ctx context.Context, logger *log.Logger) {
	if len(n.peers) == 0 {
		return
	}

	peer := n.peers[n.rng.Intn(len(n.peers))]
	msg := protocol.Message{
		Type:          protocol.MessagePing,
		From:          n.id,
		To:            peer,
		CorrelationID: n.newCorrelationID(),
		SentAt:        time.Now(),
	}

	logf(logger, "[%s] probing %s (%s)", n.id, peer, msg.CorrelationID)
	n.send(ctx, msg)
}

func (n *Node) handleMessage(ctx context.Context, msg protocol.Message, logger *log.Logger) {
	switch msg.Type {
	case protocol.MessagePing:
		logf(logger, "[%s] received PING from %s (%s)", n.id, msg.From, msg.CorrelationID)
		n.sendAck(ctx, msg, logger)
	case protocol.MessageAck:
		logf(logger, "[%s] received ACK from %s (%s)", n.id, msg.From, msg.CorrelationID)
	default:
		logf(logger, "[%s] ignored %s from %s (%s)", n.id, msg.Type, msg.From, msg.CorrelationID)
	}
}

func (n *Node) sendAck(ctx context.Context, ping protocol.Message, logger *log.Logger) {
	ack := protocol.Message{
		Type:          protocol.MessageAck,
		From:          n.id,
		To:            ping.From,
		CorrelationID: ping.CorrelationID,
		SentAt:        time.Now(),
	}

	logf(logger, "[%s] acknowledging %s (%s)", n.id, ping.From, ping.CorrelationID)
	n.send(ctx, ack)
}

func (n *Node) send(ctx context.Context, msg protocol.Message) bool {
	select {
	case <-ctx.Done():
		return false
	case n.outbox <- msg:
		return true
	}
}

func (n *Node) newCorrelationID() string {
	return fmt.Sprintf("%s-%d-%d", n.id, time.Now().UnixNano(), n.rng.Int63())
}

func logf(logger *log.Logger, format string, args ...any) {
	if logger == nil {
		return
	}

	logger.Printf(format, args...)
}
