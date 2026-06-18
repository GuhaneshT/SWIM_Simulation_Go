package node

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"sync"
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
	pendingMu     sync.Mutex
	pendingAcks   map[string]string
	suspectMu     sync.Mutex
	suspicions    map[string]struct{}
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

	bufferSize := len(peers)*4 + 8
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
		inbox:       make(chan protocol.Message, bufferSize),
		outbox:      make(chan protocol.Message, bufferSize),
		table:       membership.NewTable(cfg.ID, cfg.KnownNodes),
		pendingAcks: make(map[string]string),
		suspicions:  make(map[string]struct{}),
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

func (n *Node) MemberStatus(nodeID string) (protocol.MemberStatus, bool) {
	record, exists := n.table.Get(nodeID)
	if !exists {
		return "", false
	}
	return record.Status, true
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

func (n *Node) handleSuspectQuery(ctx context.Context, helperID string, requesterID string, correlationID string) {

	msg := protocol.Message{
		Type:          protocol.MessageAlive,
		From:          n.id,
		To:            helperID,
		CorrelationID: correlationID,
		Target:        n.id,
		Requester:     requesterID,
		SentAt:        time.Now(),
		Updates:       n.table.Updates(),
	}
	n.send(ctx, msg)
}

func (n *Node) probeSuspectedNode(ctx context.Context, suspectID string, requesterID string, correlationID string) {
	msg := protocol.Message{
		Type:          protocol.MessageSuspect,
		From:          n.id,
		To:            suspectID,
		CorrelationID: correlationID,
		Target:        suspectID,
		Requester:     requesterID,
		SentAt:        time.Now(),
		Updates:       n.table.Updates(),
	}
	n.send(ctx, msg)
}

func (n *Node) probeRandomPeer(ctx context.Context, logger *log.Logger) {
	peer, ok := n.randomProbePeer()
	if !ok {
		return
	}

	piggybackUpdates := n.table.Updates()
	msg := protocol.Message{
		Type:          protocol.MessagePing,
		From:          n.id,
		To:            peer,
		CorrelationID: n.newCorrelationID(),
		SentAt:        time.Now(),
		Updates:       piggybackUpdates,
	}
	logf(logger, "[%s] probing %s (%s)", n.id, peer, msg.CorrelationID)
	n.trackPendingAck(msg.CorrelationID, peer)
	if !n.send(ctx, msg) {
		n.clearPendingAck(msg.CorrelationID)
		return
	}
	n.watchAckTimeout(ctx, msg.CorrelationID, logger)
}

func (n *Node) randomProbePeer() (string, bool) {
	eligiblePeers := make([]string, 0, len(n.peers))
	for _, peer := range n.peers {
		record, exists := n.table.Get(peer)
		if exists && (record.Status == protocol.StatusFailed || record.Status == protocol.StatusLeft) {
			continue
		}
		eligiblePeers = append(eligiblePeers, peer)
	}

	if len(eligiblePeers) == 0 {
		return "", false
	}

	return eligiblePeers[n.rng.Intn(len(eligiblePeers))], true
}

func (n *Node) handleMessage(ctx context.Context, msg protocol.Message, logger *log.Logger) {
	n.handleUpdates(msg.Updates, msg.From)

	switch msg.Type {
	case protocol.MessagePing:
		// sender is alive, mark in table and do nothing . data will get populated when it randomly pings someother node

		logf(logger, "[%s] received PING from %s (%s)", n.id, msg.From, msg.CorrelationID)
		n.table.MarkAlive(msg.From, msg.SentAt)
		n.sendAck(ctx, msg, logger)

	case protocol.MessageAck:
		logf(logger, "[%s] received ACK from %s (%s)", n.id, msg.From, msg.CorrelationID)
		n.clearPendingAck(msg.CorrelationID)
		n.clearSuspicion(msg.From)
		n.table.MarkAlive(msg.From, msg.SentAt)
	case protocol.MessagePingReq:
		logf(logger, "[%s] received PING-REQ from %s to ping %s (%s)", n.id, msg.From, msg.Target, msg.CorrelationID)
		n.probeSuspectedNode(ctx, msg.Target, msg.Requester, msg.CorrelationID)
	case protocol.MessageSuspect:
		logf(logger, "[%s] received SUSPECT from %s about %s (%s)", n.id, msg.From, msg.Target, msg.CorrelationID)
		// tell them, as you can see i am not dead yet, wakanda forever
		n.handleSuspectQuery(ctx, msg.From, msg.Requester, msg.CorrelationID)

	case protocol.MessageAlive:
		logf(logger, "[%s] received ALIVE from %s (%s)", n.id, msg.From, msg.CorrelationID)

		n.handleAlive(ctx, msg, logger)

	case protocol.MessageDead:
		//to be handled
		logf(logger, "[%s] ignored %s from %s (%s)", n.id, msg.Type, msg.From, msg.CorrelationID)

	default:
		// to be done, handle other message types
	}
}

func (n *Node) trackPendingAck(correlationID string, peer string) {
	n.pendingMu.Lock()
	defer n.pendingMu.Unlock()
	n.pendingAcks[correlationID] = peer
}

func (n *Node) clearPendingAck(correlationID string) {
	n.pendingMu.Lock()
	defer n.pendingMu.Unlock()
	delete(n.pendingAcks, correlationID)
}

func (n *Node) consumePendingAck(correlationID string) (string, bool) {
	n.pendingMu.Lock()
	defer n.pendingMu.Unlock()

	peer, exists := n.pendingAcks[correlationID]
	if !exists {
		return "", false
	}

	delete(n.pendingAcks, correlationID)
	return peer, true
}

func (n *Node) trackSuspicion(nodeID string) {
	n.suspectMu.Lock()
	defer n.suspectMu.Unlock()
	n.suspicions[nodeID] = struct{}{}
}

func (n *Node) clearSuspicion(nodeID string) {
	n.suspectMu.Lock()
	defer n.suspectMu.Unlock()
	delete(n.suspicions, nodeID)
}

func (n *Node) consumeSuspicion(nodeID string) bool {
	n.suspectMu.Lock()
	defer n.suspectMu.Unlock()

	if _, exists := n.suspicions[nodeID]; !exists {
		return false
	}

	delete(n.suspicions, nodeID)
	return true
}

func (n *Node) watchAckTimeout(ctx context.Context, correlationID string, logger *log.Logger) {
	go func() {
		timer := time.NewTimer(n.ackTimeout)
		defer timer.Stop()

		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			peer, exists := n.consumePendingAck(correlationID)
			if !exists {
				return
			}

			logf(logger, "[%s] ACK timeout from %s (%s)", n.id, peer, correlationID)
			n.table.MarkSuspect(peer, time.Now())
			n.trackSuspicion(peer)
			n.handleSuspect(peer, ctx, logger)
			n.watchSuspectTimeout(ctx, peer, logger)
		}
	}()
}

func (n *Node) watchSuspectTimeout(ctx context.Context, nodeID string, logger *log.Logger) {
	go func() {
		timer := time.NewTimer(3 * n.ackTimeout)
		defer timer.Stop()

		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			if !n.consumeSuspicion(nodeID) {
				return
			}

			logf(logger, "[%s] suspect timeout for %s; marking failed", n.id, nodeID)
			n.table.MarkFailed(nodeID, time.Now())
		}
	}()
}

func (n *Node) handleAlive(ctx context.Context, msg protocol.Message, logger *log.Logger) {
	if msg.Requester == "" {
		logf(logger, "[%s] ALIVE from %s has empty requester (%s)", n.id, msg.From, msg.CorrelationID)
		return
	}
	if msg.Target != "" {
		n.clearSuspicion(msg.Target)
		n.table.MarkAlive(msg.Target, time.Now())
	}

	if msg.Requester == n.id {

		return
	}

	send_msg := protocol.Message{
		Type:          protocol.MessageAlive,
		From:          n.id,
		To:            msg.Requester,
		CorrelationID: msg.CorrelationID,
		Target:        msg.Target,
		Requester:     msg.Requester,
		SentAt:        time.Now(),
		Updates:       n.table.Updates(),
	}
	n.send(ctx, send_msg)

}
func (n *Node) handleSuspect(suspectID string, ctx context.Context, logger *log.Logger) {
	randomPeers := n.selectRandomPeers(3, suspectID)
	if randomPeers == nil {
		logf(logger, "[%s] no peers available to ping %s", n.id, suspectID)
	}
	for _, peer := range randomPeers {
		msg := protocol.Message{
			Type:          protocol.MessagePingReq,
			From:          n.id,
			To:            peer,
			Target:        suspectID,
			Requester:     n.id,
			CorrelationID: n.newCorrelationID(),
			Updates:       n.table.Updates(),
		}
		n.send(ctx, msg)
	}

}

func (n *Node) selectRandomPeers(nPeers int, excludeID string) []string {
	peers := make([]string, 0, len(n.peers))
	for _, peer := range n.peers {
		if peer != excludeID {
			peers = append(peers, peer)
		}
	}

	if len(peers) == 0 {
		return nil
	}

	if nPeers > len(peers) {
		nPeers = len(peers)
	}

	for i := 0; i < nPeers; i++ {
		j := n.rng.Intn(len(peers))
		peers[i], peers[j] = peers[j], peers[i]
	}

	return peers[:nPeers]
}

func (n *Node) handleUpdates(updates []protocol.Update, observedBy string) {
	if len(updates) == 0 {
		return
	}
	fmt.Println("merging updates for node ", n.id, " observed by ", observedBy)
	for _, update := range updates {
		n.table.Merge(update)
	}
}

func (n *Node) sendAck(ctx context.Context, ping protocol.Message, logger *log.Logger) {
	ack := protocol.Message{
		Type:          protocol.MessageAck,
		From:          n.id,
		To:            ping.From,
		CorrelationID: ping.CorrelationID,
		SentAt:        time.Now(),
		Updates:       n.table.Updates(),
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
