package cluster

import (
	"context"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"clusterpulse/node"
	"clusterpulse/protocol"
	"clusterpulse/simlog"
)

type Config struct {
	NodeCount     int
	ProbeInterval time.Duration
	AckTimeout    time.Duration
	BaseLatency   time.Duration
	LatencyJitter time.Duration
	DropRate      float64
	LogLevel      simlog.Level
}

type Cluster struct {
	mu          sync.Mutex
	wg          sync.WaitGroup
	log         *log.Logger
	ids         []string
	nodes       map[string]*node.Node
	ctx         context.Context
	cancel      context.CancelFunc
	failedNodes map[string]bool
	logLevel    simlog.Level
	started     bool
	stopped     bool
}

func New(cfg Config, logger *log.Logger) (*Cluster, error) {
	if cfg.NodeCount <= 0 {
		return nil, fmt.Errorf("NodeCount must be greater than 0")
	}
	failedNodes := make(map[string]bool, cfg.NodeCount)
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}
	ids := make([]string, cfg.NodeCount)

	for i := 0; i < cfg.NodeCount; i++ {
		ids[i] = fmt.Sprintf("node-%d", i+1)
	}

	nodes := make(map[string]*node.Node, len(ids))

	for _, nodeID := range ids {
		nodes[nodeID] = node.New(node.Config{
			ID:            nodeID,
			ProbeInterval: cfg.ProbeInterval,
			AckTimeout:    cfg.AckTimeout,
			KnownNodes:    ids,
			LogLevel:      cfg.LogLevel,
		})
	}

	return &Cluster{
		ids:         ids,
		nodes:       nodes,
		log:         logger,
		failedNodes: failedNodes,
		logLevel:    cfg.LogLevel,
	}, nil
}

func (c *Cluster) Start() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.started && !c.stopped {
		return nil
	}

	if c.stopped {
		return fmt.Errorf("cluster has already been stopped")
	}

	c.ctx, c.cancel = context.WithCancel(context.Background())
	c.started = true

	for _, n := range c.nodes {
		current := n

		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			current.Run(c.ctx, c.log)
		}()

		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			c.routeMessages(c.ctx, current)
		}()
	}

	c.logf(simlog.Info, "cluster", "started nodes=%d", len(c.nodes))
	return nil
}

func (c *Cluster) Stop() {
	c.mu.Lock()
	if !c.started || c.stopped {
		c.mu.Unlock()
		return
	}

	cancel := c.cancel
	c.stopped = true
	c.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	c.wg.Wait()
	c.logMembershipStatus("node-3")
	c.logf(simlog.Info, "cluster", "stopped")
}

func (c *Cluster) logMembershipStatus(nodeID string) {
	for _, observerID := range c.ids {
		observer := c.nodes[observerID]
		status, exists := observer.MemberStatus(nodeID)
		if !exists {
			c.logf(simlog.Info, "membership", "observer=%s subject=%s status=missing", observerID, nodeID)
			continue
		}
		c.logf(simlog.Info, "membership", "observer=%s subject=%s status=%s", observerID, nodeID, status)
	}
}

func (c *Cluster) routeMessages(ctx context.Context, source *node.Node) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-source.Outbox():
			if !ok {
				return
			}
			c.deliver(ctx, msg)
		}
	}
}

func (c *Cluster) deliver(ctx context.Context, msg protocol.Message) {

	if c.isFailed(msg.To) || c.isFailed(msg.From) {
		c.logf(simlog.Info, "router", "DROP type=%s from=%s to=%s reason=endpoint_failed cid=%s", msg.Type, msg.From, msg.To, msg.CorrelationID)
		return
	}
	target, exists := c.nodes[msg.To]
	if !exists {
		c.logf(simlog.Info, "router", "DROP type=%s from=%s to=%s reason=unknown_target cid=%s", msg.Type, msg.From, msg.To, msg.CorrelationID)
		return
	}

	c.logf(simlog.Debug, "router", "DELIVER type=%s from=%s to=%s cid=%s", msg.Type, msg.From, msg.To, msg.CorrelationID)

	select {
	case <-ctx.Done():
	case target.Inbox() <- msg:
	}
}

func (c *Cluster) FailNode(nodeID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failedNodes[nodeID] = true
	c.logf(simlog.Info, "scenario", "fail node=%s", nodeID)
}

func (c *Cluster) RecoverNode(nodeID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.failedNodes, nodeID)
	c.logf(simlog.Info, "scenario", "recover node=%s", nodeID)
}

func (c *Cluster) InjectSuspect(fromID string, targetID string, requesterID string) error {
	target, exists := c.nodes[targetID]
	if !exists {
		return fmt.Errorf("unknown target %s", targetID)
	}

	msg := protocol.Message{
		Type:          protocol.MessageSuspect,
		From:          fromID,
		To:            targetID,
		Target:        targetID,
		Requester:     requesterID,
		CorrelationID: fmt.Sprintf("manual-%d", time.Now().UnixNano()),
		SentAt:        time.Now(),
	}

	c.logf(simlog.Info, "scenario", "inject type=SUSPECT from=%s to=%s requester=%s cid=%s", fromID, targetID, requesterID, msg.CorrelationID)
	select {
	case <-c.ctx.Done():
		return fmt.Errorf("cluster stopped")
	case target.Inbox() <- msg:
		return nil
	}
}

func (c *Cluster) isFailed(nodeID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.failedNodes[nodeID]
}

func (c *Cluster) logf(level simlog.Level, component string, format string, args ...any) {
	simlog.Logf(c.log, c.logLevel, level, component, format, args...)
}
