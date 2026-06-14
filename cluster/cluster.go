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
)

type Config struct {
	NodeCount      int
	ProbeInterval  time.Duration
	AckTimeout     time.Duration
	BaseLatency    time.Duration
	LatencyJitter  time.Duration
	DropRate       float64
}

type Cluster struct {
	mu      sync.Mutex
	wg      sync.WaitGroup
	log     *log.Logger
	ids     []string
	nodes   map[string]*node.Node
	ctx     context.Context
	cancel  context.CancelFunc
	started bool
	stopped bool
}

func New(cfg Config, logger *log.Logger) (*Cluster, error) {
	if cfg.NodeCount <= 0 {
		return nil, fmt.Errorf("NodeCount must be greater than 0")
	}

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
		})
	}

	return &Cluster{
		ids:   ids,
		nodes: nodes,
		log:   logger,
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

	c.log.Printf("cluster started with %d nodes", len(c.nodes))
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
	c.log.Printf("cluster stopped")
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
	target, exists := c.nodes[msg.To]
	if !exists {
		c.log.Printf("[router] dropping %s from %s to unknown target %s", msg.Type, msg.From, msg.To)
		return
	}

	c.log.Printf("[router] %s -> %s %s (%s)", msg.From, msg.To, msg.Type, msg.CorrelationID)

	select {
	case <-ctx.Done():
	case target.Inbox() <- msg:
	}
}
