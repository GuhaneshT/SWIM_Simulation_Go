package main

import (
	"clusterpulse/cluster"
	"clusterpulse/simlog"
	"flag"
	"log"
	"os"
	"time"
)

func main() {
	nodeCount := flag.Int("nodes", 5, "number of nodes")
	target := flag.String("target", "node-3", "node to fail/recover")
	scenario := flag.String("scenario", "flapping-node", "scenario to run: healthy, dead-node, flapping-node")
	duration := flag.Duration("duration", 10*time.Second, "duration to run after scenario actions")
	probeInterval := flag.Duration("probe-interval", 750*time.Millisecond, "probe interval")
	ackTimeout := flag.Duration("ack-timeout", 350*time.Millisecond, "ack timeout")
	debug := flag.Bool("debug", false, "enable debug logs")

	flag.Parse()
	logger := log.New(
		os.Stdout,
		"[clusterpulse] ",
		log.LstdFlags,
	)
	logLevel := simlog.Info
	if *debug {
		logLevel = simlog.Debug
	}
	cfg := cluster.Config{
		NodeCount:     *nodeCount,
		ProbeInterval: *probeInterval,
		AckTimeout:    *ackTimeout,
		LogLevel:      logLevel,
	}

	c, err := cluster.New(cfg, logger)
	if err != nil {
		log.Fatal(err)
	}

	if err := c.Start(); err != nil {
		log.Fatal(err)
	}

	switch *scenario {
	case "healthy":
		runHealthyScenario(c, logger, logLevel, *duration)
	case "dead-node":
		runDeadNodeScenario(c, logger, logLevel, *target, *duration)
	case "flapping-node":
		runFlappingNodeScenario(c, logger, logLevel, *target, *duration)
	default:
		log.Fatalf("unknown scenario: %s", *scenario)
	}

	c.Stop()
}

func runHealthyScenario(c *cluster.Cluster, logger *log.Logger, logLevel simlog.Level, duration time.Duration) {
	simlog.Logf(logger, logLevel, simlog.Info, "scenario", "start name=healthy duration=%s", duration)
	time.Sleep(duration)
	simlog.Logf(logger, logLevel, simlog.Info, "scenario", "finish name=healthy duration=%s", duration)
}

func runDeadNodeScenario(c *cluster.Cluster, logger *log.Logger, logLevel simlog.Level, target string, duration time.Duration) {
	simlog.Logf(logger, logLevel, simlog.Info, "scenario", "start name=dead-node target=%s duration=%s", target, duration)
	time.Sleep(3 * time.Second)
	c.FailNode(target)
	time.Sleep(duration)
	simlog.Logf(logger, logLevel, simlog.Info, "scenario", "finish name=dead-node target=%s duration=%s", target, duration)
}

func runFlappingNodeScenario(c *cluster.Cluster, logger *log.Logger, logLevel simlog.Level, target string, duration time.Duration) {
	simlog.Logf(logger, logLevel, simlog.Info, "scenario", "start name=flapping-node target=%s duration=%s", target, duration)
	time.Sleep(3 * time.Second)
	c.FailNode(target)

	time.Sleep(700 * time.Millisecond)
	c.RecoverNode(target)
	if err := c.InjectSuspect("node-1", target, "node-1"); err != nil {
		log.Fatal(err)
	}

	time.Sleep(duration)
	simlog.Logf(logger, logLevel, simlog.Info, "scenario", "finish name=flapping-node target=%s duration=%s", target, duration)
}
