package main

import (
	"clusterpulse/cluster"
	"log"
	"os"
	"time"
)

func main() {
	logger := log.New(
		os.Stdout,
		"[clusterpulse] ",
		log.LstdFlags,
	)

	cfg := cluster.Config{
		NodeCount:     5,
		ProbeInterval: 750 * time.Millisecond,
		AckTimeout:    350 * time.Millisecond,
	}

	c, err := cluster.New(cfg, logger)
	if err != nil {
		log.Fatal(err)
	}

	if err := c.Start(); err != nil {
		log.Fatal(err)
	}

	time.Sleep(5 * time.Second)
	c.Stop()

	logger.Println("simulation finished")
}
