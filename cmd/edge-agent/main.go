package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/twojanazwa/aegis-edge/internal/agent"
	"github.com/twojanazwa/aegis-edge/internal/agent/hal"
	"github.com/twojanazwa/aegis-edge/internal/agent/storage"
	"github.com/twojanazwa/aegis-edge/internal/agent/transport"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	buffer, err := storage.NewBoltBuffer("edge_storage.db")
	if err != nil {
		log.Fatalf("Failed to initialize BoltDB buffer: %v", err)
	}
	defer buffer.Close()

	cfg := agent.Config{
		Target:          "localhost:50051",
		DeviceID:        "cam-raspi-01",
		DefaultInterval: 10 * time.Second,
		InitialBackoff:  1 * time.Second,
		MaxBackoff:      30 * time.Second,
		ReconnectDelay:  3 * time.Second,
	}

	grpcClient, err := transport.NewClient(cfg.Target)
	if err != nil {
		log.Fatalf("Failed to create gRPC client: %v", err)
	}
	defer grpcClient.Close()

	runner := agent.NewRunner(cfg, hal.NewSampler(), buffer, grpcClient)
	runner.Run(ctx)
}
