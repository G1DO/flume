package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/G1DO/flume/internal/broker"
)

func main() {
	port := flag.Int("port", 9092, "Port to listen on")
	dataDir := flag.String("data", "./data", "Data directory")
	flag.Parse()

	b := broker.NewBroker(broker.BrokerConfig{
		Port:    *port,
		DataDir: *dataDir,
	})

	if err := b.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to start broker: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Broker listening on port %d\n", *port)

	// Wait for interrupt
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println("\nShutting down...")
	b.Stop()
}
