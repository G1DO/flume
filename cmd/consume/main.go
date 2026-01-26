package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/G1DO/flume/pkg/client"
)

func main() {
	broker := flag.String("broker", "localhost:9092", "Broker address")
	topic := flag.String("topic", "", "Topic to consume from")
	partition := flag.Int("partition", 0, "Partition to consume from")
	offset := flag.Int64("offset", 0, "Starting offset")
	maxBytes := flag.Int("max-bytes", 65536, "Max bytes to fetch")
	flag.Parse()

	if *topic == "" {
		fmt.Fprintln(os.Stderr, "Error: --topic is required")
		os.Exit(1)
	}

	consumer, err := client.NewConsumer(*broker, *topic, int32(*partition), *offset)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect: %v\n", err)
		os.Exit(1)
	}
	defer consumer.Close()

	messages, err := consumer.Fetch(int32(*maxBytes))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to fetch: %v\n", err)
		os.Exit(1)
	}

	if len(messages) == 0 {
		fmt.Println("No messages")
		return
	}

	for _, msg := range messages {
		fmt.Printf("[%d] %s\n", msg.Offset, string(msg.Payload))
	}
}
