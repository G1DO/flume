package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/G1DO/flume/pkg/client"
)

func main() {
	broker := flag.String("broker", "localhost:9092", "Broker address")
	topic := flag.String("topic", "", "Topic to produce to")
	message := flag.String("message", "", "Message to send")
	flag.Parse()

	if *topic == "" {
		fmt.Fprintln(os.Stderr, "Error: --topic is required")
		os.Exit(1)
	}
	if *message == "" {
		fmt.Fprintln(os.Stderr, "Error: --message is required")
		os.Exit(1)
	}

	producer, err := client.NewProducer(*broker)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect: %v\n", err)
		os.Exit(1)
	}
	defer producer.Close()

	offset, err := producer.Produce(*topic, []byte(*message))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to produce: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Produced to %s at offset %d\n", *topic, offset)
}
