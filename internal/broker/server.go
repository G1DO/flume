package broker

import (
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/G1DO/flume/internal/protocol"
	"github.com/G1DO/flume/internal/topic"
)

// BrokerConfig holds broker configuration.
type BrokerConfig struct {
	Port          int
	DataDir       string
	NumPartitions int // default partitions for new topics
}

// Broker is the main server that handles client connections.
type Broker struct {
	config   BrokerConfig
	listener net.Listener
	topics   *topic.Manager
	wg       sync.WaitGroup  // tracks active connections
	quit     chan struct{}   // signals shutdown
}

// NewBroker creates a new broker instance.
func NewBroker(config BrokerConfig) *Broker {
	if config.NumPartitions <= 0 {
		config.NumPartitions = 3 // default
	}

	topicConfig := topic.TopicConfig{
		NumPartitions: config.NumPartitions,
	}

	return &Broker{
		config: config,
		topics: topic.NewManager(config.DataDir, topicConfig),
		quit:   make(chan struct{}),
	}
}

// Start begins listening for connections.
func (b *Broker) Start() error {
	// Load existing topics from disk
	if err := b.topics.LoadExisting(); err != nil {
		return fmt.Errorf("failed to load topics: %w", err)
	}

	addr := fmt.Sprintf(":%d", b.config.Port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}
	b.listener = listener

	go b.acceptLoop()
	return nil
}

// acceptLoop accepts incoming connections.
func (b *Broker) acceptLoop() {
	for {
		conn, err := b.listener.Accept()
		if err != nil {
			select {
			case <-b.quit:
				return
			default:
				continue
			}
		}
		b.wg.Add(1)
		go b.handleConnection(conn)
	}
}

// handleConnection processes requests from a single client.
func (b *Broker) handleConnection(conn net.Conn) {
	defer b.wg.Done()
	defer conn.Close()

	for {
		select {
		case <-b.quit:
			return
		default:
		}

		// Read request header
		header, err := protocol.DecodeRequestHeader(conn)
		if err != nil {
			if err == io.EOF {
				return
			}
			return
		}

		// Read payload (Size - APIKey(2) - RequestID(4))
		payloadSize := header.Size - 6
		payload := make([]byte, payloadSize)
		if _, err := io.ReadFull(conn, payload); err != nil {
			return
		}

		// Route to handler
		var response []byte
		switch header.APIKey {
		case protocol.APIKeyProduce:
			response = b.handleProduce(header.RequestID, payload)
		case protocol.APIKeyFetch:
			response = b.handleFetch(header.RequestID, payload)
		default:
			response = b.errorResponse(header.RequestID, protocol.ErrUnknown)
		}

		if _, err := conn.Write(response); err != nil {
			return
		}
	}
}

// errorResponse creates an error response.
func (b *Broker) errorResponse(requestID int32, errorCode int16) []byte {
	resp := protocol.EncodeProduceResponse(&protocol.ProduceResponse{
		Partition: -1,
		Offset:    -1,
		ErrorCode: errorCode,
	})
	header := protocol.EncodeResponseHeader(&protocol.ResponseHeader{
		Size:      int32(4 + len(resp)),
		RequestID: requestID,
	})
	return append(header, resp...)
}

// Stop shuts down the broker.
func (b *Broker) Stop() error {
	close(b.quit)
	if b.listener != nil {
		b.listener.Close()
	}
	b.wg.Wait()
	return b.topics.Close()
}

// Addr returns the listener address.
func (b *Broker) Addr() net.Addr {
	if b.listener == nil {
		return nil
	}
	return b.listener.Addr()
}
