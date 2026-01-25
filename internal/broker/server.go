package broker

import (
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/G1DO/flume/internal/protocol"
	"github.com/G1DO/flume/internal/storage"
)

// BrokerConfig holds broker configuration.
type BrokerConfig struct {
	Port    int
	DataDir string
}

// Broker is the main server that handles client connections.
type Broker struct {
	config   BrokerConfig
	listener net.Listener
	logs     map[string]*storage.Log // topic name → log
	logsMu   sync.RWMutex            // protects logs map
	wg       sync.WaitGroup          // tracks active connections
	quit     chan struct{}           // signals shutdown
}

// NewBroker creates a new broker instance.
func NewBroker(config BrokerConfig) *Broker {
	return &Broker{
		config: config,
		logs:   make(map[string]*storage.Log),
		quit:   make(chan struct{}),
	}
}

// Start begins listening for connections.
func (b *Broker) Start() error {
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

// getOrCreateLog gets or creates a log for the topic.
func (b *Broker) getOrCreateLog(topic string) (*storage.Log, error) {
	b.logsMu.RLock()
	log, exists := b.logs[topic]
	b.logsMu.RUnlock()
	if exists {
		return log, nil
	}

	b.logsMu.Lock()
	defer b.logsMu.Unlock()

	if log, exists := b.logs[topic]; exists {
		return log, nil
	}

	dir := fmt.Sprintf("%s/%s", b.config.DataDir, topic)
	log, err := storage.NewLog(dir, storage.LogConfig{})
	if err != nil {
		return nil, err
	}
	if err := log.Recover(); err != nil {
		log.Close()
		return nil, err
	}

	b.logs[topic] = log
	return log, nil
}

// errorResponse creates an error response.
func (b *Broker) errorResponse(requestID int32, errorCode int16) []byte {
	resp := protocol.EncodeProduceResponse(&protocol.ProduceResponse{
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

	b.logsMu.Lock()
	defer b.logsMu.Unlock()
	for _, log := range b.logs {
		log.Close()
	}
	return nil
}

// Addr returns the listener address.
func (b *Broker) Addr() net.Addr {
	if b.listener == nil {
		return nil
	}
	return b.listener.Addr()
}
