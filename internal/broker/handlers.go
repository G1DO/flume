package broker

import (
	"github.com/G1DO/flume/internal/protocol"
	"github.com/G1DO/flume/internal/storage"
)

// handleProduce processes a PRODUCE request.
// Stores the message and returns the assigned offset.
func (b *Broker) handleProduce(requestID int32, payload []byte) []byte {
	// Decode request
	req, err := protocol.DecodeProduceRequest(payload)
	if err != nil {
		return b.produceResponse(requestID, -1, protocol.ErrUnknown)
	}

	// Get or create log for topic
	log, err := b.getOrCreateLog(req.Topic)
	if err != nil {
		return b.produceResponse(requestID, -1, protocol.ErrUnknown)
	}

	// Create record and append
	record := storage.NewRecord(req.Payload)
	offset, err := log.Append(record)
	if err != nil {
		return b.produceResponse(requestID, -1, protocol.ErrUnknown)
	}

	return b.produceResponse(requestID, offset, protocol.ErrNone)
}

// produceResponse builds a PRODUCE response.
func (b *Broker) produceResponse(requestID int32, offset int64, errorCode int16) []byte {
	resp := protocol.EncodeProduceResponse(&protocol.ProduceResponse{
		Offset:    offset,
		ErrorCode: errorCode,
	})
	header := protocol.EncodeResponseHeader(&protocol.ResponseHeader{
		Size:      int32(4 + len(resp)), // RequestID(4) + payload
		RequestID: requestID,
	})
	return append(header, resp...)
}

// handleFetch processes a FETCH request.
// Returns messages starting from the requested offset.
func (b *Broker) handleFetch(requestID int32, payload []byte) []byte {
	// Decode request
	req, err := protocol.DecodeFetchRequest(payload)
	if err != nil {
		return b.fetchResponse(requestID, protocol.ErrUnknown, nil)
	}

	// Get log for topic
	b.logsMu.RLock()
	log, exists := b.logs[req.Topic]
	b.logsMu.RUnlock()
	if !exists {
		return b.fetchResponse(requestID, protocol.ErrTopicNotFound, nil)
	}

	// Read records starting from offset
	var records []protocol.FetchRecord
	var bytesRead int32 = 0
	offset := req.Offset

	for {
		// Check max bytes limit
		if req.MaxBytes > 0 && bytesRead >= req.MaxBytes {
			break
		}

		// Try to read next record
		record, err := log.Read(offset)
		if err != nil {
			break // No more records or error
		}

		// Add to response
		records = append(records, protocol.FetchRecord{
			Offset:  record.Offset,
			Payload: record.Payload,
		})

		bytesRead += int32(8 + 4 + len(record.Payload)) // Offset + PayloadLen + Payload
		offset++
	}

	// If no records and offset is out of range, return error
	if len(records) == 0 && req.Offset >= log.NewestOffset() {
		return b.fetchResponse(requestID, protocol.ErrOffsetOutOfRange, nil)
	}

	return b.fetchResponse(requestID, protocol.ErrNone, records)
}

// fetchResponse builds a FETCH response.
func (b *Broker) fetchResponse(requestID int32, errorCode int16, records []protocol.FetchRecord) []byte {
	resp := protocol.EncodeFetchResponse(&protocol.FetchResponse{
		ErrorCode: errorCode,
		Records:   records,
	})
	header := protocol.EncodeResponseHeader(&protocol.ResponseHeader{
		Size:      int32(4 + len(resp)), // RequestID(4) + payload
		RequestID: requestID,
	})
	return append(header, resp...)
}
