package storage

import "errors"

var (
	ErrInvalidRecord = errors.New("invalid record: data too short or corrupted")
)
