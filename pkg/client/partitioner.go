package client

import (
	"hash/fnv"
)

// Partitioner determines which partition a message should go to.
type Partitioner interface {
	Partition(key []byte, numPartitions int) int
}

// HashPartitioner uses FNV-1a hash to determine partition.
// Same key always maps to the same partition.
type HashPartitioner struct{}

// Partition returns the partition for the given key.
func (p *HashPartitioner) Partition(key []byte, numPartitions int) int {
	if len(key) == 0 || numPartitions <= 0 {
		return 0
	}

	h := fnv.New32a()
	h.Write(key)
	hash := h.Sum32()

	return int(hash % uint32(numPartitions))
}

// RoundRobinPartitioner distributes messages evenly across partitions.
type RoundRobinPartitioner struct {
	counter int
}

// Partition returns the next partition in round-robin order.
func (p *RoundRobinPartitioner) Partition(_ []byte, numPartitions int) int {
	if numPartitions <= 0 {
		return 0
	}
	partition := p.counter % numPartitions
	p.counter++
	return partition
}

// DefaultPartitioner is the default partitioner (hash-based).
var DefaultPartitioner Partitioner = &HashPartitioner{}
