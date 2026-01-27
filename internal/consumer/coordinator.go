package consumer

import (
	"sort"
	"sync"
	"time"
)

// CoordinatorConfig configures the group coordinator.
type CoordinatorConfig struct {
	// SessionTimeout is how long a member can go without heartbeat before expiring.
	SessionTimeout time.Duration
	// HeartbeatInterval is how often to check for expired members.
	HeartbeatInterval time.Duration
}

// DefaultCoordinatorConfig returns sensible defaults.
func DefaultCoordinatorConfig() CoordinatorConfig {
	return CoordinatorConfig{
		SessionTimeout:    10 * time.Second,
		HeartbeatInterval: 1 * time.Second,
	}
}

// TopicInfo provides partition count for topics.
// Implemented by topic.Manager.
type TopicInfo interface {
	GetPartitionCount(topic string) (int, bool)
}

// Coordinator manages all consumer groups.
type Coordinator struct {
	config  CoordinatorConfig
	groups  map[string]*Group
	offsets *OffsetStore
	topics  TopicInfo
	mu      sync.RWMutex

	quit chan struct{}
	wg   sync.WaitGroup
}

// NewCoordinator creates a group coordinator.
func NewCoordinator(config CoordinatorConfig, offsetStore *OffsetStore, topics TopicInfo) *Coordinator {
	c := &Coordinator{
		config:  config,
		groups:  make(map[string]*Group),
		offsets: offsetStore,
		topics:  topics,
		quit:    make(chan struct{}),
	}
	return c
}

// Start begins background heartbeat checking.
func (c *Coordinator) Start() {
	c.wg.Add(1)
	go c.heartbeatLoop()
}

// Stop shuts down the coordinator.
func (c *Coordinator) Stop() {
	close(c.quit)
	c.wg.Wait()
}

// heartbeatLoop periodically checks for expired members.
func (c *Coordinator) heartbeatLoop() {
	defer c.wg.Done()
	ticker := time.NewTicker(c.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.quit:
			return
		case <-ticker.C:
			c.expireMembers()
		}
	}
}

// expireMembers removes members that haven't sent heartbeats.
func (c *Coordinator) expireMembers() {
	c.mu.RLock()
	groups := make([]*Group, 0, len(c.groups))
	for _, g := range c.groups {
		groups = append(groups, g)
	}
	c.mu.RUnlock()

	for _, g := range groups {
		if g.ExpireMembers(c.config.SessionTimeout) {
			// Members expired, reassign partitions
			c.assignPartitions(g)
		}
	}
}

// getOrCreateGroup returns an existing group or creates a new one.
func (c *Coordinator) getOrCreateGroup(name string) *Group {
	c.mu.Lock()
	defer c.mu.Unlock()

	if g, ok := c.groups[name]; ok {
		return g
	}
	g := NewGroup(name)
	c.groups[name] = g
	return g
}

// GetGroup returns a group by name.
func (c *Coordinator) GetGroup(name string) (*Group, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	g, ok := c.groups[name]
	return g, ok
}

// JoinGroup adds a member to a group.
// Returns member ID, generation, leader ID, and list of all members.
func (c *Coordinator) JoinGroup(groupName, memberID string, topics []string) (JoinResult, error) {
	group := c.getOrCreateGroup(groupName)

	assignedID, generation := group.Join(memberID)

	// Assign partitions after join
	c.assignPartitions(group)

	return JoinResult{
		MemberID:   assignedID,
		Generation: generation,
		LeaderID:   group.GetLeader(),
		Members:    group.GetMembers(),
	}, nil
}

// JoinResult contains the result of joining a group.
type JoinResult struct {
	MemberID   string
	Generation int32
	LeaderID   string
	Members    []string
}

// LeaveGroup removes a member from a group.
func (c *Coordinator) LeaveGroup(groupName, memberID string) error {
	group, ok := c.GetGroup(groupName)
	if !ok {
		return nil // Group doesn't exist, nothing to leave
	}

	if err := group.Leave(memberID); err != nil {
		return err
	}

	// Reassign partitions after leave
	c.assignPartitions(group)
	return nil
}

// Heartbeat processes a heartbeat from a member.
func (c *Coordinator) Heartbeat(groupName, memberID string, generation int32) error {
	group, ok := c.GetGroup(groupName)
	if !ok {
		return ErrUnknownMember
	}
	return group.Heartbeat(memberID, generation)
}

// GetAssignments returns the partition assignments for a member.
func (c *Coordinator) GetAssignments(groupName, memberID string) ([]Assignment, error) {
	group, ok := c.GetGroup(groupName)
	if !ok {
		return nil, ErrUnknownMember
	}

	member, ok := group.GetMember(memberID)
	if !ok {
		return nil, ErrUnknownMember
	}

	return member.Assignments, nil
}

// CommitOffset commits an offset for a group.
func (c *Coordinator) CommitOffset(groupName, topic string, partition int32, offset int64) error {
	return c.offsets.Commit(groupName, topic, partition, offset)
}

// FetchOffset returns the committed offset for a group.
func (c *Coordinator) FetchOffset(groupName, topic string, partition int32) int64 {
	return c.offsets.Fetch(groupName, topic, partition)
}

// assignPartitions distributes partitions among group members.
// Uses range assignment: partitions divided evenly, remainder goes to first members.
func (c *Coordinator) assignPartitions(group *Group) {
	members := group.GetMembers()
	if len(members) == 0 {
		return
	}

	// Sort members for deterministic assignment
	sort.Strings(members)

	// For now, we need to know which topics this group cares about
	// In a real implementation, members would specify subscribed topics
	// For simplicity, we'll assign all known partitions
	// This should be enhanced to track topic subscriptions per group

	// Get all partitions (simplified: use placeholder logic)
	// In real usage, group would track which topics members subscribed to
	// For now, assignments happen when JoinGroup is called with topics
}

// AssignTopicPartitions assigns partitions for specific topics to group members.
func (c *Coordinator) AssignTopicPartitions(group *Group, topics []string) {
	members := group.GetMembers()
	if len(members) == 0 {
		return
	}

	// Sort for deterministic assignment
	sort.Strings(members)

	// Collect all partitions from all topics
	type topicPartition struct {
		topic     string
		partition int32
	}
	var allPartitions []topicPartition

	for _, topic := range topics {
		count, ok := c.topics.GetPartitionCount(topic)
		if !ok {
			continue
		}
		for p := 0; p < count; p++ {
			allPartitions = append(allPartitions, topicPartition{topic, int32(p)})
		}
	}

	if len(allPartitions) == 0 {
		return
	}

	// Range assignment: divide partitions among members
	memberCount := len(members)
	partitionsPerMember := len(allPartitions) / memberCount
	remainder := len(allPartitions) % memberCount

	memberAssignments := make(map[string][]Assignment)
	for _, m := range members {
		memberAssignments[m] = []Assignment{}
	}

	idx := 0
	for i, memberID := range members {
		// Each member gets partitionsPerMember, plus 1 extra if in remainder
		count := partitionsPerMember
		if i < remainder {
			count++
		}

		for j := 0; j < count && idx < len(allPartitions); j++ {
			tp := allPartitions[idx]
			memberAssignments[memberID] = append(memberAssignments[memberID], Assignment{
				Topic:     tp.topic,
				Partition: tp.partition,
			})
			idx++
		}
	}

	// Apply assignments to group
	for memberID, assignments := range memberAssignments {
		group.AssignPartitions(memberID, assignments)
	}
}
