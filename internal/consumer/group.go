package consumer

import (
	"crypto/rand"
	"fmt"
	"sync"
	"time"
)

// MemberState represents a member's current status.
type MemberState int

const (
	MemberStateActive MemberState = iota
	MemberStateLeaving
)

// Member represents a consumer in a group.
type Member struct {
	ID            string      // Unique member ID (UUID)
	State         MemberState // Current state
	LastHeartbeat time.Time   // Last heartbeat received
	Assignments   []Assignment // Partitions assigned to this member
}

// Assignment represents a topic-partition assigned to a member.
type Assignment struct {
	Topic     string
	Partition int32
}

// Group represents a consumer group.
type Group struct {
	Name       string             // Group name
	Generation int32              // Incremented on each rebalance
	LeaderID   string             // Current leader member ID
	Members    map[string]*Member // MemberID -> Member
	State      GroupState         // Current group state

	mu sync.RWMutex
}

// GroupState represents the group's current state.
type GroupState int

const (
	GroupStateEmpty GroupState = iota      // No members
	GroupStateStable                       // Members assigned, running
	GroupStateRebalancing                  // Rebalance in progress
)

// NewGroup creates a new empty consumer group.
func NewGroup(name string) *Group {
	return &Group{
		Name:       name,
		Generation: 0,
		Members:    make(map[string]*Member),
		State:      GroupStateEmpty,
	}
}

// Join adds a member to the group and triggers a rebalance.
// If memberID is empty, a new ID is generated.
// Returns the assigned member ID and the new generation.
func (g *Group) Join(memberID string) (string, int32) {
	g.mu.Lock()
	defer g.mu.Unlock()

	// Generate member ID if not provided
	if memberID == "" {
		memberID = generateMemberID()
	}

	// Check if member already exists
	if member, ok := g.Members[memberID]; ok {
		// Existing member rejoining - update heartbeat
		member.LastHeartbeat = time.Now()
		member.State = MemberStateActive
	} else {
		// New member
		g.Members[memberID] = &Member{
			ID:            memberID,
			State:         MemberStateActive,
			LastHeartbeat: time.Now(),
		}
	}

	// Trigger rebalance
	g.rebalance()

	return memberID, g.Generation
}

// Leave removes a member from the group and triggers a rebalance.
func (g *Group) Leave(memberID string) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if _, ok := g.Members[memberID]; !ok {
		return nil // Member not in group, nothing to do
	}

	delete(g.Members, memberID)

	// Trigger rebalance if there are remaining members
	if len(g.Members) > 0 {
		g.rebalance()
	} else {
		g.State = GroupStateEmpty
		g.LeaderID = ""
	}

	return nil
}

// Heartbeat updates the last heartbeat time for a member.
// Returns an error if the member is not in the group or generation is stale.
func (g *Group) Heartbeat(memberID string, generation int32) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	member, ok := g.Members[memberID]
	if !ok {
		return ErrUnknownMember
	}

	if generation != g.Generation {
		return ErrStaleGeneration
	}

	member.LastHeartbeat = time.Now()
	return nil
}

// GetMember returns a member by ID.
func (g *Group) GetMember(memberID string) (*Member, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	member, ok := g.Members[memberID]
	if !ok {
		return nil, false
	}

	// Return a copy to avoid race conditions
	copy := &Member{
		ID:            member.ID,
		State:         member.State,
		LastHeartbeat: member.LastHeartbeat,
		Assignments:   make([]Assignment, len(member.Assignments)),
	}
	for i, a := range member.Assignments {
		copy.Assignments[i] = a
	}
	return copy, true
}

// GetMembers returns all member IDs.
func (g *Group) GetMembers() []string {
	g.mu.RLock()
	defer g.mu.RUnlock()

	members := make([]string, 0, len(g.Members))
	for id := range g.Members {
		members = append(members, id)
	}
	return members
}

// GetGeneration returns the current generation.
func (g *Group) GetGeneration() int32 {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.Generation
}

// GetLeader returns the current leader ID.
func (g *Group) GetLeader() string {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.LeaderID
}

// IsLeader checks if a member is the group leader.
func (g *Group) IsLeader(memberID string) bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.LeaderID == memberID
}

// rebalance reassigns partitions among members.
// Must be called with lock held.
func (g *Group) rebalance() {
	g.Generation++
	g.State = GroupStateRebalancing

	// Elect leader (first member alphabetically for determinism)
	var leaderID string
	for id := range g.Members {
		if leaderID == "" || id < leaderID {
			leaderID = id
		}
	}
	g.LeaderID = leaderID

	// Clear all assignments - coordinator will reassign
	for _, member := range g.Members {
		member.Assignments = nil
	}

	g.State = GroupStateStable
}

// AssignPartitions assigns partitions to a member.
// Used by the coordinator after computing assignments.
func (g *Group) AssignPartitions(memberID string, assignments []Assignment) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	member, ok := g.Members[memberID]
	if !ok {
		return ErrUnknownMember
	}

	member.Assignments = assignments
	return nil
}

// ExpireMembers removes members that haven't sent a heartbeat within timeout.
// Returns true if any members were removed (triggering a rebalance).
func (g *Group) ExpireMembers(timeout time.Duration) bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	now := time.Now()
	expired := false

	for id, member := range g.Members {
		if now.Sub(member.LastHeartbeat) > timeout {
			delete(g.Members, id)
			expired = true
		}
	}

	if expired {
		if len(g.Members) > 0 {
			g.rebalance()
		} else {
			g.State = GroupStateEmpty
			g.LeaderID = ""
		}
	}

	return expired
}

// Errors
var (
	ErrUnknownMember   = &GroupError{"unknown member"}
	ErrStaleGeneration = &GroupError{"stale generation"}
)

// GroupError represents a consumer group error.
type GroupError struct {
	Message string
}

func (e *GroupError) Error() string {
	return e.Message
}

// generateMemberID creates a random member ID.
func generateMemberID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
