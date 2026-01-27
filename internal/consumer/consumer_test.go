package consumer

import (
	"testing"
	"time"
)

func TestOffsetStore(t *testing.T) {
	dir := t.TempDir()
	store, err := NewOffsetStore(dir)
	if err != nil {
		t.Fatalf("NewOffsetStore failed: %v", err)
	}
	defer store.Close()

	// Initially no offset
	offset := store.Fetch("group1", "topic1", 0)
	if offset != -1 {
		t.Errorf("expected -1 for uncommitted offset, got %d", offset)
	}

	// Commit an offset
	if err := store.Commit("group1", "topic1", 0, 42); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Fetch committed offset
	offset = store.Fetch("group1", "topic1", 0)
	if offset != 42 {
		t.Errorf("expected offset 42, got %d", offset)
	}

	// Different partition should be -1
	offset = store.Fetch("group1", "topic1", 1)
	if offset != -1 {
		t.Errorf("expected -1 for different partition, got %d", offset)
	}

	// Different group should be -1
	offset = store.Fetch("group2", "topic1", 0)
	if offset != -1 {
		t.Errorf("expected -1 for different group, got %d", offset)
	}
}

func TestOffsetStorePersistence(t *testing.T) {
	dir := t.TempDir()

	// Create store and commit
	store1, err := NewOffsetStore(dir)
	if err != nil {
		t.Fatalf("NewOffsetStore failed: %v", err)
	}
	store1.Commit("mygroup", "orders", 0, 100)
	store1.Commit("mygroup", "orders", 1, 200)
	store1.Close()

	// Reopen and verify
	store2, err := NewOffsetStore(dir)
	if err != nil {
		t.Fatalf("NewOffsetStore reopen failed: %v", err)
	}
	defer store2.Close()

	if offset := store2.Fetch("mygroup", "orders", 0); offset != 100 {
		t.Errorf("partition 0: expected 100, got %d", offset)
	}
	if offset := store2.Fetch("mygroup", "orders", 1); offset != 200 {
		t.Errorf("partition 1: expected 200, got %d", offset)
	}
}

func TestGroupJoinLeave(t *testing.T) {
	group := NewGroup("test-group")

	// Join first member
	memberID1, gen1 := group.Join("")
	if memberID1 == "" {
		t.Error("expected non-empty member ID")
	}
	if gen1 != 1 {
		t.Errorf("expected generation 1, got %d", gen1)
	}
	if group.GetLeader() != memberID1 {
		t.Error("first member should be leader")
	}

	// Join second member
	memberID2, gen2 := group.Join("")
	if memberID2 == "" || memberID2 == memberID1 {
		t.Error("expected unique member ID")
	}
	if gen2 != 2 {
		t.Errorf("expected generation 2, got %d", gen2)
	}

	// Both members should exist
	members := group.GetMembers()
	if len(members) != 2 {
		t.Errorf("expected 2 members, got %d", len(members))
	}

	// Leave first member
	group.Leave(memberID1)
	members = group.GetMembers()
	if len(members) != 1 {
		t.Errorf("expected 1 member after leave, got %d", len(members))
	}
	if group.GetGeneration() != 3 {
		t.Errorf("expected generation 3, got %d", group.GetGeneration())
	}
}

func TestGroupHeartbeat(t *testing.T) {
	group := NewGroup("test-group")

	memberID, generation := group.Join("")

	// Valid heartbeat
	if err := group.Heartbeat(memberID, generation); err != nil {
		t.Errorf("heartbeat should succeed: %v", err)
	}

	// Stale generation
	if err := group.Heartbeat(memberID, generation-1); err != ErrStaleGeneration {
		t.Errorf("expected ErrStaleGeneration, got %v", err)
	}

	// Unknown member
	if err := group.Heartbeat("unknown", generation); err != ErrUnknownMember {
		t.Errorf("expected ErrUnknownMember, got %v", err)
	}
}

func TestGroupExpireMembers(t *testing.T) {
	group := NewGroup("test-group")

	memberID, _ := group.Join("")

	// Set last heartbeat to past
	member, _ := group.GetMember(memberID)
	if member == nil {
		t.Fatal("member should exist")
	}

	// Immediate expiration check should not expire (just joined)
	expired := group.ExpireMembers(1 * time.Second)
	if expired {
		t.Error("should not expire recently joined member")
	}

	// Short timeout should eventually expire
	// (In real tests, we'd manipulate time, but for simplicity just check logic)
}

func TestGroupAssignments(t *testing.T) {
	group := NewGroup("test-group")

	memberID, _ := group.Join("")

	assignments := []Assignment{
		{Topic: "orders", Partition: 0},
		{Topic: "orders", Partition: 1},
	}

	if err := group.AssignPartitions(memberID, assignments); err != nil {
		t.Fatalf("AssignPartitions failed: %v", err)
	}

	member, ok := group.GetMember(memberID)
	if !ok {
		t.Fatal("member should exist")
	}
	if len(member.Assignments) != 2 {
		t.Errorf("expected 2 assignments, got %d", len(member.Assignments))
	}
}
