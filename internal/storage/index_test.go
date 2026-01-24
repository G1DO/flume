package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNewIndex(t *testing.T) {
	dir := t.TempDir()

	idx, err := NewIndex(dir, 0, IndexConfig{ByteInterval: 100})
	if err != nil {
		t.Fatalf("NewIndex failed: %v", err)
	}
	defer idx.Close()

	// Verify index file was created
	indexPath := filepath.Join(dir, "00000000000000000000.index")
	if _, err := os.Stat(indexPath); os.IsNotExist(err) {
		t.Error("index file was not created")
	}

	if idx.Len() != 0 {
		t.Errorf("expected 0 entries, got %d", idx.Len())
	}
}

func TestIndexMaybeAdd(t *testing.T) {
	dir := t.TempDir()

	// Set small interval for testing
	idx, err := NewIndex(dir, 0, IndexConfig{ByteInterval: 100})
	if err != nil {
		t.Fatalf("NewIndex failed: %v", err)
	}
	defer idx.Close()

	// First add should always create an entry (empty index)
	added, err := idx.MaybeAdd(0, 0, 50)
	if err != nil {
		t.Fatalf("MaybeAdd failed: %v", err)
	}
	if !added {
		t.Error("first entry should always be added")
	}
	if idx.Len() != 1 {
		t.Errorf("expected 1 entry, got %d", idx.Len())
	}

	// Second add with small size should not create entry
	added, err = idx.MaybeAdd(1, 50, 30)
	if err != nil {
		t.Fatalf("MaybeAdd failed: %v", err)
	}
	if added {
		t.Error("should not add entry before threshold")
	}
	if idx.Len() != 1 {
		t.Errorf("expected 1 entry, got %d", idx.Len())
	}

	// Third add that crosses threshold should create entry
	added, err = idx.MaybeAdd(2, 80, 50) // total 80 bytes since last entry
	if err != nil {
		t.Fatalf("MaybeAdd failed: %v", err)
	}
	if added {
		t.Error("80 bytes should not trigger entry (threshold is 100)")
	}

	// This should cross the threshold (80 + 30 = 110 >= 100)
	added, err = idx.MaybeAdd(3, 130, 30)
	if err != nil {
		t.Fatalf("MaybeAdd failed: %v", err)
	}
	if !added {
		t.Error("should add entry after crossing threshold")
	}
	if idx.Len() != 2 {
		t.Errorf("expected 2 entries, got %d", idx.Len())
	}
}

func TestIndexFind(t *testing.T) {
	dir := t.TempDir()

	idx, err := NewIndex(dir, 0, IndexConfig{ByteInterval: 100})
	if err != nil {
		t.Fatalf("NewIndex failed: %v", err)
	}
	defer idx.Close()

	// Add some entries manually by simulating threshold crossings
	idx.MaybeAdd(0, 0, 100)     // entry: offset 0 -> position 0
	idx.MaybeAdd(10, 500, 100)  // entry: offset 10 -> position 500
	idx.MaybeAdd(20, 1000, 100) // entry: offset 20 -> position 1000

	tests := []struct {
		targetOffset   int64
		expectedPos    int64
		expectedFound  bool
		description    string
	}{
		{0, 0, true, "exact match first entry"},
		{5, 0, true, "between first and second, returns first"},
		{10, 500, true, "exact match second entry"},
		{15, 500, true, "between second and third, returns second"},
		{20, 1000, true, "exact match third entry"},
		{25, 1000, true, "after last entry, returns last"},
	}

	for _, tt := range tests {
		pos, found := idx.Find(tt.targetOffset)
		if found != tt.expectedFound {
			t.Errorf("%s: expected found=%v, got %v", tt.description, tt.expectedFound, found)
		}
		if pos != tt.expectedPos {
			t.Errorf("%s: expected position %d, got %d", tt.description, tt.expectedPos, pos)
		}
	}
}

func TestIndexFindEmpty(t *testing.T) {
	dir := t.TempDir()

	idx, err := NewIndex(dir, 0, IndexConfig{ByteInterval: 100})
	if err != nil {
		t.Fatalf("NewIndex failed: %v", err)
	}
	defer idx.Close()

	// Don't add any entries
	_, found := idx.Find(5)
	if found {
		t.Error("Find on empty index should return found=false")
	}
}

func TestIndexRecover(t *testing.T) {
	dir := t.TempDir()

	// Create index and add entries
	idx, err := NewIndex(dir, 0, IndexConfig{ByteInterval: 100})
	if err != nil {
		t.Fatalf("NewIndex failed: %v", err)
	}

	idx.MaybeAdd(0, 0, 100)
	idx.MaybeAdd(10, 500, 100)
	idx.MaybeAdd(20, 1000, 100)

	if idx.Len() != 3 {
		t.Fatalf("expected 3 entries before close, got %d", idx.Len())
	}
	idx.Close()

	// Reopen and recover
	idx2, err := NewIndex(dir, 0, IndexConfig{ByteInterval: 100})
	if err != nil {
		t.Fatalf("NewIndex (reopen) failed: %v", err)
	}
	defer idx2.Close()

	if err := idx2.Recover(); err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	if idx2.Len() != 3 {
		t.Errorf("expected 3 entries after recovery, got %d", idx2.Len())
	}

	// Verify entries are correct
	pos, found := idx2.Find(15)
	if !found {
		t.Error("Find should succeed after recovery")
	}
	if pos != 500 {
		t.Errorf("expected position 500 for offset 15, got %d", pos)
	}
}

func TestIndexPersistence(t *testing.T) {
	dir := t.TempDir()

	// Create and populate index
	idx, err := NewIndex(dir, 0, IndexConfig{ByteInterval: 50})
	if err != nil {
		t.Fatalf("NewIndex failed: %v", err)
	}

	idx.MaybeAdd(0, 0, 50)
	idx.MaybeAdd(5, 100, 50)
	idx.Close()

	// Verify file size (2 entries * 16 bytes = 32 bytes)
	indexPath := filepath.Join(dir, "00000000000000000000.index")
	info, err := os.Stat(indexPath)
	if err != nil {
		t.Fatalf("stat failed: %v", err)
	}

	expectedSize := int64(2 * IndexEntrySize)
	if info.Size() != expectedSize {
		t.Errorf("expected file size %d, got %d", expectedSize, info.Size())
	}
}
