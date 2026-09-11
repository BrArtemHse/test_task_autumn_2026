package id

import "testing"

func TestNewStringReturnsUUIDLikeUniqueValues(t *testing.T) {
	first := NewString()
	second := NewString()

	if first == second {
		t.Fatal("NewString() returned duplicate values")
	}
	if len(first) != 36 {
		t.Fatalf("first id length = %d, want 36", len(first))
	}
	if first[14] != '4' {
		t.Fatalf("uuid version = %q, want 4", first[14])
	}
	if first[8] != '-' || first[13] != '-' || first[18] != '-' || first[23] != '-' {
		t.Fatalf("id = %q, want UUID hyphen layout", first)
	}
}
