package tagfiltertree

import (
	"testing"
)

func TestTrieChildrenGet(t *testing.T) {
	tc := newTrieChildren[int]()

	// Insert a child.
	data := 42
	err := tc.Insert('a', &data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Get the child.
	got := tc.Get('a')
	if got == nil {
		t.Fatalf("expected child to exist")
	}
	if *got != data {
		t.Fatalf("unexpected data: got %v, want %v", *got, data)
	}

	// Get a non-existing child.
	got = tc.Get('b')
	if got != nil {
		t.Fatalf("unexpected child: got %v, want nil", *got)
	}
}
