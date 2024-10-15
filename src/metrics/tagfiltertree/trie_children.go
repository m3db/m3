package tagfiltertree

import "fmt"

type trieChildren[T any] struct {
	children   []*T
	pointerSet PointerSet
}

func newTrieChildren[T any]() trieChildren[T] {
	return trieChildren[T]{
		children: nil,
	}
}

func (tc *trieChildren[T]) Insert(ch byte, data *T) error {
	if tc.pointerSet.IsSet(ch) {
		return fmt.Errorf("character already exists")
	}

	newIdx := tc.pointerSet.CountSetBitsUntil(ch)

	// make room for the new child.
	tc.children = append(tc.children, nil)

	for i := len(tc.children) - 1; i > newIdx; i-- {
		tc.children[i] = tc.children[i-1]
	}

	// set the new data in the correct idx.
	tc.children[newIdx] = data

	// set the idx of the new child.
	tc.pointerSet.Set(ch)

	return nil
}

func (tc *trieChildren[T]) Get(ch byte) *T {
	if !tc.pointerSet.IsSet(ch) {
		return nil
	}

	childIdx := tc.pointerSet.CountSetBitsUntil(ch) - 1
	return tc.children[childIdx]
}

func (tc *trieChildren[T]) IsSet(ch byte) bool {
	return tc.pointerSet.IsSet(ch)
}
