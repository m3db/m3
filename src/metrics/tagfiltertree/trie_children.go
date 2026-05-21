package tagfiltertree

type trieChildren[T any] struct {
	Children   []*T
	PointerSet PointerSet
}

func newTrieChildren[T any]() trieChildren[T] {
	return trieChildren[T]{
		Children: nil,
	}
}

// Insert inserts a new child with the given byte and data.
func (tc *trieChildren[T]) Insert(ch byte, data *T) error {
	if tc.PointerSet.IsSet(ch) {
		// already exists.
		return nil
	}

	newIdx := tc.PointerSet.CountSetBitsUntil(ch)

	// make room for the new child.
	tc.Children = append(tc.Children, nil)

	for i := len(tc.Children) - 1; i > newIdx; i-- {
		tc.Children[i] = tc.Children[i-1]
	}

	// set the new data in the correct idx.
	tc.Children[newIdx] = data

	// set the idx of the new child.
	tc.PointerSet.Set(ch)

	return nil
}

func (tc *trieChildren[T]) Get(ch byte) *T {
	if !tc.PointerSet.IsSet(ch) {
		return nil
	}

	childIdx := tc.PointerSet.CountSetBitsUntil(ch) - 1
	return tc.Children[childIdx]
}

// Exists returns true if the child with the given byte exists.
func (tc *trieChildren[T]) Exists(ch byte) bool {
	return tc.PointerSet.IsSet(ch)
}
