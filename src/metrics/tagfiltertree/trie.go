package tagfiltertree

import (
	"fmt"
	"strings"
)

type TrieNode[T any] struct {
	ch       byte
	children []*TrieNode[T]
	data     []T
	isEnd    bool
}

func NewEmptyNode[T any](ch byte) *TrieNode[T] {
	return &TrieNode[T]{
		ch:       ch,
		children: make([]*TrieNode[T], 256),
		data:     nil,
		isEnd:    false,
	}
}

type Trie[T any] struct {
	root *TrieNode[T]
}

func NewTrie[T any]() *Trie[T] {
	return &Trie[T]{
		root: nil,
	}
}

func (tr *Trie[T]) Insert(pattern string, data *T) error {
	if err := ValidatePattern(pattern); err != nil {
		return err
	}

	if tr.root == nil {
		tr.root = NewEmptyNode[T]('^')
	}

	return insertHelper(tr.root, pattern, 0, len(pattern), data, nil, -1)
}

func ValidatePattern(pattern string) error {
	if len(pattern) == 0 {
		return fmt.Errorf("empty pattern")
	}

	if strings.Contains(pattern, "**") {
		return fmt.Errorf("invalid pattern")
	}

	if strings.Contains(pattern, "{") != strings.Contains(pattern, "}") {
		return fmt.Errorf("invalid pattern")
	}

	// pattern cannot have more than one pair of '{' and '}'.
	if strings.Count(pattern, "{") > 1 || strings.Count(pattern, "}") > 1 {
		return fmt.Errorf("invalid pattern")
	}

	if strings.Contains(pattern, "{") {
		// '}' should always be the last character in the pattern.
		if strings.Index(pattern, "}") != len(pattern)-1 {
			return fmt.Errorf("invalid pattern")
		}
	}

	// a '{' if present can only be the first or second character.
	if strings.Index(pattern, "{") > 1 {
		return fmt.Errorf("invalid pattern")
	}

	// if '{' is the second char, then the first char can only be '!'.
	if strings.Index(pattern, "{") == 1 {
		if pattern[0] != '!' {
			return fmt.Errorf("invalid pattern")
		}
	}

	// as of now we don't support '!' within a composite pattern.
	if strings.Index(pattern, "!") > 0 {
		return fmt.Errorf("invalid pattern")
	}

	return nil
}

func insertHelper[T any](
	root *TrieNode[T],
	pattern string,
	startIdx int,
	endIdx int,
	data *T,
	negatedNodes map[*TrieNode[T]]struct{},
	negatedPatternID int,
) error {
	if root == nil {
		return nil
	}

	if startIdx == endIdx {
		return nil
	}

	if pattern[startIdx] == '{' {
		endIdx = startIdx + 1
		for endIdx < len(pattern) && pattern[endIdx] != '}' {
			endIdx++
		}
		if endIdx == len(pattern) {
			return fmt.Errorf("invalid pattern")
		}

		// insert the pattern
		optionStr := pattern[startIdx+1 : endIdx]
		options := strings.Split(optionStr, ",")
		for _, option := range options {
			if err := insertHelper(
				root,
				option,
				0,
				len(option),
				data,
				negatedNodes,
				negatedPatternID,
			); err != nil {
				return err
			}
		}

		return nil
	}

	node := root.children[pattern[startIdx]]
	if node == nil {
		node = NewEmptyNode[T](pattern[startIdx])
	}

	if node.ch == '!' {
		if node.data == nil {
			node.data = make([]T, 0, 2)
		}

		if data != nil {
			node.data = append(node.data, *data)
		}
	}

	root.children[pattern[startIdx]] = node

	if startIdx == len(pattern)-1 {
		// found the leaf node.
		node.isEnd = true
		if data != nil {
			node.data = append(node.data, *data)
		}
	}

	return insertHelper(node, pattern, startIdx+1, endIdx, data, negatedNodes, negatedPatternID)
}

func (tr *Trie[T]) Match(input string, data *[]T) (bool, error) {
	return matchHelper(tr.root, input, 0, data)
}

func matchHelper[T any](
	root *TrieNode[T],
	input string,
	startIdx int,
	data *[]T,
) (bool, error) {
	if root == nil {
		return false, nil
	}

	if startIdx == len(input) {
		// looks for patterns that end with a '*'.
		child := root.children['*']
		if child != nil && child.isEnd {
			if data != nil {
				*data = append(*data, child.data...)
			}
			return true, nil
		}

		return false, nil
	}

	matched := false
	// check matchAll case.
	child := root.children['*']
	if child != nil {
		var err error

		// move forward in the pattern and input.
		if startIdx < len(input) {
			subChild := child.children[input[startIdx]]
			if subChild != nil {
				matchedOne, err := matchHelper(subChild, input, startIdx+1, data)
				if err != nil {
					return false, err
				}
				matched = matched || matchedOne
			}
		}

		// stay on the '*'.
		matchedAll, err := matchHelper(root, input, startIdx+1, data)
		if err != nil {
			return false, err
		}

		matched = matched || matchedAll
	}

	child = root.children['!']
	if child != nil {
		matchedNegate, err := matchHelper(child, input, startIdx, nil)
		if err != nil {
			return false, err
		}

		matched = matched || !matchedNegate
		if !matchedNegate {
			if data != nil {
				*data = append(*data, child.data...)
			}
		}
	}

	subChild := root.children[input[startIdx]]
	if subChild != nil {
		matchedOne, err := matchHelper(subChild, input, startIdx+1, data)
		if err != nil {
			return false, err
		}
		matched = matched || matchedOne

		if startIdx == len(input)-1 {
			if subChild.isEnd {
				if data != nil {
					*data = append(*data, subChild.data...)
				}
				matched = true
			}
		}
	}

	return matched, nil
}
