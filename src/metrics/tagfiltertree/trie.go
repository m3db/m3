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
		children: make([]*TrieNode[T], 128),
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

	return insertHelper(tr.root, pattern, 0, len(pattern), data)
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

	openIdx := strings.Index(pattern, "{")
	if openIdx != -1 {
		closeIdx := strings.Index(pattern, "}")
		if closeIdx != -1 {
			// we do not support negation within composite patterns.
			if strings.Contains(pattern[openIdx:closeIdx], "!") {
				return fmt.Errorf("invalid pattern")
			}
		}
	}

	// we do not support certain special chars in the pattern.
	if strings.ContainsAny(pattern, "[]?") {
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
) error {
	if root == nil {
		return nil
	}

	if startIdx == endIdx {
		return nil
	}

	if pattern[startIdx] == '{' {
		options, err := ParseCompositePattern(pattern[startIdx:])
		if err != nil {
			return err
		}

		for _, option := range options {
			if err := insertHelper(
				root,
				option,
				0,
				len(option),
				data,
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

	return insertHelper(node, pattern, startIdx+1, endIdx, data)
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

// ParseCompositePattern extracts the options from a composite pattern.
// It appends the suffix (the segment after the '}' character) to each option.
// It does not check for other invalid chars in the pattern. Those checks should
// be done before calling this function.
func ParseCompositePattern(pattern string) ([]string, error) {
	openIdx := strings.Index(pattern, "{")
	closeIdx := strings.Index(pattern, "}")

	if openIdx == -1 || closeIdx == -1 {
		return nil, fmt.Errorf("invalid pattern")
	}

	if closeIdx != strings.LastIndex(pattern, "}") {
		return nil, fmt.Errorf("invalid pattern")
	}

	optionStr := pattern[openIdx+1 : closeIdx]
	optionSuffix := ""
	if closeIdx < len(pattern)-1 {
		optionSuffix = pattern[closeIdx+1:]
	}
	optionPrefix := pattern[:openIdx]

	options := strings.Split(optionStr, ",")
	for i := range options {
		options[i] = optionPrefix + options[i] + optionSuffix
	}

	return options, nil
}
