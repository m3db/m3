package tagfiltertree

import (
	"fmt"
	"strings"
)

// trieNode represents a node in the trie.
type trieNode[T any] struct {
	Ch       trieChar
	Children trieChildren[trieNode[T]]
	Data     []T
}

// newEmptyNode creates a new empty node with the given character.
func newEmptyNode[T any](ch byte) *trieNode[T] {
	return &trieNode[T]{
		Ch:       newTrieChar(ch),
		Children: newTrieChildren[trieNode[T]](),
		Data:     nil,
	}
}

// Trie represents a trie data structure.
type Trie[T any] struct {
	Root *trieNode[T]
}

// NewTrie creates a new trie.
func NewTrie[T any]() *Trie[T] {
	return &Trie[T]{
		Root: nil,
	}
}

// Insert inserts a new pattern and data into the trie.
func (tr *Trie[T]) Insert(pattern string, data *T) error {
	if err := ValidatePattern(pattern); err != nil {
		return err
	}

	if tr.Root == nil {
		tr.Root = newEmptyNode[T]('^')
	}

	return insertHelper(tr.Root, pattern, 0, len(pattern), data)
}

// ValidatePattern validates the given pattern.
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
		return fmt.Errorf("multiple composite patterns not supported")
	}

	openIdx := strings.Index(pattern, "{")
	if openIdx != -1 {
		closeIdx := strings.Index(pattern, "}")
		if closeIdx != -1 {
			// we do not support negation within composite patterns.
			if strings.Contains(pattern[openIdx:closeIdx], "!") {
				return fmt.Errorf("negation not supported in composite patterns")
			}
		}
	}

	if strings.Contains(pattern, "[") != strings.Contains(pattern, "]") {
		return fmt.Errorf("invalid pattern")
	}

	// at the moment we only support single char ranges.
	if strings.Count(pattern, "[") > 1 || strings.Count(pattern, "]") > 1 {
		return fmt.Errorf("multiple char range patterns not supported")
	}

	// we do not support certain special chars in the pattern.
	if strings.ContainsAny(pattern, "?") {
		return fmt.Errorf("invalid special chars in pattern")
	}

	for _, ch := range pattern {
		if ch > 127 {
			return fmt.Errorf("invalid character in pattern %c", ch)
		}
	}

	return nil
}

func insertHelper[T any](
	root *trieNode[T],
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

	var (
		err     error
		options []string
	)

	if pattern[startIdx] == '{' {
		options, err = ParseCompositePattern(pattern[startIdx:])
		if err != nil {
			return err
		}
	}

	if pattern[startIdx] == '[' {
		options, err = ParseCharRange(pattern[startIdx:])
		if err != nil {
			return err
		}
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

	if len(options) > 0 {
		// matched composite or char range pattern.
		return nil
	}

	var node *trieNode[T]
	if root.Children.Exists(pattern[startIdx]) {
		node = root.Children.Get(pattern[startIdx])
	}
	if node == nil {
		node = newEmptyNode[T](pattern[startIdx])
	}

	if node.Ch.Char() == '!' {
		if data != nil {
			node.Data = append(node.Data, *data)
		}
	}

	if err := root.Children.Insert(pattern[startIdx], node); err != nil {
		return err
	}

	if startIdx == len(pattern)-1 {
		// found the leaf node.
		node.Ch.SetEnd()
		if data != nil {
			node.Data = append(node.Data, *data)
		}
	}

	return insertHelper(node, pattern, startIdx+1, endIdx, data)
}

// Match matches the given input against the trie.
func (tr *Trie[T]) Match(input string, data *[]T) (bool, error) {
	return matchHelper(tr.Root, input, 0, data)
}

func matchHelper[T any](
	root *trieNode[T],
	input string,
	startIdx int,
	data *[]T,
) (bool, error) {
	if root == nil {
		return false, nil
	}

	if startIdx == len(input) {
		// looks for patterns that end with a '*'.
		var child *trieNode[T]
		if root.Children.Exists('*') {
			child = root.Children.Get('*')
		}
		if child != nil && child.Ch.IsEnd() {
			if data != nil {
				*data = append(*data, child.Data...)
			}
			return true, nil
		}

		return false, nil
	}

	matched := false
	// check matchAll case.
	var child *trieNode[T]
	if root.Children.Exists('*') {
		child = root.Children.Get('*')
	}
	if child != nil {
		var err error

		// move forward in the pattern and input.
		if startIdx < len(input) {
			var subChild *trieNode[T]
			if child.Children.Exists(input[startIdx]) {
				subChild = child.Children.Get(input[startIdx])
			}
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

	child = nil
	if root.Children.Exists('!') {
		child = root.Children.Get('!')
	}
	if child != nil {
		matchedNegate, err := matchHelper(child, input, startIdx, nil)
		if err != nil {
			return false, err
		}

		matched = matched || !matchedNegate
		if !matchedNegate {
			if data != nil {
				*data = append(*data, child.Data...)
			}
		}
	}

	var subChild *trieNode[T]
	if root.Children.Exists(input[startIdx]) {
		subChild = root.Children.Get(input[startIdx])
	}
	if subChild != nil {
		matchedOne, err := matchHelper(subChild, input, startIdx+1, data)
		if err != nil {
			return false, err
		}
		matched = matched || matchedOne

		if startIdx == len(input)-1 {
			if subChild.Ch.IsEnd() {
				if data != nil {
					*data = append(*data, subChild.Data...)
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
		option := strings.TrimSpace(options[i])
		options[i] = optionPrefix + option + optionSuffix
	}

	return options, nil
}

// ParseCharRange extracts the options from a char range pattern.
// It does not check for other invalid chars in the pattern. Those checks should
// be done before calling this function.
func ParseCharRange(pattern string) ([]string, error) {
	openIdx := strings.Index(pattern, "[")
	closeIdx := strings.Index(pattern, "]")

	if openIdx == -1 || closeIdx == -1 {
		return nil, fmt.Errorf("invalid pattern")
	}

	if closeIdx != strings.LastIndex(pattern, "]") {
		return nil, fmt.Errorf("invalid pattern")
	}

	optionStr := pattern[openIdx+1 : closeIdx]
	optionSuffix := ""
	if closeIdx < len(pattern)-1 {
		optionSuffix = pattern[closeIdx+1:]
	}
	optionPrefix := pattern[:openIdx]

	options := make([]string, 0)
	for i := 0; i < len(optionStr); i++ {
		if i+2 < len(optionStr) && optionStr[i+1] == '-' {
			for ch := optionStr[i]; ch <= optionStr[i+2]; ch++ {
				options = append(options, optionPrefix+string(ch)+optionSuffix)
			}
			i += 2
			continue
		}

		options = append(options, optionPrefix+string(optionStr[i])+optionSuffix)
	}

	return options, nil
}
