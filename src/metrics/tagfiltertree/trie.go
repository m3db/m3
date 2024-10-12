package tagfiltertree

import (
	"fmt"
	"strings"
)

type TrieNode struct {
	ch       byte
	children []*TrieNode
	data     []string
	isEnd    bool
	// to denote that the this node belongs to a negation pattern.
	negate bool
	// to record all the pattern IDs that are negated by this node.
	// this is nothing but a hash of the entire pattern string.
	negatedPatternIDs map[string]struct{}
}

func NewEmptyNode(ch byte) *TrieNode {
	return &TrieNode{
		ch:                ch,
		children:          make([]*TrieNode, 256),
		data:              nil,
		isEnd:             false,
		negate:            false,
		negatedPatternIDs: nil,
	}
}

type Trie struct {
	root *TrieNode
}

func NewTrie() *Trie {
	return &Trie{
		root: nil,
	}
}

func (tr *Trie) Insert(pattern string, data string) error {
	if tr.root == nil {
		tr.root = NewEmptyNode('^')
	}

	return insertHelper(tr.root, pattern, 0, len(pattern), data, false)
}

func insertHelper(
	root *TrieNode,
	pattern string,
	startIdx int,
	endIdx int,
	data string,
	negate bool,
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
			if err := insertHelper(root, option, 0, len(option), data, negate); err != nil {
				return err
			}
		}
	}

	node := root.children[pattern[startIdx]]
	if node == nil {
		node = NewEmptyNode(pattern[startIdx])
	}

	if node.ch == '!' {
		// add the data to this negation node.
		node.data = append(node.data, data)

		// mark as negate going forward.
		negate = true

		// add the pattern ID to the negatedPatternIDs.
		if node.negatedPatternIDs == nil {
			node.negatedPatternIDs = make(map[string]struct{}, 4)
		}

		// store the hash of the pattern.
		node.negatedPatternIDs[hashPattern(pattern)] = struct{}{}
	}

	root.children[pattern[startIdx]] = node

	if startIdx == len(pattern)-1 {
		node.isEnd = true
		node.data = append(node.data, data)

		if negate {
			if node.negatedPatternIDs == nil {
				node.negatedPatternIDs = make(map[string]struct{}, 4)
			}
			node.negatedPatternIDs[hashPattern(pattern)] = struct{}{}
		}
	}

	return insertHelper(node, pattern, startIdx+1, endIdx, data, negate)
}

func hashPattern(pattern string) string {
	return pattern
}

func (tr *Trie) Match(input string) ([]string, bool, error) {
	data := make([]string, 0, 5)
	ok, err := matchHelper(tr.root, input, 0, &data)
	return data, ok, err
}

func matchHelper(
	root *TrieNode,
	input string,
	startIdx int,
	data *[]string,
) (bool, error) {
	if root == nil {
		return false, nil
	}

	if startIdx == len(input) {
		// looks for patterns that end with a '*'.
		child := root.children['*']
		if child != nil && child.isEnd {
			*data = append(*data, child.data...)
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
		dataNegate := make([]string, 0)
		_, err := matchHelper(child, input, startIdx, &dataNegate)
		if err != nil {
			return false, err
		}

		// take difference between child.data and dataNegate.
		for _, d := range child.data {
			if !contains(dataNegate, d) {
				matched = true
				*data = append(*data, d)
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
				*data = append(*data, subChild.data...)
				matched = true
			}
		}
	}

	return matched, nil
}

func contains(data []string, d string) bool {
	for _, s := range data {
		if s == d {
			return true
		}
	}
	return false
}
