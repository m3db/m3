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
	// a '!' node will store all the leaf nodes
	// of all the patterns that are negated.
	// During a Match() call, we will take the difference
	// between the data within the negated nodes and the
	// matched nodes.
	// for instance, for "!foo" the '!' node will store
	// the leaf node of "foo" which is last 'o' with the
	// isEnd flag set to true.
	negatedNodes      map[*TrieNode]struct{}
	negatedPatternIDs []int
}

func NewEmptyNode(ch byte) *TrieNode {
	return &TrieNode{
		ch:                ch,
		children:          make([]*TrieNode, 256),
		data:              nil,
		isEnd:             false,
		negatedNodes:      nil,
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
	if err := ValidatePattern(pattern); err != nil {
		return err
	}

	if tr.root == nil {
		tr.root = NewEmptyNode('^')
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

func insertHelper(
	root *TrieNode,
	pattern string,
	startIdx int,
	endIdx int,
	data string,
	negatedNodes map[*TrieNode]struct{},
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
		node = NewEmptyNode(pattern[startIdx])
	}

	if node.ch == '!' {
		// allocate the negatedNodes map and pass it to the children.
		//assert(negatedNodes == nil, "negatedNodes should be nil")
		if node.negatedNodes == nil {
			node.negatedNodes = make(map[*TrieNode]struct{}, 4)
		}

		// set the negatedNodes here so that
		// the children can add themselves to the parent's negatedNodes.
		// note that this node '!' is the parent node.
		negatedNodes = node.negatedNodes

		// compute the negatedPatternID.
		negatedPatternID = hashPattern(pattern)
	}

	root.children[pattern[startIdx]] = node

	if startIdx == len(pattern)-1 {
		// found the leaf node.
		node.isEnd = true
		node.data = append(node.data, data)

		if negatedPatternID != -1 {
			// this is a negated node.
			if node.negatedPatternIDs == nil {
				node.negatedPatternIDs = make([]int, 0, 4)
			}
			// set the negatedPatternID in this node.
			node.negatedPatternIDs = append(node.negatedPatternIDs, negatedPatternID)

			if negatedNodes != nil {
				// add the node to the negatedNodes of the parent.
				negatedNodes[node] = struct{}{}
			}
		}
	}

	return insertHelper(node, pattern, startIdx+1, endIdx, data, negatedNodes, negatedPatternID)
}

func hashPattern(pattern string) int {
	// simple hash function.
	hash := 0
	for i := 0; i < len(pattern); i++ {
		hash = hash*31 + int(pattern[i])
	}
	return hash
}

func (tr *Trie) Match(input string) ([]string, bool, error) {
	var data []string
	matchedNodes := make([]*TrieNode, 0, 4)
	ok, err := matchHelper(tr.root, input, 0, &matchedNodes)
	for _, node := range matchedNodes {
		if len(node.data) > 0 && data == nil {
			data = make([]string, 0, len(node.data))
		}
		data = append(data, node.data...)
	}
	return data, ok, err
}

func matchHelper(
	root *TrieNode,
	input string,
	startIdx int,
	matchedNodes *[]*TrieNode,
) (bool, error) {
	if root == nil {
		return false, nil
	}

	if startIdx == len(input) {
		// looks for patterns that end with a '*'.
		child := root.children['*']
		if child != nil && child.isEnd {
			*matchedNodes = append(*matchedNodes, child)
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
				matchedOne, err := matchHelper(subChild, input, startIdx+1, matchedNodes)
				if err != nil {
					return false, err
				}
				matched = matched || matchedOne
			}
		}

		// stay on the '*'.
		matchedAll, err := matchHelper(root, input, startIdx+1, matchedNodes)
		if err != nil {
			return false, err
		}

		matched = matched || matchedAll
	}

	child = root.children['!']
	if child != nil {
		matchedNegatedNodes := make([]*TrieNode, 0)
		_, err := matchHelper(child, input, startIdx, &matchedNegatedNodes)
		if err != nil {
			return false, err
		}

		// take difference between child.data and dataNegate.
		for negatedNode := range child.negatedNodes {
			// multiple leaf nodes might be part of the same pattern.
			// for example in case of "!{foo*,bar*}", both "foo*" and "bar*"
			// will be part of the same pattern.
			// therefore the '*' from "foo*" and the '*' from "bar*" will be
			// will both be unique leaf nodes but will have the same data.
			// Our goal is to remove all the nodes belonging to the same pattern
			// as any of the matchedNegatedNodes.
			if !containsPatternID(matchedNegatedNodes, negatedNode.negatedPatternIDs) {
				matched = true
				*matchedNodes = append(*matchedNodes, negatedNode)
			}
		}
	}

	subChild := root.children[input[startIdx]]
	if subChild != nil {
		matchedOne, err := matchHelper(subChild, input, startIdx+1, matchedNodes)
		if err != nil {
			return false, err
		}
		matched = matched || matchedOne

		if startIdx == len(input)-1 {
			if subChild.isEnd {
				*matchedNodes = append(*matchedNodes, subChild)
				matched = true
			}
		}
	}

	return matched, nil
}

func containsPatternID(nodes []*TrieNode, patternIDs []int) bool {
	for _, n := range nodes {
		for _, patternID := range patternIDs {
			for _, negatedPatternID := range n.negatedPatternIDs {
				if negatedPatternID == patternID {
					return true
				}
			}
		}
	}
	return false
}
