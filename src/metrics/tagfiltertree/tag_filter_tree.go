package tagfiltertree

import (
	"errors"
	"strings"

	"github.com/m3db/m3/src/metrics/filters"
)

const (
	_matchall = "*"
)

type Tag struct {
	Name   string
	Values []string
}

// Tree is a tree data structure for tag filters.
type Tree[T any] struct {
	Nodes map[string]*node[T]
}

type node[T any] struct {
	Name string
	// key=tagValue
	Values map[string]*Tree[T]
	Data   []T
}

// New creates a new tree.
func New[T any]() *Tree[T] {
	return &Tree[T]{
		Nodes: make(map[string]*node[T]),
	}
}

// AddTagFilter adds a tag filter to the tree.
func (t *Tree[T]) AddTagFilter(tags []Tag, data T) {
	addNode(t, tags, 0, data)
}

func (n *node[T]) addValue(value string) *Tree[T] {
	if _, ok := n.Values[value]; !ok {
		n.Values[value] = &Tree[T]{
			Nodes: make(map[string]*node[T]),
		}
	}
	return n.Values[value]
}

func addNode[T any](t *Tree[T], tags []Tag, idx int, data T) {
	if idx >= len(tags) {
		return
	}

	tag := tags[idx]
	if _, ok := t.Nodes[tag.Name]; !ok {
		t.Nodes[tag.Name] = &node[T]{
			Name:   tag.Name,
			Values: make(map[string]*Tree[T]),
			Data:   make([]T, 0),
		}
	}
	node := t.Nodes[tag.Name]
	// AddValue returns a tree along the path of each added value.
	childTrees := make([]*Tree[T], 0)
	for _, value := range tag.Values {
		childTrees = append(childTrees, node.addValue(value))
	}

	// Add the data if this is the last tag.
	if idx == len(tags)-1 {
		node.Data = append(node.Data, data)
	}

	// Recurse to the next tag for each of the childTrees.
	for _, childTree := range childTrees {
		addNode(childTree, tags, idx+1, data)
	}
}

// Match returns the data for the given tags.
func (t *Tree[T]) Match(tags map[string]string) []T {
	return match(t, tags)
}

func match[T any](t *Tree[T], tags map[string]string) []T {
	if len(tags) == 0 || t == nil {
		return nil
	}

	data := make([]T, 0)
	for name, node := range t.Nodes {
		if value, tagNameFound := tags[name]; tagNameFound {
			if tree, matchAllFound := node.Values[_matchall]; matchAllFound {
				// gather data from this node and recurse.
				data = append(data, node.Data...)
				data = append(data, match(tree, tags)...)
			}

			if tree, tagValueFound := node.Values[value]; tagValueFound {
				// gather data from this node and recurse.
				data = append(data, node.Data...)
				data = append(data, match(tree, tags)...)
			}
		}
	}

	return data
}

func TagsFromTagFilter(tf string) ([]Tag, error) {
	tagFilterMap, err := filters.ParseTagFilterValueMap(tf)
	if err != nil {
		return nil, err
	}

	tags := make([]Tag, 0, len(tagFilterMap))
	for name, value := range tagFilterMap {
		if value.Negate {
			return nil, errors.New("negation is not supported")
		}

		// parse values
		// They can be of the format:
		// - simpleValue
		// - {foo,bar,baz}
		// - {{.Variable}}
		// There cannot be a mix of value formats like "simpleValue, {foo,bar}" etc.
		tags = append(tags, Tag{Name: name, Values: []string{value.Pattern}})
	}

	return tags, nil
}

func parseTagValue(value string) ([]string, error) {
	value = strings.TrimSpace(value)
	if len(value) == 0 {
		return nil, errors.New("tag value cannot be empty")
	}

	if len(value) < 2 {
		return parseSimpleTagValue(value)
	}

	if value[0] == '{' {
		if value[1] == '{' {
			return parseVarTagValue(value)
		}

		return parseCompositeTagValue(value)
	}

	return parseSimpleTagValue(value)
}

func parseSimpleTagValue(value string) ([]string, error) {
	if strings.ContainsAny(value, "{},") {
		return nil, errors.New("invalid chars found in tag value")
	}

	return []string{value}, nil
}

func parseCompositeTagValue(value string) ([]string, error) {
	// must contain a single { and single }
	// one at the start and one at the end.
	if value[0] != '{' && value[len(value)-1] != '}' {
		return nil, errors.New("malformed composite tag value")
	}

	if len(value) <= 2 {
		return nil, errors.New("no values found in composite tag value")
	}

	// remove the leading and trailing curly braces.
	value = value[1 : len(value)-1]

	// if the remaining strings contain any curly braces then
	// error out.
	if strings.ContainsAny(value, "{}") {
		return nil, errors.New("malformed composite tag value")
	}

	values := strings.Split(value, ",")
	for i := range values {
		values[i] = strings.TrimSpace(values[i])
	}

	return values, nil
}

func parseVarTagValue(value string) ([]string, error) {
	if len(value) <= 4 {
		return nil, errors.New("malformed variable tag value")
	}

	if !strings.Contains(value, "{{") ||
		!strings.Contains(value, "}}") {
		return nil, errors.New("malformed variable tag value")
	}

	value = value[2 : len(value)-2]

	if len(value) == 0 {
		return nil, errors.New("cannot have empty variable tag value")
	}

	return parseSimpleTagValue(value)
}
