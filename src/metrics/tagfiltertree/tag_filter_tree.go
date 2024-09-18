package tagfiltertree

import (
	"strings"

	"github.com/m3db/m3/src/metrics/filters"
)

const (
	_matchall  = "*"
	_matchNone = "!*"
)

type Tag struct {
	Name  string
	Value filters.Filter
	Var   string
}

// Resolvable is an interface for types that can be stored in the tree.
type Resolvable[R any] interface {
	// Resolve resolves the data based on the tags and returns
	// the data with type R.
	Resolve(tags map[string]string) (R, error)
}

// Tree is a tree data structure for tag filters.
type Tree[T Resolvable[R], R any] struct {
	Nodes map[string]*node[T, R]
}

type node[T Resolvable[R], R any] struct {
	Name string
	// key=tagValue
	Values map[filters.Filter]*Tree[T, R]
	Data   []T
}

// New creates a new tree.
func New[T Resolvable[R], R any]() *Tree[T, R] {
	return &Tree[T, R]{
		Nodes: make(map[string]*node[T, R]),
	}
}

// AddTagFilter adds a tag filter to the tree.
func (t *Tree[T, R]) AddTagFilter(tagFilter string, data T) error {
	tags, err := TagsFromTagFilter(tagFilter)
	if err != nil {
		return err
	}
	return addNode(t, tags, 0, data)
}

func (n *node[T, R]) addValue(value filters.Filter) (*Tree[T, R], error) {
	if _, ok := n.Values[value]; !ok {
		n.Values[value] = &Tree[T, R]{
			Nodes: make(map[string]*node[T, R]),
		}
	}
	return n.Values[value], nil
}

func addNode[T Resolvable[R], R any](t *Tree[T, R], tags []Tag, idx int, data T) error {
	if idx >= len(tags) {
		return nil
	}

	tag := tags[idx]
	if _, ok := t.Nodes[tag.Name]; !ok {
		t.Nodes[tag.Name] = &node[T, R]{
			Name:   tag.Name,
			Values: make(map[filters.Filter]*Tree[T, R]),
			Data:   make([]T, 0),
		}
	}
	node := t.Nodes[tag.Name]
	childTree, err := node.addValue(tag.Value)
	if err != nil {
		return err
	}

	// Add the data to the childTree if this is the last tag.
	if idx == len(tags)-1 {
		node.Data = append(node.Data, data)
	}

	// Recurse to the next tag.
	if err := addNode(childTree, tags, idx+1, data); err != nil {
		// TODO: perform cleanup to avoid partially added nodes.
		return err
	}

	return nil
}

// Match returns the data for the given tags.
func (t *Tree[T, R]) Match(tags map[string]string) []R {
	return match(t, tags)
}

func match[T Resolvable[R], R any](
	t *Tree[T, R],
	tags map[string]string,
) []R {
	if len(tags) == 0 || t == nil {
		return nil
	}

	data := make([]R, 0)
	for name, node := range t.Nodes {
		negate := false
		if IsMatchNoneTag(name) {
			name = name[1:]
			negate = true
		}
		tagValue, tagNameFound := tags[name]
		if tagNameFound != negate {
			// for each of the nodes values, recurse if:
			// - the tag value matches the node value
			// - the node value is a variable
			// - the node value is a matchall
			for nodeValue, subTree := range node.Values {
				if nodeValue.Matches([]byte(tagValue)) {
					for _, d := range node.Data {
						resolvedData, err := d.Resolve(tags)
						if err != nil {
							// TODO: trickle up the error.
							continue
						}
						data = append(data, resolvedData)
					}
					data = append(data, match(subTree, tags)...)
				}
			}
		}
	}

	return data
}

// TagsFromTagFilter creates tags from a tag filter.
// The tag values can be of the format:
// "foo" OR "{foo,bar,baz}" OR "{{Variable}}"
// There cannot be a mix of value formats like "simpleValue, {foo,bar}" etc.
func TagsFromTagFilter(tf string) ([]Tag, error) {
	tagFilterMap, err := filters.ParseTagFilterValueMap(tf)
	if err != nil {
		return nil, err
	}

	tags := make([]Tag, 0, len(tagFilterMap))
	for name, value := range tagFilterMap {
		varName := ""
		if IsVarTagValue(value.Pattern) {
			varName = value.Pattern
			value = filters.FilterValue{
				Pattern: _matchall,
			}
		}
		if value.Pattern == _matchNone {
			name = "!" + name
			value = filters.FilterValue{
				Pattern: _matchall,
			}
		}
		valueFilter, err := filters.NewFilterFromFilterValue(value)
		if err != nil {
			return nil, err
		}

		tags = append(tags, Tag{
			Name:  name,
			Value: valueFilter,
			Var:   varName,
		})
	}

	return tags, nil
}

func IsVarTagValue(value string) bool {
	if len(value) < 4 {
		return false
	}

	for i := 0; i < len(value); i++ {
		if value[i] == '{' {
			if i+1 < len(value) && value[i+1] == '{' {
				if strings.Contains(value[i+1:], "}}") {
					return true
				}
			}
		}
	}

	return false
}

func IsMatchNoneTag(tagName string) bool {
	if len(tagName) == 0 {
		return false
	}
	return tagName[0] == '!'
}
