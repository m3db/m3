package tagfiltertree

import (
	"fmt"
	"strings"
	"unsafe"

	"github.com/m3db/m3/src/metrics/filters"
)

const (
	_matchall  = "*"
	_matchNone = "!*"
)

type Tag struct {
	Name string
	Val  string
	Var  string
}

// Resolvable is an interface for types that can be stored in the tree.
type Resolvable[R any] interface {
	// Resolve resolves the data based on the tags and returns
	// the data with type R.
	Resolve(tags map[string]string) (R, error)
}

// Tree is a tree data structure for tag filters.
type Tree[T any] struct {
	Nodes []*node[T]
}

type NodeValue[T any] struct {
	Val    string
	Filter filters.Filter
	Tree   *Tree[T]
	Data   []T
}

type node[T any] struct {
	Name           string
	AbsoluteValues map[string]NodeValue[T]
	PatternValues  []NodeValue[T]
}

// New creates a new tree.
func New[T any]() *Tree[T] {
	return &Tree[T]{
		Nodes: make([]*node[T], 0),
	}
}

// AddTagFilter adds a tag filter to the tree.
func (t *Tree[T]) AddTagFilter(tagFilter string, data T) error {
	tags, err := TagsFromTagFilter(tagFilter)
	if err != nil {
		return err
	}
	return addNode(t, tags, 0, data)
}

func (n *node[T]) addValue(filter string, data *T) (*Tree[T], error) {
	if IsAbsoluteValue(filter) {
		if n.AbsoluteValues == nil {
			n.AbsoluteValues = make(map[string]NodeValue[T], 0)
		}
		if v, found := n.AbsoluteValues[filter]; found {
			if data != nil {
				if v.Data == nil {
					v.Data = make([]T, 0)
				}
				v.Data = append(v.Data, *data)
				n.AbsoluteValues[filter] = v
			}
			return v.Tree, nil
		}

		newNodeValue := NewNodeValue[T](filter, nil, data)
		n.AbsoluteValues[filter] = newNodeValue

		return newNodeValue.Tree, nil
	}

	if n.PatternValues == nil {
		n.PatternValues = make([]NodeValue[T], 0)
	}
	for i, v := range n.PatternValues {
		if v.Val == filter {
			if data != nil {
				if v.Data == nil {
					v.Data = make([]T, 0)
				}
				v.Data = append(v.Data, *data)
				n.PatternValues[i] = v
			}
			return v.Tree, nil
		}
	}

	f, err := filters.NewFilter([]byte(filter))
	if err != nil {
		return nil, err
	}

	newNodeValue := NewNodeValue[T](filter, f, data)
	n.PatternValues = append(n.PatternValues, newNodeValue)

	return newNodeValue.Tree, nil
}

func NewNodeValue[T any](val string, filter filters.Filter, data *T) NodeValue[T] {
	t := &Tree[T]{
		Nodes: make([]*node[T], 0),
	}

	v := NodeValue[T]{
		Val:    val,
		Filter: filter,
		Tree:   t,
		Data:   nil,
	}

	if data != nil {
		if v.Data == nil {
			v.Data = make([]T, 0)
		}
		v.Data = append(v.Data, *data)
	}

	return v
}

func addNode[T any](t *Tree[T], tags []Tag, idx int, data T) error {
	if idx >= len(tags) {
		return nil
	}

	tag := tags[idx]
	nodeIdx := -1
	for i, t := range t.Nodes {
		if t.Name == tag.Name {
			nodeIdx = i
			break
		}
	}
	if nodeIdx == -1 {
		t.Nodes = append(t.Nodes, &node[T]{
			Name: tag.Name,
		})
		nodeIdx = len(t.Nodes) - 1
	}
	node := t.Nodes[nodeIdx]

	// Add the data to the childTree if this is the last tag.
	var dataToAdd *T
	if idx == len(tags)-1 {
		dataToAdd = &data
	}

	childTree, err := node.addValue(tag.Val, dataToAdd)
	if err != nil {
		return err
	}

	// Recurse to the next tag.
	if err := addNode(childTree, tags, idx+1, data); err != nil {
		// TODO: perform cleanup to avoid partially added nodes.
		return err
	}

	return nil
}

// Match returns the data for the given tags.
func (t *Tree[T]) Match(tags map[string]string) ([]T, error) {
	data := make([]T, 0)
	if err := match(t, tags, &data); err != nil {
		return nil, err
	}
	return data, nil
}

func match[T any](
	t *Tree[T],
	tags map[string]string,
	data *[]T,
) error {
	if len(tags) == 0 || t == nil {
		return nil
	}

	for _, node := range t.Nodes {
		name := node.Name
		negate := false
		if IsMatchNoneTag(name) {
			name = name[1:]
			negate = true
		}
		tagValue, tagNameFound := tags[name]
		absVal, absValFound := node.AbsoluteValues[tagValue]
		if tagNameFound && absValFound {
			*data = append(*data, absVal.Data...)
			if err := match(absVal.Tree, tags, data); err != nil {
				return err
			}
		}
		if tagNameFound != negate {
			for _, v := range node.PatternValues {
				d := unsafe.StringData(tagValue)
				b := unsafe.Slice(d, len(tagValue))
				if v.Filter.Matches(b) {
					*data = append(*data, v.Data...)
					if err := match(v.Tree, tags, data); err != nil {
						return err
					}
				}
			}
		}
	}

	return nil
}

// TagsFromTagFilter creates tags from a tag filter.
// The tag values can be of the format:
// "foo" OR "{foo,bar,baz}" OR "{{Variable}}"
// There cannot be a mix of value formats like "simpleValue, {foo,bar}" etc.
func TagsFromTagFilter(tf string) ([]Tag, error) {
	tags := make([]Tag, 0)
	tfSanitized := strings.TrimSpace(tf)
	tagValuePairs := strings.Split(tfSanitized, " ")
	for _, tagValuePair := range tagValuePairs {
		tagAndValue := strings.Split(tagValuePair, ":")
		if len(tagAndValue) != 2 {
			return nil, fmt.Errorf("invalid tag filter: %s", tf)
		}
		tag := tagAndValue[0]
		val := tagAndValue[1]
		if len(tag) == 0 || len(val) == 0 {
			return nil, fmt.Errorf("invalid tag filter: %s", tf)
		}

		varName := ""
		if IsVarTagValue(val) {
			varName = val
			val = _matchall
		}
		if val == _matchNone {
			tag = "!" + tag
			val = _matchall
		}

		tags = append(tags, Tag{
			Name: tag,
			Val:  val,
			Var:  varName,
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

func IsAbsoluteValue(val string) bool {
	return !strings.ContainsAny(val, "{!*")
}

/*
tag1:value1 tag2:value2 tag3:value3
tag1:foo1 tag4:* tag8:val*

tag1 -> value1 ---->
			tag2 -> value2 ---->
						tag3 -> value3 D-> R1
        foo1 ---->
			tag4 -> * ---->
						tag8 -> val* D-> R2

input:
	tag1:foo1 tag4:foobar tag8:value8
*/
