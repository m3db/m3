package tagfiltertree

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/influxdb/pkg/testing/assert"
)

const (
	_numFilters = 1000000
)

// Predefined tag names and values for benchmarks
var tags = []string{"tag1", "tag2", "tag3", "tag4", "tag5", "tag6", "tag7", "tag8", "tag9", "tag10"}
var values = []string{"value1", "value2", "value3", "value4", "value5"}

// Filter Types
const (
	ABSOLUTE  = 0
	WILDCARD  = 1
	NEGATION  = 2
	COMPOSITE = 3
)

func BenchmarkTagFilterTreeMatch(b *testing.B) {
	// Generate a benchmark set of tag filters with varying complexity
	filters := make([]string, 0)
	for i := 0; i < _numFilters; i++ {
		filters = append(filters, generateTagFilter(1+rnd.Intn(len(tags)-1))) // 10 filters in the string
	}
	input := map[string]string{
		"tag1": "value1",
		"tag2": "value2",
		"tag3": "value3",
		"tag4": "value4",
		"tag5": "value5",
	}
	tree := New[*Rule]()
	data := &Rule{}

	for _, f := range filters {
		err := tree.AddTagFilter(f, data)
		assert.NoError(b, err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = tree.Match(input)
	}
}

// Seeded random number generator for deterministic results
var rnd = rand.New(rand.NewSource(time.Now().UnixNano()))

// Generate a single random tag filter based on type
func generateTagValuePair(idx int, tagType int) string {
	tag := tags[idx]
	switch tagType {
	case ABSOLUTE:
		value := values[rnd.Intn(len(values))]
		return fmt.Sprintf("%s:%s", tag, value)
	case WILDCARD:
		return fmt.Sprintf("%s:*", tag)
	case NEGATION:
		if rnd.Intn(2) == 0 {
			return fmt.Sprintf("%s:!*", tag) // tag should not exist
		} else {
			value := values[rnd.Intn(len(values))]
			return fmt.Sprintf("%s:!%s", tag, value) // tag should not have this value
		}
	case COMPOSITE:
		value1 := values[rnd.Intn(len(values))]
		value2 := values[rnd.Intn(len(values))]
		return fmt.Sprintf("%s:{%s,%s}", tag, value1, value2) // matches either value1 or value2
	default:
		return ""
	}
}

// Generate a tag filter with the specified number of tags.
func generateTagFilter(numTags int) string {
	var filter []string

	idxs := rnd.Perm(len(tags))
	for i := 0; i < numTags; i++ {
		tagType := rnd.Intn(4) // Randomly choose from ABSOLUTE, WILDCARD, NEGATION, COMPOSITE
		pair := generateTagValuePair(idxs[i], tagType)
		filter = append(filter, pair)
	}
	return strings.Join(filter, " ")
}
