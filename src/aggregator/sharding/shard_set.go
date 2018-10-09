// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package sharding

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
)

const (
	defaultNumShards = 1024
)

var (
	// Shard range is expected to be provided in the form of startShard..endShard.
	// Both startShard and endShard are inclusive. An example shard range is 0..63.
	rangeRegexp = regexp.MustCompile(`^([0-9]+)(\.\.([0-9]+))?$`)

	errInvalidShard = errors.New("invalid shard")
)

// ParseShardSet parses a shard set from the input string.
func ParseShardSet(s string) (ShardSet, error) {
	ss := make(ShardSet, defaultNumShards)
	if err := ss.ParseRange(s); err != nil {
		return nil, err
	}
	return ss, nil
}

// MustParseShardSet parses a shard set from the input string, and panics
// if parsing is unsuccessful.
func MustParseShardSet(s string) ShardSet {
	ss, err := ParseShardSet(s)
	if err == nil {
		return ss
	}
	panic(fmt.Errorf("unable to parse shard set from %s: %v", s, err))
}

// ShardSet is a collection of shards organized as a set.
// The shards contained in the set can be discontinuous.
type ShardSet map[uint32]struct{}

// UnmarshalYAML unmarshals YAML into a shard set.
// The following formats are supported:
// * StartShard..EndShard, e.g., 0..63.
// * Single shard, e.g., 5.
// * Array containing shard ranges and single shards.
func (ss *ShardSet) UnmarshalYAML(f func(interface{}) error) error {
	*ss = make(ShardSet, defaultNumShards)

	// If YAML contains a single string, attempt to parse out a single range.
	var s string
	if err := f(&s); err == nil {
		return ss.ParseRange(s)
	}

	// Otherwise try to parse out a list of ranges or single shards.
	var a []interface{}
	if err := f(&a); err == nil {
		for _, v := range a {
			switch c := v.(type) {
			case string:
				if err := ss.ParseRange(c); err != nil {
					return err
				}
			case int:
				ss.Add(uint32(c))
			default:
				return fmt.Errorf("unexpected range %v", c)
			}
		}
		return nil
	}

	// Otherwise try to parse out a single shard.
	var n int
	if err := f(&n); err == nil {
		ss.Add(uint32(n))
		return nil
	}

	return errInvalidShard
}

// Contains returns true if the shard set contains the given shard.
func (ss ShardSet) Contains(p uint32) bool {
	_, found := ss[p]
	return found
}

// Add adds the shard to the set.
func (ss ShardSet) Add(p uint32) {
	ss[p] = struct{}{}
}

// AddBetween adds shards between the given min (inclusive) and max (exclusive).
func (ss ShardSet) AddBetween(minInclusive, maxExclusive uint32) {
	for i := minInclusive; i < maxExclusive; i++ {
		ss.Add(i)
	}
}

// ParseRange parses a range of shards and adds them to the set.
func (ss ShardSet) ParseRange(s string) error {
	rangeMatches := rangeRegexp.FindStringSubmatch(s)
	if len(rangeMatches) != 0 {
		return ss.addRange(rangeMatches)
	}

	return fmt.Errorf("invalid range '%s'", s)
}

func (ss ShardSet) addRange(matches []string) error {
	min, err := strconv.ParseInt(matches[1], 10, 32)
	if err != nil {
		return err
	}

	max := min
	if matches[3] != "" {
		max, err = strconv.ParseInt(matches[3], 10, 32)
		if err != nil {
			return err
		}
	}

	if min > max {
		return fmt.Errorf("invalid range: %d > %d", min, max)
	}

	ss.AddBetween(uint32(min), uint32(max)+1)
	return nil
}
