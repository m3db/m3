// Copyright (c) 2018 Uber Technologies, Inc.
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

package topology

import (
	"errors"
	"fmt"
	"strings"
)

// ConsistencyLevel is the consistency level for cluster operations
type ConsistencyLevel int

// nolint: deadcode, varcheck, unused
const (
	consistencyLevelNone ConsistencyLevel = iota

	// ConsistencyLevelOne corresponds to a single node participating
	// for an operation to succeed
	ConsistencyLevelOne

	// ConsistencyLevelMajority corresponds to the majority of nodes participating
	// for an operation to succeed
	ConsistencyLevelMajority

	// ConsistencyLevelAll corresponds to all nodes participating
	// for an operation to succeed
	ConsistencyLevelAll
)

// String returns the consistency level as a string
func (l ConsistencyLevel) String() string {
	switch l {
	case consistencyLevelNone:
		return "none"
	case ConsistencyLevelOne:
		return "one"
	case ConsistencyLevelMajority:
		return "majority"
	case ConsistencyLevelAll:
		return "all"
	}
	return unknown
}

var validConsistencyLevels = []ConsistencyLevel{
	consistencyLevelNone,
	ConsistencyLevelOne,
	ConsistencyLevelMajority,
	ConsistencyLevelAll,
}

var errConsistencyLevelUnspecified = errors.New("consistency level not specified")

// UnmarshalYAML unmarshals an ConnectConsistencyLevel into a valid type from string.
func (l *ConsistencyLevel) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var str string
	if err := unmarshal(&str); err != nil {
		return err
	}
	if str == "" {
		return errConsistencyLevelUnspecified
	}
	strs := make([]string, len(validConsistencyLevels))
	for _, valid := range validConsistencyLevels {
		if str == valid.String() {
			*l = valid
			return nil
		}
		strs = append(strs, "'"+valid.String()+"'")
	}
	return fmt.Errorf("invalid ConsistencyLevel '%s' valid types are: %s",
		str, strings.Join(strs, ", "))
}

// ConnectConsistencyLevel is the consistency level for connecting to a cluster
type ConnectConsistencyLevel int

const (
	// ConnectConsistencyLevelAny corresponds to connecting to any number of nodes for a given shard
	// set, this strategy will attempt to connect to all, then the majority, then one and then none.
	ConnectConsistencyLevelAny ConnectConsistencyLevel = iota

	// ConnectConsistencyLevelNone corresponds to connecting to no nodes for a given shard set
	ConnectConsistencyLevelNone

	// ConnectConsistencyLevelOne corresponds to connecting to a single node for a given shard set
	ConnectConsistencyLevelOne

	// ConnectConsistencyLevelMajority corresponds to connecting to the majority of nodes for a given shard set
	ConnectConsistencyLevelMajority

	// ConnectConsistencyLevelAll corresponds to connecting to all of the nodes for a given shard set
	ConnectConsistencyLevelAll
)

// String returns the consistency level as a string
func (l ConnectConsistencyLevel) String() string {
	switch l {
	case ConnectConsistencyLevelAny:
		return "any"
	case ConnectConsistencyLevelNone:
		return "none"
	case ConnectConsistencyLevelOne:
		return "one"
	case ConnectConsistencyLevelMajority:
		return "majority"
	case ConnectConsistencyLevelAll:
		return "all"
	}
	return unknown
}

var validConnectConsistencyLevels = []ConnectConsistencyLevel{
	ConnectConsistencyLevelAny,
	ConnectConsistencyLevelNone,
	ConnectConsistencyLevelOne,
	ConnectConsistencyLevelMajority,
	ConnectConsistencyLevelAll,
}

var errClusterConnectConsistencyLevelUnspecified = errors.New("cluster connect consistency level not specified")

// UnmarshalYAML unmarshals an ConnectConsistencyLevel into a valid type from string.
func (l *ConnectConsistencyLevel) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var str string
	if err := unmarshal(&str); err != nil {
		return err
	}
	if str == "" {
		return errClusterConnectConsistencyLevelUnspecified
	}
	strs := make([]string, len(validConnectConsistencyLevels))
	for _, valid := range validConnectConsistencyLevels {
		if str == valid.String() {
			*l = valid
			return nil
		}
		strs = append(strs, "'"+valid.String()+"'")
	}
	return fmt.Errorf("invalid ConnectConsistencyLevel '%s' valid types are: %s",
		str, strings.Join(strs, ", "))
}

// ReadConsistencyLevel is the consistency level for reading from a cluster
type ReadConsistencyLevel int

const (
	// ReadConsistencyLevelNone corresponds to reading from no nodes
	ReadConsistencyLevelNone ReadConsistencyLevel = iota

	// ReadConsistencyLevelOne corresponds to reading from a single node
	ReadConsistencyLevelOne

	// ReadConsistencyLevelUnstrictMajority corresponds to reading from the majority of nodes
	// but relaxing the constraint when it cannot be met, falling back to returning success when
	// reading from at least a single node after attempting reading from the majority of nodes
	ReadConsistencyLevelUnstrictMajority

	// ReadConsistencyLevelMajority corresponds to reading from the majority of nodes
	ReadConsistencyLevelMajority

	// ReadConsistencyLevelAll corresponds to reading from all of the nodes
	ReadConsistencyLevelAll
)

// String returns the consistency level as a string
func (l ReadConsistencyLevel) String() string {
	switch l {
	case ReadConsistencyLevelNone:
		return "none"
	case ReadConsistencyLevelOne:
		return "one"
	case ReadConsistencyLevelUnstrictMajority:
		return "unstrict_majority"
	case ReadConsistencyLevelMajority:
		return "majority"
	case ReadConsistencyLevelAll:
		return "all"
	}
	return unknown
}

var validReadConsistencyLevels = []ReadConsistencyLevel{
	ReadConsistencyLevelNone,
	ReadConsistencyLevelOne,
	ReadConsistencyLevelUnstrictMajority,
	ReadConsistencyLevelMajority,
	ReadConsistencyLevelAll,
}

var errReadConsistencyLevelUnspecified = errors.New("read consistency level not specified")

// UnmarshalYAML unmarshals an ConnectConsistencyLevel into a valid type from string.
func (l *ReadConsistencyLevel) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var str string
	if err := unmarshal(&str); err != nil {
		return err
	}
	if str == "" {
		return errReadConsistencyLevelUnspecified
	}
	strs := make([]string, len(validReadConsistencyLevels))
	for _, valid := range validReadConsistencyLevels {
		if str == valid.String() {
			*l = valid
			return nil
		}
		strs = append(strs, "'"+valid.String()+"'")
	}
	return fmt.Errorf("invalid ReadConsistencyLevel '%s' valid types are: %s",
		str, strings.Join(strs, ", "))
}

// unknown string constant, required to fix lint complaining about
// multiple occurrences of same literal string...
const unknown = "unknown"
