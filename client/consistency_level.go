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

package client

import (
	"github.com/m3db/m3db/topology"
)

// queryableReadConsistencyLevel is a queryable value for a
// read consistency level, this supports it being able to change
// dynamically or it can be just static if not required to be changed
// during an operation.
type queryableReadConsistencyLevel interface {
	value() topology.ReadConsistencyLevel
}

type staticQueryableReadConsistencyLevel struct {
	val topology.ReadConsistencyLevel
}

func newStaticQueryableReadConsistencyLevel(
	value topology.ReadConsistencyLevel,
) queryableReadConsistencyLevel {
	return staticQueryableReadConsistencyLevel{val: value}
}

func (l staticQueryableReadConsistencyLevel) value() topology.ReadConsistencyLevel {
	return l.val
}

// nolint: unused
type sessionReadQueryableReadConsistencyLevel struct {
	s *session
}

// nolint: deadcode,unused
func newSessionReadQueryableReadConsistencyLevel(
	s *session,
) queryableReadConsistencyLevel {
	return sessionReadQueryableReadConsistencyLevel{s: s}
}

func (l sessionReadQueryableReadConsistencyLevel) value() topology.ReadConsistencyLevel {
	l.s.state.RLock()
	value := l.s.state.readLevel
	l.s.state.RUnlock()
	return value
}

type sessionBootstrapQueryableReadConsistencyLevel struct {
	s *session
}

func newSessionBootstrapQueryableReadConsistencyLevel(
	s *session,
) queryableReadConsistencyLevel {
	return sessionBootstrapQueryableReadConsistencyLevel{s: s}
}

func (l sessionBootstrapQueryableReadConsistencyLevel) value() topology.ReadConsistencyLevel {
	l.s.state.RLock()
	value := l.s.state.bootstrapLevel
	l.s.state.RUnlock()
	return value
}
