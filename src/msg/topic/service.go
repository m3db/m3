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

package topic

import (
	"errors"

	"github.com/m3db/m3cluster/kv"
)

var (
	defaultNamespace     = "/topic"
	errTopicNotAvailable = errors.New("topic is not available")
)

type service struct {
	store kv.Store
}

// NewService creates a topic service.
func NewService(sOpts ServiceOptions) (Service, error) {
	kvOpts := sanitizeKVOptions(sOpts.KVOverrideOptions())
	store, err := sOpts.ConfigService().Store(kvOpts)
	if err != nil {
		return nil, err
	}
	return &service{
		store: store,
	}, nil
}

func (s *service) Get(name string) (Topic, error) {
	value, err := s.store.Get(key(name))
	if err != nil {
		return nil, err
	}
	t, err := NewTopicFromValue(value)
	if err != nil {
		return nil, err
	}
	return t, nil
}

func (s *service) CheckAndSet(t Topic, version int) (Topic, error) {
	if err := t.Validate(); err != nil {
		return nil, err
	}
	pb, err := ToProto(t)
	if err != nil {
		return nil, err
	}
	version, err = s.store.CheckAndSet(key(t.Name()), version, pb)
	if err != nil {
		return nil, err
	}
	return t.SetVersion(version), nil
}

func (s *service) Delete(name string) error {
	_, err := s.store.Delete(key(name))
	return err
}

func (s *service) Watch(name string) (Watch, error) {
	w, err := s.store.Watch(key(name))
	if err != nil {
		return nil, err
	}
	return NewWatch(w), nil
}

func key(name string) string {
	return name
}

func sanitizeKVOptions(opts kv.OverrideOptions) kv.OverrideOptions {
	if opts.Namespace() == "" {
		opts = opts.SetNamespace(defaultNamespace)
	}
	return opts
}

// NewWatch creates a new topic watch.
func NewWatch(w kv.ValueWatch) Watch {
	return &watch{w}
}

type watch struct {
	kv.ValueWatch
}

func (w *watch) Get() (Topic, error) {
	value := w.ValueWatch.Get()
	if value == nil {
		return nil, errTopicNotAvailable
	}
	t, err := NewTopicFromValue(value)
	if err != nil {
		return nil, err
	}
	return t, nil
}
