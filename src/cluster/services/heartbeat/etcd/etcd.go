// Copyright (c) 2016 Uber Technologies, Inc.
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

package etcd

import (
	"fmt"
	"strings"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/m3db/m3cluster/services/heartbeat"
	"golang.org/x/net/context"
)

const (
	heartbeatKeyPrefix = "_hb"
	keySeparator       = "/"
	keyFormat          = "%s/%s"
)

// NewStore creates a heartbeat store based on etcd
func NewStore(c *clientv3.Client) heartbeat.Store {
	return &client{l: c.Lease, kv: c.KV}
}

type client struct {
	l  clientv3.Lease
	kv clientv3.KV
}

func (c *client) Heartbeat(service, id string, ttl time.Duration) error {
	resp, err := c.l.Grant(context.Background(), int64(ttl/time.Second))
	if err != nil {
		return err
	}

	_, err = c.kv.Put(context.Background(), heartbeatKey(service, id), "", clientv3.WithLease(resp.ID))
	return err
}

func (c *client) Get(service string) ([]string, error) {
	gr, err := c.kv.Get(context.Background(), servicePrefix(service), clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	r := make([]string, len(gr.Kvs))
	for i, kv := range gr.Kvs {
		r[i] = instanceID(string(kv.Key), service)
	}
	return r, nil
}

func heartbeatKey(service, id string) string {
	return fmt.Sprintf(keyFormat, servicePrefix(service), id)
}

func instanceID(key, service string) string {
	return strings.TrimPrefix(
		strings.TrimPrefix(key, servicePrefix(service)),
		keySeparator,
	)
}

func servicePrefix(service string) string {
	return fmt.Sprintf(keyFormat, heartbeatKeyPrefix, service)
}
