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

// +build big

package main_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3x/ident"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	hostID                 = "m3dbtest01"
	serviceName            = "m3dbnode_test"
	serviceEnv             = "test"
	serviceZone            = "local"
	servicePort            = 9000
	namespaceID            = "metrics"
	lpURL                  = "http://0.0.0.0:2380"
	lcURL                  = "http://0.0.0.0:2379"
	apURL                  = "http://localhost:2380"
	acURL                  = "http://localhost:2379"
	etcdEndpoint           = acURL
	initialClusterHostID   = hostID
	initialClusterEndpoint = apURL
)

type cleanup func()

func tempFile(t *testing.T, name string) (*os.File, cleanup) {
	fd, err := ioutil.TempFile("", name)
	require.NoError(t, err)

	fname := fd.Name()
	return fd, func() {
		assert.NoError(t, fd.Close())
		assert.NoError(t, os.Remove(fname))
	}
}

func tempFileTouch(t *testing.T, name string) (string, cleanup) {
	fd, err := ioutil.TempFile("", name)
	require.NoError(t, err)

	fname := fd.Name()
	require.NoError(t, fd.Close())
	return fname, func() {
		assert.NoError(t, os.Remove(fname))
	}
}

func tempDir(t *testing.T, name string) (string, cleanup) {
	dir, err := ioutil.TempDir("", name)
	require.NoError(t, err)
	return dir, func() {
		assert.NoError(t, os.RemoveAll(dir))
	}
}

// yamlArray returns a JSON array which is valid YAML, hehe..
func yamlArray(t *testing.T, values []string) string {
	buff := bytes.NewBuffer(nil)
	err := json.NewEncoder(buff).Encode(values)
	require.NoError(t, err)
	return strings.TrimSpace(buff.String())
}

func endpoint(ip string, port uint32) string {
	return fmt.Sprintf("%s:%d", ip, port)
}

func newNamespaceProtoValue(id string) (proto.Message, error) {
	md, err := namespace.NewMetadata(
		ident.StringID(id),
		namespace.NewOptions().
			SetNeedsBootstrap(true).
			SetNeedsFilesetCleanup(true).
			SetNeedsFlush(true).
			SetNeedsRepair(true).
			SetWritesToCommitLog(true).
			SetRetentionOptions(
				retention.NewOptions().
					SetBlockSize(1*time.Hour).
					SetRetentionPeriod(24*time.Hour)))
	if err != nil {
		return nil, err
	}
	nsMap, err := namespace.NewMap([]namespace.Metadata{md})
	if err != nil {
		return nil, err
	}
	return namespace.ToProto(nsMap), nil
}
