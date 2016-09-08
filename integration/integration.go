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

package integration

import (
	"fmt"
	"testing"
	"time"

	"github.com/m3db/m3db/client"
	"github.com/m3db/m3db/encoding"
	"github.com/m3db/m3db/instrument"
	"github.com/m3db/m3db/persist/fs"
	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/sharding"
	"github.com/m3db/m3db/storage"
	"github.com/m3db/m3db/topology"
	"github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

const (
	multiAddrPortStart = 9000
	multiAddrPortEach  = 4
)

type conditionFn func() bool

func waitUntil(fn conditionFn, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return true
		}
		time.Sleep(time.Second)
	}
	return false
}

func multiAddrTestOptions(opts testOptions, instance int) testOptions {
	bind := "0.0.0.0"
	start := multiAddrPortStart + (instance * multiAddrPortEach)
	return opts.
		SetTChannelNodeAddr(fmt.Sprintf("%s:%d", bind, start)).
		SetTChannelClusterAddr(fmt.Sprintf("%s:%d", bind, start+1)).
		SetHTTPNodeAddr(fmt.Sprintf("%s:%d", bind, start+2)).
		SetHTTPClusterAddr(fmt.Sprintf("%s:%d", bind, start+3))
}

func newMultiAddrAdminSession(
	t *testing.T,
	instrumentOpts instrument.Options,
	shardSet sharding.ShardSet,
	replicas int,
	instance int,
) client.AdminSession {
	var (
		start         = multiAddrPortStart
		hostShardSets []topology.HostShardSet
		origin        topology.Host
	)
	for i := 0; i < replicas; i++ {
		id := fmt.Sprintf("localhost%d", i)
		nodeAddr := fmt.Sprintf("127.0.0.1:%d", start+(i*multiAddrPortEach))
		host := topology.NewHost(id, nodeAddr)
		if i == instance {
			origin = host
		}
		hostShardSet := topology.NewHostShardSet(host, shardSet)
		hostShardSets = append(hostShardSets, hostShardSet)
	}

	staticOptions := topology.NewStaticOptions().
		SetShardSet(shardSet).
		SetReplicas(replicas).
		SetHostShardSets(hostShardSets)

	opts := client.NewAdminOptions().
		SetOrigin(origin).
		SetInstrumentOptions(instrumentOpts).
		SetTopologyInitializer(topology.NewStaticInitializer(staticOptions)).
		SetClusterConnectConsistencyLevel(client.ConnectConsistencyLevelAny).
		SetClusterConnectTimeout(time.Second)

	adminClient, err := client.NewAdminClient(opts.(client.AdminOptions))
	require.NoError(t, err)

	session, err := adminClient.NewAdminSession()
	require.NoError(t, err)

	return session
}

func newBootstrappableTestSetup(
	t *testing.T,
	opts testOptions,
	retentionOpts retention.Options,
	newBootstrapFn storage.NewBootstrapFn,
) *testSetup {
	setup, err := newTestSetup(opts)
	require.NoError(t, err)

	setup.storageOpts = setup.storageOpts.SetRetentionOptions(retentionOpts)
	setup.storageOpts = setup.storageOpts.SetNewBootstrapFn(newBootstrapFn)

	return setup
}

type testData struct {
	ids       []string
	numPoints int
	start     time.Time
}

func writeTestDataToDisk(
	namespace string,
	testSetup *testSetup,
	input []testData,
) (map[time.Time]seriesList, error) {
	storageOpts := testSetup.storageOpts
	fsOpts := storageOpts.CommitLogOptions().FilesystemOptions()
	writerBufferSize := fsOpts.WriterBufferSize()
	blockSize := storageOpts.RetentionOptions().BlockSize()
	filePathPrefix := fsOpts.FilePathPrefix()
	newFileMode := fsOpts.NewFileMode()
	newDirectoryMode := fsOpts.NewDirectoryMode()
	writer := fs.NewWriter(blockSize, filePathPrefix, writerBufferSize, newFileMode, newDirectoryMode)
	encoder := storageOpts.EncoderPool().Get()

	seriesMaps := make(map[time.Time]seriesList)
	for _, data := range input {
		generated := generateTestData(data.ids, data.numPoints, data.start)
		seriesMaps[data.start] = generated

		err := writeToDisk(writer, testSetup.shardSet, encoder, data.start, namespace, generated)
		if err != nil {
			return nil, err
		}
	}

	return seriesMaps, nil
}

func writeToDisk(
	writer fs.FileSetWriter,
	shardSet sharding.ShardSet,
	encoder encoding.Encoder,
	start time.Time,
	namespace string,
	seriesList seriesList,
) error {
	seriesPerShard := make(map[uint32][]series)
	for _, s := range seriesList {
		shard := shardSet.Shard(s.id)
		seriesPerShard[shard] = append(seriesPerShard[shard], s)
	}
	segmentHolder := make([][]byte, 2)
	for shard, seriesList := range seriesPerShard {
		if err := writer.Open(namespace, shard, start); err != nil {
			return err
		}
		for _, series := range seriesList {
			encoder.Reset(start, 0)
			for _, dp := range series.data {
				if err := encoder.Encode(dp, xtime.Second, nil); err != nil {
					return err
				}
			}
			segment := encoder.Stream().Segment()
			segmentHolder[0] = segment.Head
			segmentHolder[1] = segment.Tail
			if err := writer.WriteAll(series.id, segmentHolder); err != nil {
				return err
			}
		}
		if err := writer.Close(); err != nil {
			return err
		}
	}

	return nil
}
