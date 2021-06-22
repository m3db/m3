// Copyright (c) 2021 Uber Technologies, Inc.
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

package placement

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/m3db/m3/src/cluster/generated/proto/placementpb"
	"github.com/m3db/m3/src/cluster/shard"
)

func TestMarshalAndUnmarshalPlacementProto(t *testing.T) {
	b, err := testLatestPlacementProto.Marshal()
	require.NoError(t, err)

	actual := &placementpb.Placement{}
	err = proto.Unmarshal(b, actual)
	require.NoError(t, err)
	require.Equal(t, testLatestPlacementProto.String(), actual.String())
}

func TestCompressAndDecompressPlacementProto(t *testing.T) {
	t.Run("nil input", func(t *testing.T) {
		_, err := compressPlacementProto(nil)
		require.Equal(t, errNilPlacementSnapshotsProto, err)

		_, err = decompressPlacementProto(nil)
		require.Equal(t, errNilValue, err)
	})

	t.Run("compress roundtrip", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		var wg sync.WaitGroup

		for i := 10; i < 100; i++ {
			wg.Add(1)
			i := i
			go func() {
				defer wg.Done()
				pl, err := testRandPlacement(50, i).Proto()
				require.NoError(t, err)

				compressed, err := compressPlacementProto(pl)
				require.NoError(t, err)

				actual, err := decompressPlacementProto(compressed)
				require.NoError(t, err)
				require.Equal(t, pl.String(), actual.String())
			}()
		}

		wg.Wait()
	})

	t.Run("compress roundtrip, invalid", func(t *testing.T) {
		var wg sync.WaitGroup

		for i := 2; i < 50; i++ {
			wg.Add(1)
			i := i
			go func() {
				defer wg.Done()
				pl, err := testRandPlacement(50+i, i).Proto()
				require.NoError(t, err)

				compressed, err := compressPlacementProto(pl)
				require.NoError(t, err)

				// corrupt placement by trimming the last few bytes
				l := len(compressed) - (len(compressed) / i)
				actual, err := decompressPlacementProto(compressed[:l])
				require.Error(t, err)
				require.NotEqual(t, pl.String(), actual.String())
			}()
		}

		wg.Wait()
	})
}

func BenchmarkCompressPlacementProto(b *testing.B) {
	p := testBigRandPlacement()
	ps, err := NewPlacementsFromLatest(p)
	require.NoError(b, err)

	proto, err := ps.Latest().Proto()
	require.NoError(b, err)

	totalCompressedBytes := 0
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		compressed, err := compressPlacementProto(proto)
		if err != nil {
			b.FailNow()
		}

		totalCompressedBytes += len(compressed)
	}

	uncompressed, err := proto.Marshal()
	require.NoError(b, err)

	avgCompressedBytes := float64(totalCompressedBytes) / float64(b.N)
	avgRate := float64(len(uncompressed)) / avgCompressedBytes
	b.ReportMetric(avgRate, "compress_rate/op")
}

func BenchmarkDecompressPlacementProto(b *testing.B) {
	ps, err := NewPlacementsFromLatest(testBigRandPlacement())
	require.NoError(b, err)

	proto, err := ps.Latest().Proto()
	require.NoError(b, err)

	compressed, err := compressPlacementProto(proto)
	if err != nil {
		b.FailNow()
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_, err := decompressPlacementProto(compressed)
		if err != nil {
			b.FailNow()
		}
	}
}

func testBigRandPlacement() Placement {
	numInstances := 2000
	numShardsPerInstance := 20
	return testRandPlacement(numInstances, numShardsPerInstance)
}

func testRandPlacement(numInstances, numShardsPerInstance int) Placement {
	instances := make([]Instance, numInstances)
	var shardID uint32
	for i := 0; i < numInstances; i++ {
		instances[i] = testRandInstance()
		shards := make([]shard.Shard, numShardsPerInstance)
		for j := 0; j < numShardsPerInstance; j++ {
			shardID++
			shards[j] = shard.NewShard(shardID).
				SetState(shard.Available).
				SetCutoverNanos(0)
		}
		instances[i].SetShards(shard.NewShards(shards))
	}

	return NewPlacement().SetInstances(instances)
}

func testRandInstance() Instance {
	id := "prod-cluster-zone-" + randStringBytes(10)
	host := "host" + randStringBytes(4) + "-zone"
	port := rand.Intn(1000) // nolint:gosec
	endpoint := fmt.Sprintf("%s:%d", host, port)
	return NewInstance().
		SetID(id).
		SetHostname(host).
		SetIsolationGroup(host).
		SetPort(uint32(port)).
		SetEndpoint(endpoint).
		SetMetadata(InstanceMetadata{DebugPort: 80}).
		SetZone("zone").
		SetWeight(1)
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func randStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))] // nolint:gosec
	}
	return string(b)
}
