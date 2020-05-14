package harness

import (
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/generated/thrift/rpc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type dp struct {
	t time.Time
	v float64
}

func writeReq(ns, id string, dp dp) rpc.WriteRequest {
	return rpc.WriteRequest{
		NameSpace: ns,
		ID:        id,
		Datapoint: &rpc.Datapoint{
			Timestamp: dp.t.Unix(),
			Value:     dp.v,
		},
	}
}

func fetchReq(ns, id string) rpc.FetchRequest {
	return rpc.FetchRequest{
		NameSpace:  ns,
		ID:         id,
		RangeStart: 0,
		RangeEnd:   time.Now().Unix(),
	}
}

func ago(mins time.Duration) time.Time {
	return time.Now().Add(time.Minute * -mins)
}

func verifyFetch(t *testing.T, res rpc.FetchResult_, exDps ...dp) {
	dps := res.GetDatapoints()
	require.Equal(t, len(dps), len(exDps))

	for i, dp := range exDps {
		other := dps[i]
		assert.Equal(t, dp.t.Unix(), other.GetTimestamp())
		assert.Equal(t, dp.v, other.GetValue())
	}
}

func TestHarness(t *testing.T) {
	dockerResources, err := setupSingleM3DBNode()
	require.NoError(t, err)

	defer dockerResources.cleanup()
	require.Equal(t, 1, len(dockerResources.nodes))
	node := dockerResources.nodes[0]

	warmDp := dp{t: ago(20), v: 12.3456789}
	req := writeReq(coldWriteNsName, "foo", warmDp)
	require.NoError(t, node.WritePoint(req))

	fetch, err := node.Fetch(fetchReq(coldWriteNsName, "foo"))
	require.NoError(t, err)
	verifyFetch(t, fetch, warmDp)

	coldDp := dp{t: ago(120), v: 98.7654321}
	req = writeReq(coldWriteNsName, "foo", coldDp)
	require.NoError(t, node.WritePoint(req))

	fetch, err = node.Fetch(fetchReq(coldWriteNsName, "foo"))
	require.NoError(t, err)
	verifyFetch(t, fetch, coldDp, warmDp)

	err = node.CheckForCheckpoint()
	assert.NoError(t, err)

	err = node.Restart()
	require.NoError(t, err)

	err = node.WaitForBootstrap()
	require.NoError(t, err)

	fetch, err = node.Fetch(fetchReq(coldWriteNsName, "foo"))
	require.NoError(t, err)
	verifyFetch(t, fetch, coldDp, warmDp)
}
