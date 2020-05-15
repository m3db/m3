package harness

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
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

func writeReq(ns, id string, dp dp) *rpc.WriteRequest {
	return &rpc.WriteRequest{
		NameSpace: ns,
		ID:        id,
		Datapoint: &rpc.Datapoint{
			Timestamp: dp.t.Unix(),
			Value:     dp.v,
		},
	}
}

func fetchReq(ns, id string) *rpc.FetchRequest {
	return &rpc.FetchRequest{
		NameSpace:  ns,
		ID:         id,
		RangeStart: 0,
		RangeEnd:   time.Now().Unix(),
	}
}

func ago(mins time.Duration) time.Time {
	return time.Now().Add(time.Minute * -mins)
}

func verifyFetch(t *testing.T, res *rpc.FetchResult_, exDps ...dp) {
	dps := res.GetDatapoints()
	require.Equal(t, len(dps), len(exDps))

	for i, dp := range exDps {
		other := dps[i]
		assert.Equal(t, dp.t.Unix(), other.GetTimestamp())
		assert.Equal(t, dp.v, other.GetValue())
	}
}

func hasFileVerifier(filter string) GoalStateVerifier {
	return func(out string, err error) error {
		if err != nil {
			return err
		}

		if len(filter) == 0 {
			return nil
		}

		re := regexp.MustCompile(filter)
		lines := strings.Split(out, "\n")
		for _, line := range lines {
			if re.MatchString(line) {
				return nil
			}
		}

		return errors.New("no matches")
	}
}

var singleDBNodeDockerResources *dockerResources

func TestMain(m *testing.M) {
	var err error
	singleDBNodeDockerResources, err = setupSingleM3DBNode()

	if err != nil {
		if singleDBNodeDockerResources.cleanup != nil {
			singleDBNodeDockerResources.cleanup()
		}

		fmt.Println("could not set up db docker containers", err)
		os.Exit(1)
	}

	if l := len(singleDBNodeDockerResources.nodes); l != 1 {
		singleDBNodeDockerResources.cleanup()
		fmt.Println("should only have a single node, have", l)
		os.Exit(1)
	}

	code := m.Run()
	// singleDBNodeDockerResources.cleanup()
	os.Exit(code)
}

func renderVerifier() GoalStateVerifier {
	return func(s string, err error) error {
		if err != nil {
			fmt.Println("Got err", err)
			return err
		}

		fmt.Println("Got s", s)
		return fmt.Errorf(s)
	}
}

func graphiteQuery(target string, start time.Time) string {
	from := start.Add(time.Minute * -5).Unix()
	until := start.Add(time.Minute * 5).Unix()
	return fmt.Sprintf("api/v1/graphite/render?target=%s&from=%d&until=%d",
		target, from, until)
}

func TestCarbon(t *testing.T) {
	coord := singleDBNodeDockerResources.coordinator

	aggMetric := "foo.min.aggregate.baz"
	timestamp := time.Now()
	assert.NoError(t, coord.WriteCarbon(7204, aggMetric, 41, timestamp))
	assert.NoError(t, coord.WriteCarbon(7204, aggMetric, 42, timestamp))
	assert.NoError(t, coord.WriteCarbon(7204, aggMetric, 40, timestamp))
	time.Sleep(time.Minute)
	err := coord.RunQuery(renderVerifier(), graphiteQuery(aggMetric, timestamp))
	assert.NoError(t, err)
}

func testColdWritesSimple(t *testing.T) {
	node := singleDBNodeDockerResources.nodes[0]
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

	err = node.GoalStateExec(hasFileVerifier(".*1-checkpoint.db"),
		"find",
		"/var/lib/m3db/data/coldWritesRepairAndNoIndex",
		"-name",
		"*1-checkpoint.db")

	assert.NoError(t, err)

	err = node.Restart()
	require.NoError(t, err)

	err = node.WaitForBootstrap()
	require.NoError(t, err)

	fetch, err = node.Fetch(fetchReq(coldWriteNsName, "foo"))
	require.NoError(t, err)
	verifyFetch(t, fetch, coldDp, warmDp)
}
