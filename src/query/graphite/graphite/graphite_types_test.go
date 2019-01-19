package graphite

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"math"
	"testing"
	"time"

	"github.com/hydrogen18/stalecucumber"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMarshalRenderResults(t *testing.T) {
	expectedJSON := "[{\"target\":\"foo.bar\"," +
		"\"datapoints\":[[100,1431470141],[null,1431470151],[3.1456,1431470161]]}]"

	tm := time.Date(2015, 5, 12, 22, 35, 41, 0, time.UTC)

	target := RenderTarget{
		Target: "foo.bar",
	}

	target.Datapoints.Add(tm, 100)
	target.Datapoints.Add(tm.Add(10*time.Second), math.NaN())
	target.Datapoints.Add(tm.Add(20*time.Second), 3.1456)
	results := RenderResults{target}

	r, err := json.Marshal(results)
	require.Nil(t, err)
	assert.Equal(t, expectedJSON, string(r))

	var parsed RenderResults
	err = json.Unmarshal(r, &parsed)
	require.Nil(t, err)

	parsedDatapoints := parsed[0].Datapoints
	timestamp, value := parsedDatapoints.Get(0)
	assert.Equal(t, tm, timestamp)
	assert.Equal(t, float64(100), value)

	timestamp, value = parsedDatapoints.Get(1)
	assert.Equal(t, tm.Add(10*time.Second), timestamp)
	assert.Equal(t, math.NaN(), value)

	timestamp, value = parsedDatapoints.Get(2)
	assert.Equal(t, tm.Add(20*time.Second), timestamp)
	assert.Equal(t, float64(3.1456), value)
}

func TestDatapointAccess(t *testing.T) {
	data, err := ioutil.ReadFile("../../test/fixtures/graphite/no-results.json")
	require.Nil(t, err)

	var results RenderResults
	err = json.Unmarshal(data, &results)
	require.Nil(t, err)
	require.Equal(t, 1, len(results))
	require.Equal(t, "exp.sjc1.timers.cn.production.cn37-sjc1.city_load.monterrey.total.p95", results[0].Target)
	require.Equal(t, 360, len(results[0].Datapoints))

	datapoints := results[0].Datapoints
	for i := range datapoints {
		_, value := datapoints.Get(i)
		assert.Equal(t, math.NaN(), value, "invalid value for %d", i)
	}
}

func TestMarshalUnmarshalJSONResults(t *testing.T) {
	expectedJSON := "{\"foo.bar\":[" +
		"{\"t\":1431470141,\"v\":100}," +
		"{\"t\":1431470151,\"v\":null}," +
		"{\"t\":1431470161,\"v\":3.1456}]}"

	tm := time.Date(2015, 5, 12, 22, 35, 41, 0, time.UTC)
	results := Results{
		"foo.bar": {
			{Timestamp(tm), Datavalue(100)},
			{Timestamp(tm.Add(10 * time.Second)), Datavalue(math.NaN())},
			{Timestamp(tm.Add(20 * time.Second)), Datavalue(3.1456)},
		},
	}

	r, err := json.Marshal(results)
	require.Nil(t, err)
	assert.Equal(t, expectedJSON, string(r))

	var unmarshalled Results
	require.Nil(t, json.Unmarshal(r, &unmarshalled))
	assert.Equal(t, results, unmarshalled)
}

func TestPickleValueAccess(t *testing.T) {
	now := time.Now().Truncate(time.Second).UTC()
	r := RenderResultsPickle{
		Name:   "foo.bar.baz space",
		Start:  uint32(now.Unix()),
		End:    uint32(now.Add(time.Minute).Unix()),
		Step:   20,
		Values: []interface{}{1.01, nil, -2.02},
	}

	assert.Equal(t, 3, r.Len())
	assert.Equal(t, 1.01, r.ValueAt(0))
	assert.Equal(t, math.NaN(), r.ValueAt(1))
	assert.Equal(t, -2.02, r.ValueAt(2))

	timestamp, n := r.Get(0)
	assert.Equal(t, now, timestamp)
	assert.Equal(t, 1.01, n)

	timestamp, n = r.Get(1)
	assert.Equal(t, now.Add(time.Second*20), timestamp)
	assert.Equal(t, math.NaN(), n)

	timestamp, n = r.Get(2)
	assert.Equal(t, now.Add(time.Second*40), timestamp)
	assert.Equal(t, -2.02, n)
}

func TestMarshalUnmarshalPickleResults(t *testing.T) {

	f1 := 1.01
	f2 := -2.02
	f3 := math.NaN() // should really become nil

	in := []RenderResultsPickle{
		RenderResultsPickle{
			Name:   "foo.bar.baz space",
			Start:  1234,
			End:    4321,
			Step:   1000,
			Values: []interface{}{f1, f2, f3, nil},
		},
	}

	buf := new(bytes.Buffer)
	_, err := stalecucumber.NewPickler(buf).Pickle(in)
	assert.Nil(t, err, "Unable to pickle data")

	out, err := ParseRenderResultsPickle(buf.Bytes())
	assert.Nil(t, err, "Unable to unpickle data")

	assert.Equal(t, in, out)
}
