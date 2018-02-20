package fanout

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/m3db/m3coordinator/policy/filter"
	"github.com/m3db/m3coordinator/policy/resolver"
	"github.com/m3db/m3coordinator/storage"
	"github.com/m3db/m3coordinator/storage/local"
	"github.com/m3db/m3coordinator/ts"
	"github.com/m3db/m3coordinator/util/logging"

	"github.com/m3db/m3db/client"
	"github.com/m3db/m3db/encoding"
	"github.com/m3db/m3metrics/policy"
	xtime "github.com/m3db/m3x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func filterFunc(output bool) filter.Storage {
	return func(query storage.Query, store storage.Storage) bool {
		return output
	}
}

func fakeIterator() encoding.SeriesIterator {
	return encoding.NewSeriesIterator("id", time.Now(), time.Now(), nil, nil)
}

type fetchResponse struct {
	result encoding.SeriesIterator
	err    error
}

func setup() {
	logging.InitWithCores(nil)
	logger := logging.WithContext(context.TODO())
	defer logger.Sync()
}

func setupFanoutRead(t *testing.T, output bool, response ...*fetchResponse) storage.Storage {
	setup()
	if len(response) == 0 {
		response = []*fetchResponse{{err: fmt.Errorf("unable to get response")}}
	}

	ctrl := gomock.NewController(t)
	session1 := client.NewMockSession(ctrl)
	session2 := client.NewMockSession(ctrl)
	session1.EXPECT().Fetch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(response[0].result, response[0].err)
	session2.EXPECT().Fetch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(response[len(response)-1].result, response[len(response)-1].err)
	stores := []storage.Storage{
		local.NewStorage(session1, "metrics", resolver.NewStaticResolver(policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour*48))),
		local.NewStorage(session2, "metrics", resolver.NewStaticResolver(policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour*48))),
	}

	store := NewStorage(stores, filterFunc(output), filterFunc(output))
	return store
}

func setupFanoutWrite(t *testing.T, output bool, errs ...error) storage.Storage {
	setup()
	ctrl := gomock.NewController(t)
	session1 := client.NewMockSession(ctrl)
	session2 := client.NewMockSession(ctrl)
	session1.EXPECT().Write(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(errs[0])
	session2.EXPECT().Write(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(errs[len(errs)-1])
	stores := []storage.Storage{
		local.NewStorage(session1, "metrics", resolver.NewStaticResolver(policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour*48))),
		local.NewStorage(session2, "metrics", resolver.NewStaticResolver(policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour*48))),
	}
	store := NewStorage(stores, filterFunc(output), filterFunc(output))
	return store
}

func TestFanoutReadEmpty(t *testing.T) {
	store := setupFanoutRead(t, false)
	res, err := store.Fetch(context.TODO(), nil, nil)
	assert.NoError(t, err, "No error")
	require.NotNil(t, res, "Non empty result")
	assert.Len(t, res.SeriesList, 0, "No series")
}

func TestFanoutReadError(t *testing.T) {
	store := setupFanoutRead(t, true)
	_, err := store.Fetch(context.TODO(), &storage.FetchQuery{}, &storage.FetchOptions{})
	assert.Error(t, err)
}

func TestFanoutReadSuccess(t *testing.T) {
	store := setupFanoutRead(t, true, &fetchResponse{result: fakeIterator()}, &fetchResponse{result: fakeIterator()})
	res, err := store.Fetch(context.TODO(), &storage.FetchQuery{}, &storage.FetchOptions{})
	require.NoError(t, err, "no error on read")
	assert.NotNil(t, res)
}

func TestFanoutWriteEmpty(t *testing.T) {
	store := setupFanoutWrite(t, false, fmt.Errorf("write error"))
	err := store.Write(context.TODO(), nil)
	assert.NoError(t, err)
}

func TestFanoutWriteError(t *testing.T) {
	store := setupFanoutWrite(t, true, fmt.Errorf("write error"))
	datapoints := make(ts.Datapoints, 1)
	datapoints[0] = &ts.Datapoint{Timestamp: time.Now(), Value: 1}
	err := store.Write(context.TODO(), &storage.WriteQuery{
		Datapoints: datapoints,
	})
	assert.Error(t, err)
}

func TestFanoutWriteSuccess(t *testing.T) {
	store := setupFanoutWrite(t, true, nil)
	datapoints := make(ts.Datapoints, 1)
	datapoints[0] = &ts.Datapoint{Timestamp: time.Now(), Value: 1}
	err := store.Write(context.TODO(), &storage.WriteQuery{
		Datapoints: datapoints,
	})
	assert.NoError(t, err)
}
