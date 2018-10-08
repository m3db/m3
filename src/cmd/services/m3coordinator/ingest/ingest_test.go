package ingest

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/cmd/services/m3coordinator/server/m3msg"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3/src/x/serialize"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3msg/consumer"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/pool"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestIngest(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cfg := Configuration{
		WorkerPoolSize: 2,
		OpPool: pool.ObjectPoolConfiguration{
			Size: 1,
		},
	}
	appender := &mockAppender{}
	ingester, err := cfg.NewIngester(appender, instrument.NewOptions())
	require.NoError(t, err)

	id := newTestID(t, "__name__", "foo", "app", "bar")
	timestamp := int64(1234)
	val := float64(1)
	sp := policy.MustParseStoragePolicy("1m:40d")
	m := consumer.NewMockMessage(ctrl)
	callback := m3msg.NewRefCountedCallback(m)
	callback.IncRef()

	m.EXPECT().Ack()
	ingester.Ingest(id, timestamp, val, sp, callback)

	for appender.cnt() != 1 {
		time.Sleep(100 * time.Millisecond)
	}

	require.Equal(t,
		storage.WriteQuery{
			Annotation: nil,
			Attributes: storage.Attributes{
				MetricsType: storage.AggregatedMetricsType,
				Resolution:  time.Minute,
				Retention:   40 * 24 * time.Hour,
			},
			Datapoints: ts.Datapoints{
				ts.Datapoint{
					Timestamp: time.Unix(0, timestamp),
					Value:     val,
				},
			},
			Raw: string(id),
			Tags: models.Tags{
				models.Tag{
					Name:  []byte("__name__"),
					Value: []byte("foo"),
				},
				models.Tag{
					Name:  []byte("app"),
					Value: []byte("bar"),
				},
			},
			Unit: sp.Resolution().Precision,
		},
		*appender.received[0],
	)

	// Make sure the op is put back to pool.
	op := ingester.p.Get().(*ingestOp)
	require.Equal(t, id, op.id)
}

type mockAppender struct {
	sync.RWMutex

	expectErr error
	received  []*storage.WriteQuery
}

func (m *mockAppender) Write(ctx context.Context, query *storage.WriteQuery) error {
	m.Lock()
	defer m.Unlock()

	if m.expectErr != nil {
		return m.expectErr
	}
	m.received = append(m.received, query)
	return nil
}

func (m *mockAppender) cnt() int {
	m.Lock()
	defer m.Unlock()

	return len(m.received)
}

func newTestID(t *testing.T, tags ...string) []byte {
	tagEncoderPool := serialize.NewTagEncoderPool(serialize.NewTagEncoderOptions(),
		pool.NewObjectPoolOptions().SetSize(1))
	tagEncoderPool.Init()

	tagsIter := ident.MustNewTagStringsIterator(tags...)
	tagEncoder := tagEncoderPool.Get()
	err := tagEncoder.Encode(tagsIter)
	require.NoError(t, err)

	data, ok := tagEncoder.Data()
	require.True(t, ok)
	return data.Bytes()
}
