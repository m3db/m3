package ingest

import (
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/x/serialize"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/pool"
	"github.com/m3db/m3x/retry"
	xsync "github.com/m3db/m3x/sync"
)

// Configuration configs the ingester.
type Configuration struct {
	WorkerPoolSize int                          `yaml:"workerPoolSize"`
	OpPool         pool.ObjectPoolConfiguration `yaml:"opPool"`
	Retry          retry.Configuration          `yaml:"retry"`
}

// NewIngester creates an ingester with an appender.
func (cfg Configuration) NewIngester(
	appender storage.Appender,
	instrumentOptions instrument.Options,
) (*Ingester, error) {
	opts, err := cfg.newOptions(appender, instrumentOptions)
	if err != nil {
		return nil, err
	}
	return NewIngester(opts), nil
}

func (cfg Configuration) newOptions(
	appender storage.Appender,
	instrumentOptions instrument.Options,
) (*Options, error) {
	scope := instrumentOptions.MetricsScope().Tagged(
		map[string]string{"component": "ingester"},
	)
	workers, err := xsync.NewPooledWorkerPool(
		cfg.WorkerPoolSize,
		xsync.NewPooledWorkerPoolOptions().
			SetInstrumentOptions(instrumentOptions),
	)
	if err != nil {
		return nil, err
	}

	workers.Init()
	tagDecoderPool := serialize.NewTagDecoderPool(
		serialize.NewTagDecoderOptions(),
		pool.NewObjectPoolOptions().
			SetInstrumentOptions(instrumentOptions.
				SetMetricsScope(instrumentOptions.MetricsScope().
					SubScope("tag-decoder-pool"))),
	)
	tagDecoderPool.Init()
	opts := Options{
		Appender:          appender,
		Workers:           workers,
		PoolOptions:       cfg.OpPool.NewObjectPoolOptions(instrumentOptions),
		TagDecoderPool:    tagDecoderPool,
		RetryOptions:      cfg.Retry.NewOptions(scope),
		InstrumentOptions: instrumentOptions,
	}
	return &opts, nil
}
