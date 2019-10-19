// Copyright (c) 2019 Uber Technologies, Inc.
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

package downsample

import (
	"errors"

	"github.com/m3db/m3/src/query/models"

	"github.com/m3db/m3/src/aggregator/client"
	clusterclient "github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/kv/mem"
	"github.com/m3db/m3/src/metrics/generated/proto/rulepb"
	"github.com/m3db/m3/src/metrics/matcher"
	"github.com/m3db/m3/src/metrics/rules"
	ruleskv "github.com/m3db/m3/src/metrics/rules/store/kv"
	"github.com/m3db/m3/src/query/storage/mock"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/pool"
	"github.com/m3db/m3/src/x/serialize"
	xtest "github.com/m3db/m3/src/x/test"

	"github.com/golang/mock/gomock"
)

type TestDownsampler struct {
	Downsampler    Downsampler
	Opts           DownsamplerOptions
	TestOpts       TestDownsamplerOptions
	Matcher        matcher.Matcher
	Storage        mock.Storage
	RulesStore     rules.Store
	InstrumentOpts instrument.Options
}

type TestDownsamplerOptions struct {
	ClockOpts      clock.Options
	InstrumentOpts instrument.Options

	// Options for the test
	AutoMappingRules   []MappingRule
	TimedSamples       bool
	SampleAppenderOpts *SampleAppenderOptions
	RemoteClientMock   *client.MockClient
	BufferPastLimits   []BufferPastLimitConfiguration

	// Expected values overrides
	ExpectedAdjusted map[string]float64
}

func NewTestDownsampler(opts TestDownsamplerOptions) (TestDownsampler, error) {
	storage := mock.NewMockStorage()
	rulesKVStore := mem.NewStore()

	clockOpts := clock.NewOptions()
	if opts.ClockOpts != nil {
		clockOpts = opts.ClockOpts
	}

	instrumentOpts := instrument.NewOptions()
	if opts.InstrumentOpts != nil {
		instrumentOpts = opts.InstrumentOpts
	}

	matcherOpts := matcher.NewOptions()

	// Initialize the namespaces
	_, err := rulesKVStore.Set(matcherOpts.NamespacesKey(), &rulepb.Namespaces{})
	if err != nil {
		return TestDownsampler{}, err
	}

	rulesetKeyFmt := matcherOpts.RuleSetKeyFn()([]byte("%s"))
	rulesStoreOpts := ruleskv.NewStoreOptions(matcherOpts.NamespacesKey(),
		rulesetKeyFmt, nil)
	rulesStore := ruleskv.NewStore(rulesKVStore, rulesStoreOpts)

	tagEncoderOptions := serialize.NewTagEncoderOptions()
	tagDecoderOptions := serialize.NewTagDecoderOptions()
	tagEncoderPoolOptions := pool.NewObjectPoolOptions().
		SetInstrumentOptions(instrumentOpts.
			SetMetricsScope(instrumentOpts.MetricsScope().
				SubScope("tag-encoder-pool")))
	tagDecoderPoolOptions := pool.NewObjectPoolOptions().
		SetInstrumentOptions(instrumentOpts.
			SetMetricsScope(instrumentOpts.MetricsScope().
				SubScope("tag-decoder-pool")))

	var cfg Configuration
	if opts.RemoteClientMock != nil {
		// Optionally set an override to use remote aggregation
		// with a mock client
		cfg.RemoteAggregator = &RemoteAggregatorConfiguration{
			clientOverride: opts.RemoteClientMock,
		}
	}
	if len(opts.BufferPastLimits) > 0 {
		cfg.BufferPastLimits = opts.BufferPastLimits
	}

	clusterClient := clusterclient.NewMockClient(
		gomock.NewController(xtest.PanicReporter{}))

	instance, err := cfg.NewDownsampler(DownsamplerOptions{
		Storage:               storage,
		ClusterClient:         clusterClient,
		RulesKVStore:          rulesKVStore,
		AutoMappingRules:      opts.AutoMappingRules,
		ClockOptions:          clockOpts,
		InstrumentOptions:     instrumentOpts,
		TagEncoderOptions:     tagEncoderOptions,
		TagDecoderOptions:     tagDecoderOptions,
		TagEncoderPoolOptions: tagEncoderPoolOptions,
		TagDecoderPoolOptions: tagDecoderPoolOptions,
		TagOptions:            models.NewTagOptions(),
	})
	if err != nil {
		return TestDownsampler{}, err
	}

	downcast, ok := instance.(*downsampler)
	if !ok {
		return TestDownsampler{}, errors.New("cannot get access downsampler fields")
	}

	return TestDownsampler{
		Downsampler:    instance,
		Opts:           downcast.opts,
		TestOpts:       opts,
		Matcher:        downcast.agg.matcher,
		Storage:        storage,
		RulesStore:     rulesStore,
		InstrumentOpts: instrumentOpts,
	}, nil
}
