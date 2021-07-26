package instrument

import (
	"go.uber.org/zap"
)

// WithOptions creates a new instrument options with options.
func WithOptions(iOpts Options, opts ...Option) Options {
	ptr := &iOpts
	for _, opt := range opts {
		opt(ptr)
	}
	return *ptr
}

// Option configures the instrument options.
type Option func(*Options)

// WithScopeAndLoggerTagged tags both the metric scope and logger in the
// instrument options.
func WithScopeAndLoggerTagged(k, v string) Option {
	return func(ptr *Options) {
		WithScopeTagged(k, v)(ptr)
		WithLoggerTagged(k, v)(ptr)
	}
}

// WithScopeTagged tags the metric scope in the instrument options.
func WithScopeTagged(k, v string) Option {
	return func(ptr *Options) {
		iOpts := *ptr
		iOpts = iOpts.SetMetricsScope(iOpts.MetricsScope().Tagged(map[string]string{k: v}))
		*ptr = iOpts
	}
}

// WithLoggerTagged tags the logger in the instrument options.
func WithLoggerTagged(k, v string) Option {
	return func(ptr *Options) {
		iOpts := *ptr
		iOpts = iOpts.SetLogger(iOpts.Logger().With(zap.String(k, v)))
		*ptr = iOpts
	}
}
