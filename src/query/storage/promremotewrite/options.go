package promremotewrite

import (
	"time"

	xhttp "github.com/m3db/m3/src/x/net/http"
)

type Options struct {
	endpoint        string
	RequestTimeout  time.Duration `yaml:"requestTimeout"`
	ConnectTimeout  time.Duration `yaml:"connectTimeout"`
	KeepAlive       time.Duration `yaml:"keepAlive"`
	IdleConnTimeout time.Duration `yaml:"idleConnTimeout"`
	MaxIdleConns    int           `yaml:"maxIdleConns"`
}

func (o *Options) HTTPClientOptions() xhttp.HTTPClientOptions {
	clientOpts := xhttp.DefaultHTTPClientOptions()
	if o.RequestTimeout != 0 {
		clientOpts.RequestTimeout = o.RequestTimeout
	}
	if o.ConnectTimeout != 0 {
		clientOpts.ConnectTimeout = o.ConnectTimeout
	}
	if o.KeepAlive != 0 {
		clientOpts.KeepAlive = o.KeepAlive
	}
	if o.IdleConnTimeout != 0 {
		clientOpts.IdleConnTimeout = o.IdleConnTimeout
	}
	if o.MaxIdleConns != 0 {
		clientOpts.MaxIdleConns = o.MaxIdleConns
	}

	clientOpts.DisableCompression = true // Already snappy compressed.
	return clientOpts
}

