// Copyright (c) 2018 Uber Technologies, Inc.
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

package consumer

import (
	"time"

	"github.com/m3db/m3/src/msg/protocol/proto"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/pool"
)

// Configuration configs the consumer options.
type Configuration struct {
	Encoder                   *proto.Configuration      `yaml:"encoder"`
	Decoder                   *proto.Configuration      `yaml:"decoder"`
	MessagePool               *MessagePoolConfiguration `yaml:"messagePool"`
	AckFlushInterval          *time.Duration            `yaml:"ackFlushInterval"`
	AckBufferSize             *int                      `yaml:"ackBufferSize"`
	ConnectionWriteBufferSize *int                      `yaml:"connectionWriteBufferSize"`
	ConnectionReadBufferSize  *int                      `yaml:"connectionReadBufferSize"`
	ConnectionWriteTimeout    *time.Duration            `yaml:"connectionWriteTimeout"`
}

// MessagePoolConfiguration is the message pool configuration
// options, which extends the default object pool configuration.
type MessagePoolConfiguration struct {
	// Size is the size of the pool.
	Size pool.Size `yaml:"size"`

	// Watermark is the object pool watermark configuration.
	Watermark pool.WatermarkConfiguration `yaml:"watermark"`

	// MaxBufferReuseSize specifies the maximum buffer which can
	// be reused and pooled, if a buffer greater than this
	// is used then it is discarded. Zero specifies no limit.
	MaxBufferReuseSize int `yaml:"maxBufferReuseSize"`
}

// NewOptions creates message pool options.
func (c MessagePoolConfiguration) NewOptions(
	iopts instrument.Options,
) MessagePoolOptions {
	poolCfg := pool.ObjectPoolConfiguration{
		Size:      c.Size,
		Watermark: c.Watermark,
	}
	return MessagePoolOptions{
		PoolOptions:        poolCfg.NewObjectPoolOptions(iopts),
		MaxBufferReuseSize: c.MaxBufferReuseSize,
	}
}

// NewOptions creates consumer options.
func (c *Configuration) NewOptions(iOpts instrument.Options) Options {
	opts := NewOptions().SetInstrumentOptions(iOpts)
	if c.Encoder != nil {
		opts = opts.SetEncoderOptions(c.Encoder.NewOptions(iOpts))
	}
	if c.Decoder != nil {
		opts = opts.SetDecoderOptions(c.Decoder.NewOptions(iOpts))
	}
	if c.MessagePool != nil {
		opts = opts.SetMessagePoolOptions(c.MessagePool.NewOptions(iOpts))
	}
	if c.AckFlushInterval != nil {
		opts = opts.SetAckFlushInterval(*c.AckFlushInterval)
	}
	if c.AckBufferSize != nil {
		opts = opts.SetAckBufferSize(*c.AckBufferSize)
	}
	if c.ConnectionWriteBufferSize != nil {
		opts = opts.SetConnectionWriteBufferSize(*c.ConnectionWriteBufferSize)
	}
	if c.ConnectionReadBufferSize != nil {
		opts = opts.SetConnectionReadBufferSize(*c.ConnectionReadBufferSize)
	}
	if c.ConnectionWriteTimeout != nil {
		opts = opts.SetConnectionWriteTimeout(*c.ConnectionWriteTimeout)
	}
	return opts
}
