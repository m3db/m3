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
	"github.com/m3db/m3msg/protocol/proto"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/pool"
)

// Configuration configs the consumer options.
type Configuration struct {
	EncodeDecoder             *proto.EncodeDecoderConfiguration `yaml:"encodeDecoder"`
	MessagePool               *pool.ObjectPoolConfiguration     `yaml:"messagePool"`
	AckBufferSize             *int                              `yaml:"ackBufferSize"`
	ConnectionWriteBufferSize *int                              `yaml:"connectionWriteBufferSize"`
	ConnectionReadBufferSize  *int                              `yaml:"connectionReadBufferSize"`
}

// NewOptions creates consumer options.
func (c *Configuration) NewOptions(iOpts instrument.Options) Options {
	opts := NewOptions()
	if c.EncodeDecoder != nil {
		opts = opts.SetEncodeDecoderOptions(c.EncodeDecoder.NewEncodeDecoderOptions(iOpts))
	}
	if c.MessagePool != nil {
		opts = opts.SetMessagePoolOptions(c.MessagePool.NewObjectPoolOptions(iOpts))
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
	return opts
}
