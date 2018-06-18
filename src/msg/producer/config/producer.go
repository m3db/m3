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

package config

import (
	"github.com/m3db/m3cluster/client"
	"github.com/m3db/m3msg/producer"
	"github.com/m3db/m3msg/producer/buffer"
	"github.com/m3db/m3msg/producer/writer"
	"github.com/m3db/m3x/instrument"
)

// ProducerConfiguration configs the producer.
type ProducerConfiguration struct {
	Buffer BufferConfiguration `yaml:"buffer"`
	Writer WriterConfiguration `yaml:"writer"`
}

func (c *ProducerConfiguration) newOptions(
	cs client.Client,
	iOpts instrument.Options,
) (producer.Options, error) {
	wOpts, err := c.Writer.NewOptions(cs, iOpts)
	if err != nil {
		return nil, err
	}
	b, err := buffer.NewBuffer(c.Buffer.NewOptions(iOpts))
	if err != nil {
		return nil, err
	}
	return producer.NewOptions().
		SetBuffer(b).
		SetWriter(writer.NewWriter(wOpts)), nil
}

// NewProducer creates new producer.
func (c *ProducerConfiguration) NewProducer(
	cs client.Client,
	iOpts instrument.Options,
) (producer.Producer, error) {
	opts, err := c.newOptions(cs, iOpts)
	if err != nil {
		return nil, err
	}
	return producer.NewProducer(opts), nil
}
