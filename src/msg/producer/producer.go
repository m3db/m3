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

package producer

type producer struct {
	Buffer
	Writer
}

// NewProducer returns a new producer.
func NewProducer(opts Options) Producer {
	return &producer{
		Buffer: opts.Buffer(),
		Writer: opts.Writer(),
	}
}

func (p *producer) Init() error {
	p.Buffer.Init()
	return p.Writer.Init()
}

func (p *producer) Produce(m Message) error {
	rm, err := p.Buffer.Add(m)
	if err != nil {
		return err
	}
	return p.Writer.Write(rm)
}

func (p *producer) Close(ct CloseType) {
	// NB: Must close buffer first, it will start returning errors on
	// new writes immediately. Then if the close type is to wait for consumption
	// it will block until all messages got consumed. If the close type is to drop
	// everything, it will drop everything buffered.
	p.Buffer.Close(ct)
	// Then we can close writer to clean up outstanding go routines.
	p.Writer.Close()
}
