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

package storage

import (
	"fmt"

	"github.com/MichaelTJones/pcg"
)

// dice is an interface that allows for random sampling.
type dice interface {
	// Rate returns the sampling rate of this dice: a number in (0.0, 1.0].
	Rate() float64

	// Roll returns whether the dice roll succeeded.
	Roll() bool
}

// newDice constructs a new dice based on a given success rate.
func newDice(rate float64) (dice, error) {
	if rate <= 0.0 || rate > 1.0 {
		return nil, fmt.Errorf("invalid sample rate %f", rate)
	}

	return &epoch{
		r:   uint64(1.0 / rate),
		rng: pcg.NewPCG64(),
	}, nil
}

type epoch struct {
	r   uint64
	rng *pcg.PCG64
}

func (d *epoch) Rate() float64 {
	return 1 / float64(d.r)
}

func (d *epoch) Roll() bool {
	return d.rng.Random()%d.r == 0
}
