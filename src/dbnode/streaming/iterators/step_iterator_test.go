package iterators

import (
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/streaming/downsamplers"

	xtime "github.com/m3db/m3/src/x/time"
)

func TestStepIterator(t *testing.T) {
	now := time.Now().Truncate(time.Hour)

	testCases := []struct {
		name         string
		start        time.Time
		step         time.Duration
		givenInput   []iteratorSample
		wantedOutput []iteratorSample
	}{
		{
			name:  "Empty",
			start: now,
			step:  time.Minute,
		},
		{
			name:  "Single sample",
			start: now,
			step:  time.Minute,
			givenInput: []iteratorSample{
				{
					dp:   datapoint(now, 1),
					unit: xtime.Millisecond,
				},
			},
			wantedOutput: []iteratorSample{
				{
					dp:   datapoint(now, 1),
					unit: xtime.Millisecond,
				},
			},
		},
		{
			name:  "Single offset sample",
			start: now,
			step:  time.Minute,
			givenInput: []iteratorSample{
				{
					dp:   datapoint(now.Add(time.Minute), 1),
					unit: xtime.Millisecond,
				},
			},
			wantedOutput: []iteratorSample{
				{
					dp:   datapoint(now.Add(time.Minute), 1),
					unit: xtime.Millisecond,
				},
			},
		},
		{
			name:  "Two samples within single step",
			start: now,
			step:  time.Hour,
			givenInput: []iteratorSample{
				{
					dp:   datapoint(now, 1),
					unit: xtime.Millisecond,
				},
				{
					dp:   datapoint(now.Add(time.Minute), 2),
					unit: xtime.Millisecond,
				},
			},
			wantedOutput: []iteratorSample{
				{
					dp:   datapoint(now.Add(time.Minute), 2),
					unit: xtime.Millisecond,
				},
			},
		},
		{
			name:  "Two samples within two steps",
			start: now,
			step:  time.Minute,
			givenInput: []iteratorSample{
				{
					dp:   datapoint(now, 1),
					unit: xtime.Millisecond,
				},
				{
					dp:   datapoint(now.Add(time.Minute), 2),
					unit: xtime.Millisecond,
				},
			},
			wantedOutput: []iteratorSample{
				{
					dp:   datapoint(now, 1),
					unit: xtime.Millisecond,
				},
				{
					dp:   datapoint(now.Add(time.Minute), 2),
					unit: xtime.Millisecond,
				},
			},
		},
		{
			name:  "Two offset samples within two steps",
			start: now,
			step:  time.Hour,
			givenInput: []iteratorSample{
				{
					dp:   datapoint(now.Add(59*time.Minute), 1),
					unit: xtime.Millisecond,
				},
				{
					dp:   datapoint(now.Add(60*time.Minute), 2),
					unit: xtime.Millisecond,
				},
			},
			wantedOutput: []iteratorSample{
				{
					dp:   datapoint(now.Add(59*time.Minute), 1),
					unit: xtime.Millisecond,
				},
				{
					dp:   datapoint(now.Add(60*time.Minute), 2),
					unit: xtime.Millisecond,
				},
			},
		},
		{
			name:  "Two sparse samples within two steps",
			start: now,
			step:  time.Minute,
			givenInput: []iteratorSample{
				{
					dp:   datapoint(now, 1),
					unit: xtime.Millisecond,
				},
				{
					dp:   datapoint(now.Add(20*time.Minute), 2),
					unit: xtime.Millisecond,
				},
			},
			wantedOutput: []iteratorSample{
				{
					dp:   datapoint(now, 1),
					unit: xtime.Millisecond,
				},
				{
					dp:   datapoint(now.Add(20*time.Minute), 2),
					unit: xtime.Millisecond,
				},
			},
		},
		{
			name:  "Three samples within single step",
			start: now,
			step:  time.Hour,
			givenInput: []iteratorSample{
				{
					dp:   datapoint(now, 1),
					unit: xtime.Millisecond,
				},
				{
					dp:   datapoint(now.Add(time.Minute), 2),
					unit: xtime.Millisecond,
				},
				{
					dp:   datapoint(now.Add(2*time.Minute), 3),
					unit: xtime.Millisecond,
				},
			},
			wantedOutput: []iteratorSample{
				{
					dp:   datapoint(now.Add(2*time.Minute), 3),
					unit: xtime.Millisecond,
				},
			},
		},
		{
			name:  "Three samples within two steps",
			start: now,
			step:  time.Minute,
			givenInput: []iteratorSample{
				{
					dp:   datapoint(now, 1),
					unit: xtime.Millisecond,
				},
				{
					dp:   datapoint(now.Add(30*time.Second), 2),
					unit: xtime.Millisecond,
				},
				{
					dp:   datapoint(now.Add(60*time.Second), 3),
					unit: xtime.Millisecond,
				},
			},
			wantedOutput: []iteratorSample{
				{
					dp:   datapoint(now.Add(30*time.Second), 2),
					unit: xtime.Millisecond,
				},
				{
					dp:   datapoint(now.Add(60*time.Second), 3),
					unit: xtime.Millisecond,
				},
			},
		},
		{
			name:  "Three samples within three steps",
			start: now,
			step:  time.Minute,
			givenInput: []iteratorSample{
				{
					dp:   datapoint(now, 1),
					unit: xtime.Millisecond,
				},
				{
					dp:   datapoint(now.Add(time.Minute), 2),
					unit: xtime.Millisecond,
				},
				{
					dp:   datapoint(now.Add(2*time.Minute), 3),
					unit: xtime.Millisecond,
				},
			},
			wantedOutput: []iteratorSample{
				{
					dp:   datapoint(now, 1),
					unit: xtime.Millisecond,
				},
				{
					dp:   datapoint(now.Add(time.Minute), 2),
					unit: xtime.Millisecond,
				},
				{
					dp:   datapoint(now.Add(2*time.Minute), 3),
					unit: xtime.Millisecond,
				},
			},
		},
		{
			name:  "One sample distantly followed by two close samples",
			start: now,
			step:  time.Hour,
			givenInput: []iteratorSample{
				{
					dp:   datapoint(now, 1),
					unit: xtime.Millisecond,
				},
				{
					dp:   datapoint(now.Add(121*time.Minute), 2),
					unit: xtime.Millisecond,
				},
				{
					dp:   datapoint(now.Add(122*time.Minute), 3),
					unit: xtime.Millisecond,
				},
			},
			wantedOutput: []iteratorSample{
				{
					dp:   datapoint(now, 1),
					unit: xtime.Millisecond,
				},
				{
					dp:   datapoint(now.Add(122*time.Minute), 3),
					unit: xtime.Millisecond,
				},
			},
		},
		{
			name:  "Unaligned start time",
			start: now.Add(-time.Minute),
			step:  time.Hour,
			givenInput: []iteratorSample{
				{
					dp:   datapoint(now, 1),
					unit: xtime.Millisecond,
				},
				{
					dp:   datapoint(now.Add(time.Hour-time.Second), 2),
					unit: xtime.Millisecond,
				},
				{
					dp:   datapoint(now.Add(time.Hour), 3),
					unit: xtime.Millisecond,
				},
			},
			wantedOutput: []iteratorSample{
				{
					dp:   datapoint(now, 1),
					unit: xtime.Millisecond,
				},
				{
					dp:   datapoint(now.Add(time.Hour), 3),
					unit: xtime.Millisecond,
				},
			},
		},
		{
			name:  "Zero start time",
			start: time.Unix(0, 0),
			step:  time.Hour,
			givenInput: []iteratorSample{
				{
					dp:   datapoint(now.Add(time.Minute), 1),
					unit: xtime.Millisecond,
				},
				{
					dp:   datapoint(now.Add(2 * time.Minute), 2),
					unit: xtime.Millisecond,
				},
				{
					dp:   datapoint(now.Add(3 * time.Minute), 3),
					unit: xtime.Millisecond,
				},
			},
			wantedOutput: []iteratorSample{
				{
					dp:   datapoint(now.Add(3 * time.Minute), 3),
					unit: xtime.Millisecond,
				},
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			input := newSliceBasedIterator(tc.givenInput)
			wanted := newSliceBasedIterator(tc.wantedOutput)
			output := NewStepIterator(input, xtime.ToUnixNano(tc.start), tc.step, downsamplers.NewLastValueDownsampler())

			assertIteratorsEqual(t, wanted, output)
			assertExhaustedAndClosed(t, input)
		})
	}
}
