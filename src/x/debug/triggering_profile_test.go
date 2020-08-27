// Copyright (c) 2020 Uber Technologies, Inc.
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

package debug

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/m3db/m3/src/x/instrument"

	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestContinuousFileProfile(t *testing.T) {
	for _, test := range []struct {
		name     string
		duration time.Duration
		debug    int
	}{
		{
			name:     ContinuousCPUProfileName,
			duration: time.Second,
		},
		{
			name:  "goroutine",
			debug: 2,
		},
	} {
		test := test
		name := fmt.Sprintf("%s_%s_%d", test.name, test.duration, test.debug)
		t.Run(name, func(t *testing.T) {
			dir, err := ioutil.TempDir("", "")
			require.NoError(t, err)

			defer os.RemoveAll(dir)

			ext := ".profile"

			cond := atomic.NewBool(false)
			opts := ContinuousFileProfileOptions{
				FilePathTemplate:  path.Join(dir, "{{.ProfileName}}-{{.UnixTime}}"+ext),
				ProfileName:       test.name,
				ProfileDuration:   test.duration,
				ProfileDebug:      test.debug,
				Interval:          20 * time.Millisecond,
				Conditional:       cond.Load,
				InstrumentOptions: instrument.NewTestOptions(t),
			}

			profile, err := NewContinuousFileProfile(opts)
			require.NoError(t, err)

			require.NoError(t, profile.Start())

			for i := 0; i < 10; i++ {
				// Make sure doesn't create files until conditional returns true
				time.Sleep(opts.Interval)

				files, err := ioutil.ReadDir(dir)
				require.NoError(t, err)

				for _, f := range files {
					if strings.HasSuffix(f.Name(), ext) {
						require.FailNow(t, "conditional false, profile generated")
					}
				}
			}

			// Trigger profile.
			cond.Store(true)

			for start := time.Now(); time.Since(start) < 10*time.Second; {
				files, err := ioutil.ReadDir(dir)
				require.NoError(t, err)

				profiles := 0
				for _, f := range files {
					if strings.HasSuffix(f.Name(), ext) {
						pattern := test.name + `-[0-9]+\` + ext
						re := regexp.MustCompile(pattern)
						require.True(t, re.MatchString(f.Name()),
							fmt.Sprintf(
								"file should match pattern: pattern=%s, actual=%s",
								pattern, f.Name()))
						profiles++
					}
				}

				if profiles >= 2 {
					// Successfully continuously emitting.
					return
				}
			}

			require.FailNow(t, "did not generate profiles")
		})
	}
}
