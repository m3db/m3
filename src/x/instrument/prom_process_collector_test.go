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

package instrument

import (
	"bytes"
	"errors"
	"os"
	"regexp"
	"testing"

	"github.com/m3db/prometheus_client_golang/prometheus"
	dto "github.com/m3db/prometheus_client_model/go"
	"github.com/m3db/prometheus_common/expfmt"
	procfs "github.com/m3db/prometheus_procfs"
)

func TestProcessCollector(t *testing.T) {
	if _, err := procfs.Self(); err != nil {
		t.Skipf("skipping TestProcessCollector, procfs not available: %s", err)
	}

	registry := prometheus.NewRegistry()
	if err := registry.Register(NewPrometheusProcessCollector(ProcessCollectorOpts{})); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(NewPrometheusProcessCollector(ProcessCollectorOpts{
		PidFn:        func() (int, error) { return os.Getpid(), nil },
		Namespace:    "foobar",
		ReportErrors: true, // No errors expected, just to see if none are reported.
	})); err != nil {
		t.Fatal(err)
	}

	mfs, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	for _, mf := range mfs {
		if _, err := expfmt.MetricFamilyToText(&buf, mf); err != nil {
			t.Fatal(err)
		}
	}

	for _, re := range []*regexp.Regexp{
		regexp.MustCompile("\nprocess_cpu_seconds_total [0-9]"),
		regexp.MustCompile("\nprocess_max_fds [1-9]"),
		regexp.MustCompile("\nprocess_open_fds [1-9]"),
		regexp.MustCompile("\nprocess_virtual_memory_max_bytes (-1|[1-9])"),
		regexp.MustCompile("\nprocess_virtual_memory_bytes [1-9]"),
		regexp.MustCompile("\nprocess_resident_memory_bytes [1-9]"),
		regexp.MustCompile("\nprocess_start_time_seconds [0-9.]{10,}"),
		regexp.MustCompile("\nfoobar_process_cpu_seconds_total [0-9]"),
		regexp.MustCompile("\nfoobar_process_max_fds [1-9]"),
		regexp.MustCompile("\nfoobar_process_open_fds [1-9]"),
		regexp.MustCompile("\nfoobar_process_virtual_memory_max_bytes (-1|[1-9])"),
		regexp.MustCompile("\nfoobar_process_virtual_memory_bytes [1-9]"),
		regexp.MustCompile("\nfoobar_process_resident_memory_bytes [1-9]"),
		regexp.MustCompile("\nfoobar_process_start_time_seconds [0-9.]{10,}"),
	} {
		if !re.Match(buf.Bytes()) {
			t.Errorf("want body to match %s\n%s", re, buf.String())
		}
	}

	brokenProcessCollector := NewPrometheusProcessCollector(ProcessCollectorOpts{
		PidFn:        func() (int, error) { return 0, errors.New("boo") },
		ReportErrors: true,
	})

	ch := make(chan prometheus.Metric)
	go func() {
		brokenProcessCollector.Collect(ch)
		close(ch)
	}()
	n := 0
	for m := range ch {
		n++
		pb := &dto.Metric{}
		err := m.Write(pb)
		if err == nil {
			t.Error("metric collected from broken process collector is unexpectedly valid")
		}
	}
	if n != 1 {
		t.Errorf("%d metrics collected, want 1", n)
	}
}
