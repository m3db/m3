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

// Copyright 2015 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Parts of code were taken from prometheus repo: https://github.com/prometheus/prometheus/blob/master/promql/test.go

package compatibility

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	cparser "github.com/m3db/m3/src/cmd/services/m3comparator/main/parser"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus"

	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/util/testutil"
)

var (
	minNormal = math.Float64frombits(0x0010000000000000) // The smallest positive normal value of type float64.

	patSpace       = regexp.MustCompile("[\t ]+")
	patLoad        = regexp.MustCompile(`^load\s+(.+?)$`)
	patEvalInstant = regexp.MustCompile(`^eval(?:_(fail|ordered))?\s+instant\s+(?:at\s+(.+?))?\s+(.+)$`)
)

const (
	epsilon      = 0.000001            // Relative error allowed for sample values.
	startingTime = 1587393285000000000 // 2020-04-20 17:34:45
)

var testStartTime = time.Unix(0, 0).UTC()

// Test is a sequence of read and write commands that are run
// against a test storage.
type Test struct {
	testutil.T

	cmds []testCommand

	context context.Context

	m3comparator *m3comparatorClient
}

// NewTest returns an initialized empty Test.
func NewTest(t testutil.T, input string) (*Test, error) {
	test := &Test{
		T:            t,
		cmds:         []testCommand{},
		m3comparator: newM3ComparatorClient("localhost", 9001),
	}
	err := test.parse(input)
	if err != nil {
		return test, err
	}
	err = test.clear()
	return test, err
}

func newTestFromFile(t testutil.T, filename string) (*Test, error) {
	content, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	return NewTest(t, string(content))
}

func raise(line int, format string, v ...interface{}) error {
	return &parser.ParseErr{
		LineOffset: line,
		Err:        errors.Errorf(format, v...),
	}
}

func (t *Test) parseLoad(lines []string, i int) (int, *loadCmd, error) {
	if !patLoad.MatchString(lines[i]) {
		return i, nil, raise(i, "invalid load command. (load <step:duration>)")
	}
	parts := patLoad.FindStringSubmatch(lines[i])

	gap, err := model.ParseDuration(parts[1])
	if err != nil {
		return i, nil, raise(i, "invalid step definition %q: %s", parts[1], err)
	}
	cmd := newLoadCmd(t.m3comparator, time.Duration(gap))
	for i+1 < len(lines) {
		i++
		defLine := lines[i]
		if len(defLine) == 0 {
			i--
			break
		}
		metric, vals, err := parser.ParseSeriesDesc(defLine)
		if err != nil {
			if perr, ok := err.(*parser.ParseErr); ok {
				perr.LineOffset = i
			}
			return i, nil, err
		}
		cmd.set(metric, vals...)
	}
	return i, cmd, nil
}

func (t *Test) parseEval(lines []string, i int) (int, *evalCmd, error) {
	if !patEvalInstant.MatchString(lines[i]) {
		return i, nil, raise(i, "invalid evaluation command. (eval[_fail|_ordered] instant [at <offset:duration>] <query>")
	}
	parts := patEvalInstant.FindStringSubmatch(lines[i])
	var (
		mod  = parts[1]
		at   = parts[2]
		expr = parts[3]
	)
	_, err := parser.ParseExpr(expr)
	if err != nil {
		if perr, ok := err.(*parser.ParseErr); ok {
			perr.LineOffset = i
			posOffset := parser.Pos(strings.Index(lines[i], expr))
			perr.PositionRange.Start += posOffset
			perr.PositionRange.End += posOffset
			perr.Query = lines[i]
		}
		return i, nil, err
	}

	offset, err := model.ParseDuration(at)
	if err != nil {
		return i, nil, raise(i, "invalid step definition %q: %s", parts[1], err)
	}
	ts := testStartTime.Add(time.Duration(offset))

	cmd := newEvalCmd(expr, ts, i+1)
	switch mod {
	case "ordered":
		cmd.ordered = true
	case "fail":
		cmd.fail = true
	}

	for j := 1; i+1 < len(lines); j++ {
		i++
		defLine := lines[i]
		if len(defLine) == 0 {
			i--
			break
		}
		if f, err := parseNumber(defLine); err == nil {
			cmd.expect(0, nil, parser.SequenceValue{Value: f})
			break
		}
		metric, vals, err := parser.ParseSeriesDesc(defLine)
		if err != nil {
			if perr, ok := err.(*parser.ParseErr); ok {
				perr.LineOffset = i
			}
			return i, nil, err
		}

		// Currently, we are not expecting any matrices.
		if len(vals) > 1 {
			return i, nil, raise(i, "expecting multiple values in instant evaluation not allowed")
		}
		cmd.expect(j, metric, vals...)
	}
	return i, cmd, nil
}

// getLines returns trimmed lines after removing the comments.
func getLines(input string) []string {
	lines := strings.Split(input, "\n")
	for i, l := range lines {
		l = strings.TrimSpace(l)
		if strings.HasPrefix(l, "#") {
			l = ""
		}
		lines[i] = l
	}
	return lines
}

// parse the given command sequence and appends it to the test.
func (t *Test) parse(input string) error {
	lines := getLines(input)
	var err error
	// Scan for steps line by line.
	for i := 0; i < len(lines); i++ {
		l := lines[i]
		if len(l) == 0 {
			continue
		}
		var cmd testCommand

		switch c := strings.ToLower(patSpace.Split(l, 2)[0]); {
		case c == "clear":
			cmd = &clearCmd{}
		case c == "load":
			i, cmd, err = t.parseLoad(lines, i)
		case strings.HasPrefix(c, "eval"):
			i, cmd, err = t.parseEval(lines, i)
		default:
			return raise(i, "invalid command %q", l)
		}
		if err != nil {
			return err
		}
		t.cmds = append(t.cmds, cmd)
	}
	return nil
}

// testCommand is an interface that ensures that only the package internal
// types can be a valid command for a test.
type testCommand interface {
	testCmd()
}

func (*clearCmd) testCmd() {}
func (*loadCmd) testCmd()  {}
func (*evalCmd) testCmd()  {}

// loadCmd is a command that loads sequences of sample values for specific
// metrics into the storage.
type loadCmd struct {
	gap          time.Duration
	metrics      map[uint64]labels.Labels
	defs         map[uint64][]promql.Point
	m3compClient *m3comparatorClient
}

func newLoadCmd(m3compClient *m3comparatorClient, gap time.Duration) *loadCmd {
	return &loadCmd{
		gap:          gap,
		metrics:      map[uint64]labels.Labels{},
		defs:         map[uint64][]promql.Point{},
		m3compClient: m3compClient,
	}
}

func (cmd loadCmd) String() string {
	return "load"
}

// set a sequence of sample values for the given metric.
func (cmd *loadCmd) set(m labels.Labels, vals ...parser.SequenceValue) {
	h := m.Hash()

	samples := make([]promql.Point, 0, len(vals))
	ts := testStartTime
	for _, v := range vals {
		if !v.Omitted {
			samples = append(samples, promql.Point{
				T: ts.UnixNano() / int64(time.Millisecond/time.Nanosecond),
				V: v.Value,
			})
		}
		ts = ts.Add(cmd.gap)
	}
	cmd.defs[h] = samples
	cmd.metrics[h] = m
}

// append the defined time series to the storage.
func (cmd *loadCmd) append() error {
	series := make([]cparser.Series, 0, len(cmd.defs))

	for h, smpls := range cmd.defs {
		m := cmd.metrics[h]
		start := time.Unix(0, startingTime)

		ser := cparser.Series{
			Tags:       make(cparser.Tags, 0, len(m)),
			Start:      start,
			Datapoints: make(cparser.Datapoints, 0, len(smpls)),
		}
		for _, l := range m {
			ser.Tags = append(ser.Tags, cparser.NewTag(l.Name, l.Value))
		}

		for _, s := range smpls {
			ts := start.Add(time.Duration(s.T) * time.Millisecond)
			ser.Datapoints = append(ser.Datapoints, cparser.Datapoint{
				Timestamp: ts,
				Value:     cparser.Value(s.V),
			})

			ser.End = ts.Add(cmd.gap * time.Millisecond)
		}
		series = append(series, ser)
	}

	j, err := json.Marshal(series)
	if err != nil {
		return err
	}

	return cmd.m3compClient.load(j)
}

// evalCmd is a command that evaluates an expression for the given time (range)
// and expects a specific result.
type evalCmd struct {
	expr  string
	start time.Time
	line  int

	fail, ordered bool

	metrics  map[uint64]labels.Labels
	expected map[uint64]entry
	m3query  *m3queryClient
}

type entry struct {
	pos  int
	vals []parser.SequenceValue
}

func (e entry) String() string {
	return fmt.Sprintf("%d: %s", e.pos, e.vals)
}

func newEvalCmd(expr string, start time.Time, line int) *evalCmd {
	return &evalCmd{
		expr:  expr,
		start: start,
		line:  line,

		metrics:  map[uint64]labels.Labels{},
		expected: map[uint64]entry{},
		m3query:  newM3QueryClient("localhost", 7201),
	}
}

func (ev *evalCmd) String() string {
	return "eval"
}

// expect adds a new metric with a sequence of values to the set of expected
// results for the query.
func (ev *evalCmd) expect(pos int, m labels.Labels, vals ...parser.SequenceValue) {
	if m == nil {
		ev.expected[0] = entry{pos: pos, vals: vals}
		return
	}
	h := m.Hash()
	ev.metrics[h] = m
	ev.expected[h] = entry{pos: pos, vals: vals}
}

// Hash returns a hash value for the label set.
func hash(ls prometheus.Tags) uint64 {
	lbs := make(labels.Labels, 0, len(ls))
	for k, v := range ls {
		lbs = append(lbs, labels.Label{
			Name:  k,
			Value: v,
		})
	}

	sort.Slice(lbs[:], func(i, j int) bool {
		return lbs[i].Name < lbs[j].Name
	})

	return lbs.Hash()
}

// compareResult compares the result value with the defined expectation.
func (ev *evalCmd) compareResult(j []byte) error {
	var response prometheus.Response
	err := json.Unmarshal(j, &response)
	if err != nil {
		return err
	}

	if response.Status != "success" {
		return fmt.Errorf("unsuccess status received: %s", response.Status)
	}

	result := response.Data.Result

	switch result := result.(type) {
	case *prometheus.MatrixResult:
		return errors.New("received range result on instant evaluation")

	case *prometheus.VectorResult:
		seen := map[uint64]bool{}
		for pos, v := range result.Result {
			fp := hash(v.Metric)
			if _, ok := ev.metrics[fp]; !ok {
				return errors.Errorf("unexpected metric %s in result", v.Metric)
			}

			exp := ev.expected[fp]
			if ev.ordered && exp.pos != pos+1 {
				return errors.Errorf("expected metric %s with %v at position %d but was at %d", v.Metric, exp.vals, exp.pos, pos+1)
			}
			val, err := parseNumber(fmt.Sprint(v.Value[1]))
			if err != nil {
				return err
			}
			if !almostEqual(exp.vals[0].Value, val) {
				return errors.Errorf("expected %v for %s but got %v", exp.vals[0].Value, v.Metric, val)
			}
			seen[fp] = true
		}

		for fp, expVals := range ev.expected {
			if !seen[fp] {
				fmt.Println("vector result", len(result.Result), ev.expr)
				for _, ss := range result.Result {
					fmt.Println("    ", ss.Metric, ss.Value)
				}
				return errors.Errorf("expected metric %s with %v not found", ev.metrics[fp], expVals)
			}
		}

	case *prometheus.ScalarResult:
		v, err := parseNumber(fmt.Sprint(result.Result[1]))
		if err != nil {
			return err
		}
		if len(ev.expected) == 0 || len(ev.expected[0].vals) == 0 {
			return errors.Errorf("expected no Scalar value but got %v", v)
		}
		expected := ev.expected[0].vals[0].Value
		if !almostEqual(expected, v) {
			return errors.Errorf("expected Scalar %v but got %v", expected, v)
		}

	default:
		panic(errors.Errorf("promql.Test.compareResult: unexpected result type %T", result))
	}

	return nil
}

// clearCmd is a command that wipes the test's storage state.
type clearCmd struct{}

func (cmd clearCmd) String() string {
	return "clear"
}

// Run executes the command sequence of the test. Until the maximum error number
// is reached, evaluation errors do not terminate execution.
func (t *Test) Run() error {
	for _, cmd := range t.cmds {
		// TODO(fabxc): aggregate command errors, yield diffs for result
		// comparison errors.
		if err := t.exec(cmd); err != nil {
			return err
		}
	}
	return nil
}

// exec processes a single step of the test.
func (t *Test) exec(tc testCommand) error {
	switch cmd := tc.(type) {
	case *clearCmd:
		return t.clear()

	case *loadCmd:
		return cmd.append()

	case *evalCmd:
		expr, err := parser.ParseExpr(cmd.expr)
		if err != nil {
			return err
		}

		t := time.Unix(0, startingTime+(cmd.start.Unix()*1000000000))
		bodyBytes, err := cmd.m3query.query(expr.String(), t)
		if err != nil {
			if cmd.fail {
				return nil
			}
			return errors.Wrapf(err, "error in %s %s, line %d", cmd, cmd.expr, cmd.line)
		}
		if cmd.fail {
			return fmt.Errorf("expected to fail at %s %s, line %d", cmd, cmd.expr, cmd.line)
		}

		err = cmd.compareResult(bodyBytes)
		if err != nil {
			return errors.Wrapf(err, "error in %s %s, line %d. m3query response: %s", cmd, cmd.expr, cmd.line, string(bodyBytes))
		}

	default:
		panic("promql.Test.exec: unknown test command type")
	}
	return nil
}

// clear the current test storage of all inserted samples.
func (t *Test) clear() error {
	return t.m3comparator.clear()
}

// Close closes resources associated with the Test.
func (t *Test) Close() {
}

// almostEqual returns true if the two sample lines only differ by a
// small relative error in their sample value.
func almostEqual(a, b float64) bool {
	// NaN has no equality but for testing we still want to know whether both values
	// are NaN.
	if math.IsNaN(a) && math.IsNaN(b) {
		return true
	}

	// Cf. http://floating-point-gui.de/errors/comparison/
	if a == b {
		return true
	}

	diff := math.Abs(a - b)

	if a == 0 || b == 0 || diff < minNormal {
		return diff < epsilon*minNormal
	}
	return diff/(math.Abs(a)+math.Abs(b)) < epsilon
}

func parseNumber(s string) (float64, error) {
	n, err := strconv.ParseInt(s, 0, 64)
	f := float64(n)
	if err != nil {
		f, err = strconv.ParseFloat(s, 64)
	}
	if err != nil {
		return 0, errors.Wrap(err, "error parsing number")
	}
	return f, nil
}
