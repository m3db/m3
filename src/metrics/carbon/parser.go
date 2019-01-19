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

package carbon

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	xstrings "github.com/m3db/m3/src/x/strings"
)

const (
	negativeNanStr = "-nan"
	nanStr         = "nan"

	floatFormatByte = 'f'
	floatPrecision  = -1
	floatBitSize    = 64
	intBase         = 10
)

var (
	errInvalidLine      = errors.New("invalid line")
	errNotUTF8          = errors.New("not valid UTF8 string")
	recvValidTimeBuffer = time.Hour
	mathNan             = math.NaN()
)

// Metric represents a carbon metric.
type Metric struct {
	Name string
	Time time.Time
	Val  float64
	Recv time.Time
}

// ToLine converts the carbon Metric struct to a line.
func (m *Metric) ToLine() string {
	return m.Name + " " + strconv.FormatFloat(m.Val, floatFormatByte, floatPrecision, floatBitSize) +
		" " + strconv.FormatInt(m.Time.Unix(), intBase) + "\n"
}

// ParsePacket parses a carbon packet and returns the metrics and number of malformed lines.
func ParsePacket(packet string) ([]Metric, int) {
	var malformed, prevIdx, i int
	mets := []Metric{}
	for i = 0; i < len(packet); i++ {
		if packet[i] == '\n' {
			if (i - prevIdx) > 1 {
				name, timestamp, value, err := Parse(packet[prevIdx:i])
				if err == nil {
					mets = append(mets, Metric{
						Name: name,
						Time: timestamp,
						Val:  value,
					})
				} else {
					malformed++
				}
			}
			prevIdx = i + 1
		}
	}

	if (i - prevIdx) > 1 {
		name, timestamp, value, err := Parse(packet[prevIdx:i])
		if err == nil {
			mets = append(mets, Metric{
				Name: name,
				Time: timestamp,
				Val:  value,
			})
		} else {
			malformed++
		}
	}

	return mets, malformed
}

// ParseName parses out the name portion of a string and returns the
// name and the remaining portion of the line.
func ParseName(line string) (name string, rest string, err error) {
	firstSepIdx := -1
	for i := 0; i < len(line); i++ {
		if line[i] == ' ' && !(i != 0 && line[i-1] == ' ') {
			firstSepIdx = i
			break
		}
	}

	if firstSepIdx == -1 {
		err = errInvalidLine
		return
	}

	name = line[:firstSepIdx]
	if len(name) == 0 {
		err = errInvalidLine
		return
	}

	nonSpaceIdx := firstSepIdx + 1
	for nonSpaceIdx < len(line) && line[nonSpaceIdx] == ' ' {
		nonSpaceIdx++
	}

	rest = line[nonSpaceIdx:]
	return
}

// ParseRemainder parses a line's components (name and remainder) and returns
// all but the name and returns the timestamp of the metric, its value, the
// time it was received and any error encountered.
func ParseRemainder(name, rest string) (timestamp time.Time, value float64, err error) {
	if !utf8.ValidString(name) || !utf8.ValidString(rest) {
		err = errNotUTF8
		return
	}

	if strings.Contains(rest, "  ") {
		rest = xstrings.CondenseDuplicateChars(rest, ' ')
	}

	secIdx := -1
	lineLen := len(rest)
	for i := 0; i < lineLen; i++ {
		if rest[i] == ' ' && !(i != 0 && rest[i-1] == ' ') {
			secIdx = i
			break
		}
	}

	if secIdx == -1 {
		// Incorrect number of ' ' chars
		err = errInvalidLine
		return
	}

	tsEndIdx := lineLen

	var tsInSecs int64
	tsInSecs, err = strconv.ParseInt(rest[secIdx+1:tsEndIdx], 10, 64)
	if err != nil {
		err = fmt.Errorf("invalid timestamp %s: %v", rest[secIdx+1:tsEndIdx], err)
		return
	}
	timestamp = time.Unix(tsInSecs, 0)

	if val := strings.ToLower(rest[:secIdx]); val == negativeNanStr || val == nanStr {
		value = mathNan
	} else {
		value, err = strconv.ParseFloat(rest[:secIdx], 64)
	}

	return
}

// Parse parses a carbon line into the corresponding parts.
func Parse(line string) (name string, timestamp time.Time, value float64, err error) {
	var rest string
	name, rest, err = ParseName(line)
	if err != nil {
		return
	}

	timestamp, value, err = ParseRemainder(name, rest)
	return
}

// A Scanner is used to scan carbon lines from an underlying io.Reader.
type Scanner struct {
	scanner   *bufio.Scanner
	timestamp time.Time
	path      string
	value     float64
	recv      time.Time

	// The number of malformed metrics encountered.
	MalformedCount int
}

// NewScanner creates a new carbon scanner.
func NewScanner(r io.Reader) *Scanner {
	s := bufio.NewScanner(r)
	s.Split(bufio.ScanLines)
	return &Scanner{scanner: s}
}

// Scan scans for the next carbon metric. Malformed metrics are skipped but counted.
func (s *Scanner) Scan() bool {
	for {
		if !s.scanner.Scan() {
			return false
		}

		var err error
		if s.path, s.timestamp, s.value, err = Parse(s.scanner.Text()); err != nil {
			s.MalformedCount++
			continue
		}

		return true
	}
}

// Metric returns the path, timestamp, and value of the last parsed metric.
func (s *Scanner) Metric() (string, time.Time, float64) {
	return s.path, s.timestamp, s.value
}

// MetricAndRecvTime returns the path, timestamp, value and receive time of the last parsed metric.
func (s *Scanner) MetricAndRecvTime() (string, time.Time, float64) {
	return s.path, s.timestamp, s.value
}

// Err returns any errors in the scan.
func (s *Scanner) Err() error { return s.scanner.Err() }
