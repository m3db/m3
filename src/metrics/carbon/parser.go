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

	"github.com/m3db/m3x/unsafe"
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
	Name []byte
	Time time.Time
	Val  float64
	Recv time.Time
}

// ToLine converts the carbon Metric struct to a line.
func (m *Metric) ToLine() string {
	return string(m.Name) + " " + strconv.FormatFloat(m.Val, floatFormatByte, floatPrecision, floatBitSize) +
		" " + strconv.FormatInt(m.Time.Unix(), intBase) + "\n"
}

// ParsePacket parses a carbon packet and returns the metrics and number of malformed lines.
func ParsePacket(packet []byte) ([]Metric, int) {
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
func ParseName(line []byte) (name []byte, rest []byte, err error) {
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
	if !utf8.Valid(name) {
		err = errNotUTF8
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
func ParseRemainder(rest []byte) (timestamp time.Time, value float64, err error) {
	if !utf8.Valid(rest) {
		err = errNotUTF8
		return
	}

	// Determine the start and end offsets for the value.
	valStart, valEnd := parseWordOffsets(rest)
	if valStart == -1 || valEnd == -1 || valEnd >= len(rest) {
		// If we couldn't determine the offsets, or the end of the value is also
		// the end of the line, then this is an invalid line.
		err = errInvalidLine
		return
	}

	// Found valid offsets for the value, try and parse it into a float. Note that
	// we use unsafe.WithString() so that we can use standard library functions
	// without allocating a string.
	unsafe.WithString(rest, func(s string) {
		if val := strings.ToLower(s[valStart:valEnd]); val == negativeNanStr || val == nanStr {
			value = mathNan
		} else {
			value, err = strconv.ParseFloat(s[valStart:valEnd], 64)
		}
	})
	if err != nil {
		return
	}

	// Determine the start and end offsets for the timestamp (seconds).
	rest = rest[valEnd:]
	secStart, secEnd := parseWordOffsets(rest)

	if secStart == -1 || secEnd == -1 || secEnd != len(rest) {
		// If we couldn't determine the offsets, or the end of the the timestamp
		// is not the end of the line (I.E there are still characters after the end
		// of the timestamp), then this is an invalid line.
		err = errInvalidLine
		return
	}

	// Found valid offsets for the timestamp, try and parse it into an integer. Note that
	// we use unsafe.WithString() so that we can use standard library functions without
	// allocating a string.
	var tsInSecs int64
	unsafe.WithString(rest, func(s string) {
		tsInSecs, err = strconv.ParseInt(s[secStart:secEnd], 10, 64)
		if err != nil {
			err = fmt.Errorf("invalid timestamp %s: %v", rest[secStart:secEnd], err)
		}
	})
	if err != nil {
		return
	}
	timestamp = time.Unix(tsInSecs, 0)

	return
}

// Parse parses a carbon line into the corresponding parts.
func Parse(line []byte) (name []byte, timestamp time.Time, value float64, err error) {
	var rest []byte
	name, rest, err = ParseName(line)
	if err != nil {
		return
	}

	timestamp, value, err = ParseRemainder(rest)
	return
}

// A Scanner is used to scan carbon lines from an underlying io.Reader.
type Scanner struct {
	scanner   *bufio.Scanner
	timestamp time.Time
	path      []byte
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
		if s.path, s.timestamp, s.value, err = Parse(s.scanner.Bytes()); err != nil {
			s.MalformedCount++
			continue
		}

		return true
	}
}

// Metric returns the path, timestamp, and value of the last parsed metric.
func (s *Scanner) Metric() ([]byte, time.Time, float64) {
	return s.path, s.timestamp, s.value
}

// Err returns any errors in the scan.
func (s *Scanner) Err() error { return s.scanner.Err() }

// parseWordOffsets scans through b searching for the start and end offsets
// of the next "word" (ignores spaces on either side), returning offsets
// such that b[start:end] will return the complete word with no spaces. Note
// that the function will tolerate any number of spaces on either side.
func parseWordOffsets(b []byte) (int, int) {
	valStart := -1
	for i := 0; i < len(b); i++ {
		charByte := b[i]
		if valStart == -1 && charByte != ' ' {
			valStart = i
			break
		}
	}

	valEnd := -1
	reachedEnd := true
	for i := valStart + 1; i < len(b); i++ {
		valEnd = i

		charByte := b[i]
		if charByte == ' ' {
			reachedEnd = false
			break
		}
	}
	if reachedEnd {
		valEnd = valEnd + 1
	}

	return valStart, valEnd
}
