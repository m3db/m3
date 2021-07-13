package yio

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/golang/snappy"
	"github.com/m3db/m3/src/x/instrument"
	xio "github.com/m3db/m3/src/x/io"
	"github.com/uber-go/tally"
)

const (
	// defined in https://github.com/golang/snappy/blob/master/snappy.go
	chunkTypeStreamIdentifier = 0xff
)

var (
	// defined in https://github.com/golang/snappy/blob/master/snappy.go
	magicBody = []byte("sNaPpY")

	errMisreadHeaderSize = errors.New("snappy: header size misread")
	errReaderUnset       = errors.New("snappy: unset reader")
)

type trySnappyReader struct {
	err        error
	buf        []byte
	metrics    snappyReaderMetrics
	r          io.Reader
	readerInit bool
	isSnappy   bool
}

var _ xio.ResettableReader = (*trySnappyReader)(nil)

type snappyReaderMetrics struct {
	receivedSnappy          tally.Counter
	receivedSnappyUnencoded tally.Counter
}

func newSnappyReaderMetrics(scope tally.Scope) snappyReaderMetrics {
	scope = scope.SubScope("rw-network-compression")
	return snappyReaderMetrics{
		receivedSnappy: scope.
			Tagged(map[string]string{"status": "snappy"}).Counter("count"),
		receivedSnappyUnencoded: scope.
			Tagged(map[string]string{"status": "unencoded"}).Counter("count"),
	}
}

func newTrySnappyReader(
	reader io.Reader,
	iOpts instrument.Options,
) xio.ResettableReader {
	r := &trySnappyReader{
		// magicBoyd + 4 accounts for the initial header bytes preceeding the body.
		buf:     make([]byte, 0, len(magicBody)+4),
		metrics: newSnappyReaderMetrics(iOpts.MetricsScope()),
		r:       reader,
	}

	fmt.Println("Using the reader from new.")
	return r
}

func (r *trySnappyReader) Reset(reader io.Reader) {
	if reader == nil {
		r.err = errReaderUnset
		return
	}

	fmt.Println("Using the reader from reset.")
	r.err = nil
	r.readerInit = false
	r.buf = r.buf[:0]
	r.r = reader
}

func (r *trySnappyReader) Read(p []byte) (int, error) {
	fmt.Println("trying to read", r.readerInit, r.isSnappy)
	if r.err != nil {
		fmt.Println("already err", r.err)
		return 0, r.err
	}

	if !r.readerInit {
		r.readerInit = true

		fmt.Println("initializing")
		isSnappy, err := r.isSnappyEncoded()
		fmt.Println("initialized", isSnappy, err)
		if err != nil {
			r.err = err
			fmt.Println("error when checking if snappy encoded", err)
			return 0, r.err
		}

		r.isSnappy = isSnappy
		fmt.Println(isSnappy, "r.buf is", string(r.buf), r.buf, "!")
		r.r = newHeaderReader(r.buf, r.r)
		if isSnappy {
			fmt.Println("snappy message")
			r.metrics.receivedSnappy.Inc(1)
			r.r = snappy.NewReader(r.r)
		} else {
			fmt.Println("non snappy message")
			r.metrics.receivedSnappyUnencoded.Inc(1)
		}
	}

	fmt.Println("reading", r.isSnappy)
	return r.r.Read(p)
}

func tryReadFull(reader io.Reader, p []byte) (int, []byte, bool, error) {
	var (
		min = len(p)
		n   = 0
		err error
	)
	fmt.Println("trying read full")
	if len(p) < min {
		fmt.Println("XXX about to start reading")
		n, err := reader.Read(p)
		fmt.Println("XXX read out", n, "with err", err)
		return n, p, false, err
	}

	for n < min && err == nil {
		var nn int
		fmt.Println("    about to start reading", len(p), cap(p))
		nn, err = reader.Read(p[n:])
		fmt.Println("    read out", nn, "with err", err, "min", min)
		n += nn
	}

	if err != nil {
		if err == io.EOF {
			return n, p, false, nil
		}

		return n, p, false, err
	}

	if n < min {
		return n, p, false, nil
	}

	if n > min {
		return n, p, false, errMisreadHeaderSize
	}

	return n, p, true, nil
}

// isSnappyEncoded checks the header of the underlying reader to see
// if it matches the snappy format, defined at:
// https://github.com/google/snappy/blob/master/framing_format.txt
func (r *trySnappyReader) isSnappyEncoded() (bool, error) {
	n, buf, success, err := tryReadFull(r.r, r.buf[:4])
	fmt.Println("READ ONCE")
	r.buf = append(r.buf, buf...)
	if err != nil {
		return false, err
	}

	if n == 0 {
		return false, nil
	}

	if !success {
		return false, nil
	}

	chunkType := buf[0]
	if chunkType != chunkTypeStreamIdentifier {
		return false, nil
	}

	chunkLen := int(buf[1]) | int(buf[2])<<8 | int(buf[3])<<16
	// Section 4.1. Stream identifier (chunk type 0xff).
	// https://github.com/google/snappy/blob/master/framing_format.txt
	if chunkLen != len(magicBody) {
		return false, nil
	}

	nn, buf, success, err := tryReadFull(r.r, r.buf[4:4+len(magicBody)])
	r.buf = append(r.buf, buf...)
	n += nn
	if err != nil {
		return false, err
	}

	if !success {
		return false, nil
	}

	return bytes.Equal(buf, magicBody), nil
}
