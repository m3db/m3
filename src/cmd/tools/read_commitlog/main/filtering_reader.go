package main

import (
	"errors"
	"io"
	"log"
	"strings"

	"github.com/m3db/m3/src/dbnode/persist/fs/commitlog"
)

type filteringReader struct {
	reader       commitlog.Reader
	idFilter     *string
	idSizeFilter *int
}

func newFilteringReader(path string, idFilter *string, idSizeFilter *int) (*filteringReader, error) {
	opts := commitlog.NewReaderOptions(commitlog.NewOptions(), false)
	reader := commitlog.NewReader(opts)

	_, err := reader.Open(path)
	if err != nil {
		return nil, err
	}
	return &filteringReader{reader: reader, idFilter: idFilter, idSizeFilter: idSizeFilter}, nil
}

func (c *filteringReader) Read() (commitlog.LogEntry, bool) {
	for {
		entry, err := c.reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		series := entry.Series
		if *c.idFilter != "" && !strings.Contains(series.ID.String(), *c.idFilter) {
			continue
		}
		if *c.idSizeFilter != 0 && len(series.ID.Bytes()) < *c.idSizeFilter {
			continue
		}
		return entry, true
	}
	return commitlog.LogEntry{}, false
}

func (c *filteringReader) Close() {
	if c != nil && c.reader != nil {
		if err := c.reader.Close(); err != nil {
			log.Fatalf("unable to close reader: %v", err)
		}
	}
}
