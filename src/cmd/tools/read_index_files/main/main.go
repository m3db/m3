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

package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strconv"
	"time"

	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/m3ninx/doc"
	m3ninxpersist "github.com/m3db/m3/src/m3ninx/persist"
	"github.com/m3db/m3x/ident"
	xlog "github.com/m3db/m3x/log"

	"github.com/pborman/getopt"
)

func main() {
	var (
		optPathPrefix      = getopt.StringLong("path-prefix", 'p', "/var/lib/m3db", "Path prefix [e.g. /var/lib/m3db]")
		optNamespace       = getopt.StringLong("namespace", 'n', "metrics", "Namespace [e.g. metrics]")
		optBlockstart      = getopt.Int64Long("block-start", 'b', 0, "Block Start Time [in nsec]")
		optVolumeIndex     = getopt.Int64Long("volume-index", 'v', 0, "Volume index")
		optLargeFieldLimit = getopt.Int64Long("large-field-limit", 'l', 0, "Large Field Limit (non-zero to display fields with num terms > limit)")
		optOutputIdsPrefix = getopt.StringLong("output-ids-prefix", 'o', "", "If set, it emits all terms for the _m3ninx_id field.")
		log                = xlog.NewLogger(os.Stderr)
	)
	getopt.Parse()

	if *optPathPrefix == "" ||
		*optNamespace == "" ||
		*optBlockstart <= 0 {
		getopt.Usage()
		os.Exit(1)
	}

	fsOpts := fs.NewOptions().SetFilePathPrefix(*optPathPrefix)
	reader, err := fs.NewIndexReader(fsOpts)
	if err != nil {
		log.Fatalf("could not create new index reader: %v", err)
	}

	openOpts := fs.IndexReaderOpenOptions{
		Identifier: fs.FileSetFileIdentifier{
			FileSetContentType: persist.FileSetIndexContentType,
			Namespace:          ident.StringID(*optNamespace),
			BlockStart:         time.Unix(0, *optBlockstart),
			VolumeIndex:        int(*optVolumeIndex),
		},
	}

	_, err = reader.Open(openOpts)
	if err != nil {
		log.Fatalf("unable to open reader: %v", err)
	}

	i := 0
	for {
		i++
		fileset, err := reader.ReadSegmentFileSet()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("unable to retrieve fileset: %v", err)
		}

		seg, err := m3ninxpersist.NewSegment(fileset, fsOpts.FSTOptions())
		if err != nil {
			log.Fatalf("unable to open segment reader: %v", err)
		}
		defer seg.Close()

		var (
			idsFile   *os.File
			idsWriter *bufio.Writer
		)
		if *optOutputIdsPrefix != "" {
			idsFile, err = os.Create(fmt.Sprintf("%s-ids-segment-%d.out", *optOutputIdsPrefix, i))
			if err != nil {
				log.Fatalf("unable to create output ids file: %v", err)
			}
			idsWriter = bufio.NewWriter(idsFile)
			defer func() {
				idsWriter.Flush()
				idsFile.Sync()
				if err := idsFile.Close(); err != nil {
					log.Fatalf("error closing ids file: %v", err)
				}
			}()
		}

		fields, err := seg.Fields()
		if err != nil {
			log.Fatalf("unable to retrieve segment fields: %v", err)
		}

		type largeField struct {
			field    string
			numTerms int
		}
		var largeFields []largeField
		var termLens ints
		var numFields int
		for fields.Next() {
			numFields++

			f := fields.Current()
			terms, err := seg.Terms(f)
			if err != nil {
				log.Fatalf("unable to retrieve segment term: %v", err)
			}

			numTerms := 0
			isNameField := bytes.Equal(doc.IDReservedFieldName, f)
			for terms.Next() {
				numTerms++

				if isNameField && idsWriter != nil {
					// ids output
					t := terms.Current()
					idsWriter.Write(t)
					idsWriter.WriteByte('\n')
				}
			}

			// large field output
			if *optLargeFieldLimit > 0 && numTerms > int(*optLargeFieldLimit) {
				largeFields = append(largeFields, largeField{
					field:    string(f),
					numTerms: numTerms,
				})
			}
			termLens = append(termLens, numTerms)
		}

		summary := termLens.summary()
		log.Infof("Segment: [%v], Size: [%v], NumFields: [%v], Num Terms: [%+v]", i,
			formatCommas(int(seg.Size())), formatCommas(numFields), summary)
		if *optLargeFieldLimit > 0 {
			log.Infof("Large fields: %+v", largeFields)
		}
	}
}

type summaryStats struct {
	max     float64
	min     float64
	average float64
	median  float64
}

type ints []int

func (vals ints) summary() summaryStats {
	res := summaryStats{}
	sort.Ints(vals)
	if len(vals)%2 == 1 {
		res.median = float64(vals[len(vals)/2])
	} else {
		res.median = (float64(vals[len(vals)/2]) + float64(vals[(1+len(vals))/2])) / 2
	}
	res.min = float64(vals[0])
	res.max = float64(vals[len(vals)-1])
	sum := 0
	for _, val := range vals {
		sum += val
	}
	res.average = float64(sum) / float64(len(vals))
	return res
}

func formatCommas(num int) string {
	str := strconv.Itoa(num)
	re := regexp.MustCompile(`(\d+)(\d{3})`)
	for i := 0; i < (len(str)-1)/3; i++ {
		str = re.ReplaceAllString(str, "$1,$2")
	}
	return str
}
