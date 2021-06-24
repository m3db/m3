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

package main

import (
	"context"
	"flag"
	"io/ioutil"
	"log"
	"time"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/m3ninx/idx"
	xcontext "github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

	yaml "gopkg.in/yaml.v2"
)

const (
	namespace = "default"
)

var namespaceID = ident.StringID(namespace)

type config struct {
	Client client.Configuration `yaml:"client"`
}

var configFile = flag.String("f", "", "configuration file")

func main() {
	flag.Parse()
	if *configFile == "" {
		flag.Usage()
		return
	}

	cfgBytes, err := ioutil.ReadFile(*configFile)
	if err != nil {
		log.Fatalf("unable to read config file: %s, err: %v", *configFile, err)
	}

	cfg := &config{}
	if err := yaml.UnmarshalStrict(cfgBytes, cfg); err != nil {
		log.Fatalf("unable to parse YAML: %v", err)
	}

	client, err := cfg.Client.NewClient(client.ConfigurationParameters{})
	if err != nil {
		log.Fatalf("unable to create new M3DB client: %v", err)
	}

	session, err := client.DefaultSession()
	if err != nil {
		log.Fatalf("unable to create new M3DB session: %v", err)
	}
	defer session.Close()

	runTaggedExample(session)
}

// runTaggedExample demonstrates how to write "tagged" (indexed) metrics data
// and then read it back out again by either:
//
//   1. Querying for a set of time series using an inverted index query
//   2. Querying for a specific time series by its ID directly
func runTaggedExample(session client.Session) {
	log.Printf("------ run tagged example ------")
	var (
		seriesID = ident.StringID("{__name__=\"network_in\",host=\"host-01\",region=\"us-east-1\"}")
		tags     = []ident.Tag{
			{Name: ident.StringID("host"), Value: ident.StringID("host01")},
			{Name: ident.StringID("region"), Value: ident.StringID("us-east-1")},
		}
		tagsIter = ident.NewTagsIterator(ident.NewTags(tags...))
	)
	// Write a tagged series ID using millisecond precision.
	timestamp := xtime.Now()
	value := 42.0
	err := session.WriteTagged(
		xcontext.NewBackground(), namespaceID, seriesID, tagsIter,
		timestamp, value, xtime.Millisecond, nil)
	if err != nil {
		log.Fatalf("error writing series %s, err: %v", seriesID.String(), err)
	}

	// 1. Fetch data for the tagged seriesID using a query (only data written
	// within the last minute).
	end := xtime.Now()
	start := end.Add(-time.Minute)

	// Use regexp to filter on a single tag, use idx.NewConjunctionQuery to
	// to search on multiple tags, etc.
	reQuery, err := idx.NewRegexpQuery([]byte("host"), []byte("host[0-9]+"))
	if err != nil {
		log.Fatalf("error in creating query: %v", err)
	}

	resultsIter, _, err := session.FetchTagged(context.Background(), namespaceID, index.Query{Query: reQuery},
		index.QueryOptions{StartInclusive: start, EndExclusive: end})
	if err != nil {
		log.Fatalf("error fetching data for tagged series: %v", err)
	}
	for _, seriesIter := range resultsIter.Iters() {
		log.Printf("series: %s", seriesIter.ID().String())
		tags := seriesIter.Tags()
		for tags.Next() {
			tag := tags.Current()
			log.Printf("%s=%s", tag.Name.String(), tag.Value.String())
		}
		if err := tags.Err(); err != nil {
			log.Fatalf("error in tag iterator: %v", err)
		}
		for seriesIter.Next() {
			dp, _, _ := seriesIter.Current()
			log.Printf("%s: %v", dp.TimestampNanos.String(), dp.Value)
		}
		if err := seriesIter.Err(); err != nil {
			log.Fatalf("error in series iterator: %v", err)
		}
	}

	// 2. Fetch data for the series ID directly, skips the inverted index.
	seriesIter, err := session.Fetch(namespaceID, seriesID, start, end)
	if err != nil {
		log.Fatalf("error fetching data for untagged series: %v", err)
	}
	for seriesIter.Next() {
		dp, _, _ := seriesIter.Current()
		log.Printf("%s: %v", dp.TimestampNanos.String(), dp.Value)
	}
	if err := seriesIter.Err(); err != nil {
		log.Fatalf("error in series iterator: %v", err)
	}
}
