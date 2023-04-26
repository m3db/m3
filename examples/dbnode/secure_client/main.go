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
	"flag"
	"io/ioutil"
	"log"
	"time"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

	yaml "gopkg.in/yaml.v2"
)

const (
	namespace = "default"
)

var (
	namespaceID = ident.StringID(namespace)
)

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

	defer func() {
		err = session.Close()

		if err != nil {
			log.Fatalf("unable to close new M3DB session: %v", err)
		}
	}()

	runExample(session)
}

// runExample demonstrates how to write metrics data and then read it back out again
func runExample(session client.Session) {
	var (
		seriesID = ident.StringID("{__name__=\"metric_001\"}")
	)

	// Write a series ID using millisecond precision.
	timestamp := xtime.Now()
	value := 42.0

	log.Printf("Writing to series: %s", seriesID)
	err := session.Write(namespaceID, seriesID, timestamp, value, xtime.Millisecond, nil)
	if err != nil {
		log.Fatalf("error writing series %s, err: %v", seriesID.String(), err)
	}

	// Fetch data for the series ID
	end := xtime.Now()
	start := end.Add(-time.Minute)

	log.Printf("Fetching from series: %s", seriesID)
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
