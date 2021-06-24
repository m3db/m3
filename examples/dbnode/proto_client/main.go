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
	"sync"
	"time"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/encoding/proto"
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
	Client client.Configuration `yaml:"m3db_client"`
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

	// TODO(rartoul): Provide guidelines on reducing memory usage by tuning pooling options.
	client, err := cfg.Client.NewClient(client.ConfigurationParameters{})
	if err != nil {
		log.Fatalf("unable to create new M3DB client: %v", err)
	}

	session, err := client.DefaultSession()
	if err != nil {
		log.Fatalf("unable to create new M3DB session: %v", err)
	}
	defer session.Close()

	schemaConfig, ok := cfg.Client.Proto.SchemaRegistry[namespace]
	if !ok {
		log.Fatalf("schema path for namespace: %s not found", namespace)
	}

	// NB(rartoul): Using dynamic / reflection based library for marshaling and unmarshaling protobuf
	// messages for simplicity, use generated message-specific bindings in production.
	schema, err := proto.ParseProtoSchema(schemaConfig.SchemaFilePath, schemaConfig.MessageName)
	if err != nil {
		log.Fatalf("could not parse proto schema: %v", err)
	}

	runUntaggedExample(session, schema)
	runTaggedExample(session, schema)
	// TODO(rartoul): Add an aggregations query example.
}

// runUntaggedExample demonstrates how to write "untagged" (unindexed) data into M3DB with a given
// protobuf schema and then read it back out again.
func runUntaggedExample(session client.Session, schema *desc.MessageDescriptor) {
	log.Printf("------ run untagged example ------")
	var (
		untaggedSeriesID = ident.StringID("untagged_seriesID")
		m                = newTestValue(schema)
	)
	marshaled, err := m.Marshal()
	if err != nil {
		log.Fatalf("error marshaling protobuf message: %v", err)
	}

	// Write an untagged series ID. Pass 0 for value since it is ignored.
	if err := session.Write(namespaceID, untaggedSeriesID, xtime.Now(), 0, xtime.Nanosecond, marshaled); err != nil {
		log.Fatalf("unable to write untagged series: %v", err)
	}

	// Fetch data for the untagged seriesID written within the last minute.
	seriesIter, err := session.Fetch(namespaceID, untaggedSeriesID, xtime.Now().Add(-time.Minute), xtime.Now())
	if err != nil {
		log.Fatalf("error fetching data for untagged series: %v", err)
	}
	for seriesIter.Next() {
		m = dynamic.NewMessage(schema)
		dp, _, marshaledProto := seriesIter.Current()
		if err := m.Unmarshal(marshaledProto); err != nil {
			log.Fatalf("error unmarshaling protobuf message: %v", err)
		}
		log.Printf("%s: %s", dp.TimestampNanos.String(), m.String())
	}
	if err := seriesIter.Err(); err != nil {
		log.Fatalf("error in series iterator: %v", err)
	}
}

// runTaggedExample demonstrates how to write "tagged" (indexed) data into M3DB with a given protobuf
// schema and then read it back out again by either:
//
//   1. Querying for a specific time series by its ID directly
//   2. TODO(rartoul): Querying for a set of time series using an inverted index query
func runTaggedExample(session client.Session, schema *desc.MessageDescriptor) {
	log.Printf("------ run tagged example ------")
	var (
		seriesID = ident.StringID("vehicle_id_1")
		tags     = []ident.Tag{
			{Name: ident.StringID("type"), Value: ident.StringID("sedan")},
			{Name: ident.StringID("city"), Value: ident.StringID("san_francisco")},
		}
		tagsIter = ident.NewTagsIterator(ident.NewTags(tags...))
		m        = newTestValue(schema)
	)
	marshaled, err := m.Marshal()
	if err != nil {
		log.Fatalf("error marshaling protobuf message: %v", err)
	}

	// Write a tagged series ID. Pass 0 for value since it is ignored.
	if err := session.WriteTagged(xcontext.NewBackground(), namespaceID, seriesID, tagsIter, xtime.Now(), 0, xtime.Nanosecond, marshaled); err != nil {
		log.Fatalf("error writing series %s, err: %v", seriesID.String(), err)
	}

	// Fetch data for the tagged seriesID using a direct ID lookup (only data written within the last minute).
	seriesIter, err := session.Fetch(namespaceID, seriesID, xtime.Now().Add(-time.Minute), xtime.Now())
	if err != nil {
		log.Fatalf("error fetching data for untagged series: %v", err)
	}
	for seriesIter.Next() {
		m = dynamic.NewMessage(schema)
		dp, _, marshaledProto := seriesIter.Current()
		if err := m.Unmarshal(marshaledProto); err != nil {
			log.Fatalf("error unamrshaling protobuf message: %v", err)
		}
		log.Printf("%s: %s", dp.TimestampNanos.String(), m.String())
	}
	if err := seriesIter.Err(); err != nil {
		log.Fatalf("error in series iterator: %v", err)
	}

	// TODO(rartoul): Show an example of how to execute a FetchTagged() call with an index query.
}

var (
	testValueLock  sync.Mutex
	testValueCount = 1
)

func newTestValue(schema *desc.MessageDescriptor) *dynamic.Message {
	testValueLock.Lock()
	defer testValueLock.Unlock()

	m := dynamic.NewMessage(schema)
	m.SetFieldByName("latitude", float64(testValueCount))
	m.SetFieldByName("longitude", float64(testValueCount))
	m.SetFieldByName("fuel_percent", 0.75)
	m.SetFieldByName("status", "active")

	testValueCount++

	return m
}
