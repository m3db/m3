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

package yaml

import (
	"io/ioutil"
	"testing"

	"github.com/gogo/protobuf/jsonpb"
	pb "github.com/m3db/m3/src/cmd/tools/m3ctl/yaml/generated"
)

// this uses load to get an encoded stream of the
// structure, then unmarshals it back into a struct
// and verifies the unmarshalled struct matches
// what was specified in the yaml
func TestLoadBasic(t *testing.T) {
	content, err := ioutil.ReadFile("./testdata/basic_create.yaml")
	if err != nil {
		t.Fatalf("failed to read yaml test data:%v:\n", err)
	}
	// load the yaml and encode it
	source := pb.DatabaseCreateRequestYaml{}
	data, err := load(content, &source)
	if err != nil {
		t.Fatalf("failed to encode the basic test data:%v:\n", err)
	}
	dest := pb.DatabaseCreateRequestYaml{}
	unmarshaller := &jsonpb.Unmarshaler{AllowUnknownFields: true}
	// unmarshal the stream back into a struct and verify it
	if err := unmarshaller.Unmarshal(data, &dest); err != nil {
		t.Fatalf("failed to unmarshal basic test data:%v:\n", err)
	}
	if dest.Operation != opCreate {
		t.Errorf("dest type does not have the correct type:expected:%v:got:%v:", opCreate, dest.Operation)
	}
	if dest.Request.ReplicationFactor != 327 {
		t.Errorf("in and out ReplicationFactor did not match:expected:%d:got:%d:\n", source.Request.ReplicationFactor, dest.Request.ReplicationFactor)
	}
	if dest.Request.ReplicationFactor != source.Request.ReplicationFactor {
		t.Errorf("in and out ReplicationFactor did not match:expected:%d:got:%d:\n", source.Request.ReplicationFactor, dest.Request.ReplicationFactor)
	}
	if dest.Request.NamespaceName != "default" {
		t.Errorf("namespace is wrong:expected:%s:got:%s:\n", "default", dest.Request.NamespaceName)
	}
	if len(dest.Request.Hosts) != 1 {
		t.Errorf("number of hosts is wrong:expected:%d:got:%d:\n", 1, len(dest.Request.Hosts))
	}
	if dest.Request.Hosts[0].Id != "m3db_seed" {
		t.Errorf("hostname is wrong:expected:%s:got:%s:\n", "m3db_seed", dest.Request.Hosts[0])
	}
}
