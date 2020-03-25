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
	"fmt"
	"github.com/ghodss/yaml"
	"github.com/gogo/protobuf/proto"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/placements"
	pb "github.com/m3db/m3/src/cmd/tools/m3ctl/yaml/generated"
)

// peek into the yaml to see what it is expected to be
// don't try to decode the entire thing since its
// going into something that's currently unknown
// so just grab the "operation" then dispatch
//
// returns the url path, proto.Message, and error
func peeker(data []byte) (string, proto.Message, error) {
	type peeker struct {
		Operation string
	}
	peek := &peeker{}

	// this really does nothing more than unpack into the above
	// private type to take a peek at Operation
	// no data is saved from this
	// nothing is returned from thie
	// its just a peek
	// and then it returns other data depending on the reults of the peek
	if err := yaml.Unmarshal(data, &peek); err != nil {
		return "", nil, err
	}
	switch peek.Operation {
	case opCreate:
		return dbcreatePath, &pb.DatabaseCreateRequestYaml{}, nil
	case opInit:
		return fmt.Sprintf("%s/init", placements.DefaultPath), &pb.PlacementInitRequestYaml{}, nil
	case opReplace:
		return fmt.Sprintf("%s/replace", placements.DefaultPath), &pb.PlacementReplaceRequestYaml{}, nil
	case opNewNode:
		return fmt.Sprintf("%s", placements.DefaultPath), &pb.PlacementInitRequestYaml{}, nil
	default:
		return "", nil, fmt.Errorf("Unknown operation specified in the yaml\n")
	}
}
