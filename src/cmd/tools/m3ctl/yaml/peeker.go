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
	"github.com/m3db/m3/src/query/generated/proto/admin"
)

// peek into the yaml to see what it is expected to be
//
// returns the url path, proto.Message, and error
func peeker(data []byte) (string, proto.Message, error) {
	type peeker struct {
		Operation string
		Service   string
	}
	peek := &peeker{}

	// this really does nothing more than unpack into the above
	// private type to take a peek at Operation
	// its just a peek
	if err := yaml.Unmarshal(data, &peek); err != nil {
		return "", nil, err
	}

	// now the payload is of known type
	// unmarshal it and return the proto.Message
	if peek.Service == "" {
		peek.Service = "m3db"
	}
	placementPath := placements.DefaultPath + peek.Service + "/placement"

	switch peek.Operation {
	case opCreate:
		payload := struct{ Request admin.DatabaseCreateRequest }{}
		if err := yaml.Unmarshal(data, &payload); err != nil {
			return "", nil, err
		}
		return dbcreatePath, &payload.Request, nil
	case opInit:
		payload := struct{ Request admin.PlacementInitRequest }{}
		if err := yaml.Unmarshal(data, &payload); err != nil {
			return "", nil, err
		}
		return fmt.Sprintf("%s/init", placementPath), &payload.Request, nil
	case opReplace:
		payload := struct{ Request admin.PlacementReplaceRequest }{}
		if err := yaml.Unmarshal(data, &payload); err != nil {
			return "", nil, err
		}
		return fmt.Sprintf("%s/replace", placementPath), &payload.Request, nil
	case opNewNode:
		payload := struct{ Request admin.PlacementInitRequest }{}
		if err := yaml.Unmarshal(data, &payload); err != nil {
			return "", nil, err
		}
		return placementPath, &payload.Request, nil
	default:
		return "", nil, fmt.Errorf("unknown operation specified in the yaml")
	}

}
