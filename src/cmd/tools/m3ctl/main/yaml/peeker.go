package yaml

import (
	"fmt"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/placements"
	"github.com/m3db/m3/src/query/generated/proto/admin"

	"github.com/ghodss/yaml"
	"github.com/gogo/protobuf/proto"
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
	if err := yaml.Unmarshal(data, &peek); err != nil {
		return "", nil, err
	}
	switch peek.Operation {
	case opCreate:
		return dbcreatePath, &admin.DatabaseCreateRequest{}, nil
	case opInit:
		return fmt.Sprintf("%s/init", placements.DefaultPath), &admin.PlacementInitRequest{}, nil
	case opReplace:
		return fmt.Sprintf("%s/replace", placements.DefaultPath), &admin.PlacementReplaceRequest{}, nil
	case opNewNode:
		return fmt.Sprintf("%s", placements.DefaultPath), &admin.PlacementInitRequest{}, nil
	default:
		return "", nil, fmt.Errorf("Unknown operation specified in the yaml\n")
	}
}
