package yaml

import (
	"fmt"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/placements"

	yaml2 "github.com/m3db/m3/src/cmd/tools/m3ctl/main/yaml/generated"

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
		return dbcreatePath, &yaml2.DatabaseCreateRequestYaml{}, nil
	case opInit:
		return fmt.Sprintf("%s/init", placements.DefaultPath), &yaml2.PlacementInitRequestYaml{}, nil
	case opReplace:
		return fmt.Sprintf("%s/replace", placements.DefaultPath), &yaml2.PlacementReplaceRequestYaml{}, nil
	case opNewNode:
		return fmt.Sprintf("%s", placements.DefaultPath), &yaml2.PlacementInitRequestYaml{}, nil
	default:
		return "", nil, fmt.Errorf("Unknown operation specified in the yaml\n")
	}
}
