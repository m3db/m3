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
