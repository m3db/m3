package placements

import (
	"flag"
	"fmt"
	"github.com/m3db/m3/src/cmd/tools/m3db_tool/main/http"
	"github.com/m3db/m3/src/cmd/tools/m3db_tool/main/yaml"
	"github.com/m3db/m3/src/query/generated/proto/admin"
	"github.com/m3db/m3/src/x/config/configflag"
	"go.uber.org/zap"
	"os"
)

const (
	placementCommandName = "pl"
	defaultPath          = "/api/v1/services/m3db/placement"

	// flag names
	DeleteAllName   = "deleteAll"
	DeleteNodeName  = "deleteNode"
	InitName        = "init"
	NewNodeName     = "newNode"
	ReplaceNodeName = "replaceNode"
)

type PlacementArgs struct {
	deletePlacement *bool
	deleteNode      *string
	initFlag        configflag.FlagStringSlice
	newNodeFlag     configflag.FlagStringSlice
	replaceFlag     configflag.FlagStringSlice
}

func Command(flags *PlacementArgs, endpoint string, log *zap.SugaredLogger) {
	log.Debugf("PlacementArgs:%+v:\n", flags)
	if len(*flags.deleteNode) > 0 {
		url := fmt.Sprintf("%s%s/%s", endpoint, defaultPath, *flags.deleteNode)
		http.DoDelete(url, log, http.DoDump)
	} else if len(flags.newNodeFlag.Value) > 0 && len(flags.newNodeFlag.Value) > 0 {
		data := yaml.Load(flags.newNodeFlag.Value[0], &admin.PlacementInitRequest{}, log)
		url := fmt.Sprintf("%s%s", endpoint, defaultPath)
		http.DoPost(url, data, log, http.DoDump)
	} else if len(flags.initFlag.Value) > 0 && len(flags.initFlag.Value[0]) > 0 {
		data := yaml.Load(flags.initFlag.Value[0], &admin.PlacementInitRequest{}, log)
		url := fmt.Sprintf("%s%s%s", endpoint, defaultPath, "/init")
		http.DoPost(url, data, log, http.DoDump)
	} else if len(flags.replaceFlag.Value) > 0 && len(flags.replaceFlag.Value[0]) > 0 {
		data := yaml.Load(flags.replaceFlag.Value[0], &admin.PlacementReplaceRequest{}, log)
		url := fmt.Sprintf("%s%s%s", endpoint, defaultPath, "/replace")
		http.DoPost(url, data, log, http.DoDump)
	} else if *flags.deletePlacement {
		url := fmt.Sprintf("%s%s", endpoint, defaultPath)
		http.DoDelete(url, log, http.DoDump)
	} else {
		url := fmt.Sprintf("%s%s", endpoint, defaultPath)
		http.DoGet(url, log, http.DoDump)
	}
	return
}

func SetupPlacementFlags(flags *PlacementArgs) *flag.FlagSet {
	placementFlags := flag.NewFlagSet(placementCommandName, flag.ExitOnError)
	flags.deletePlacement = placementFlags.Bool(DeleteAllName, false, "delete all instances in the placement")
	flags.deleteNode = placementFlags.String(DeleteNodeName, "", "delete the specified node in the placement")
	placementFlags.Var(&flags.initFlag, InitName, "initialize a placement. Specify a yaml file.")
	placementFlags.Var(&flags.newNodeFlag, NewNodeName, "add a new node to the placement. Specify the filename of the yaml.")
	placementFlags.Var(&flags.replaceFlag, ReplaceNodeName, "add a new node to the placement. Specify the filename of the yaml.")
	placementFlags.Usage = func() {
		fmt.Fprintf(os.Stderr, `
"%s" is for acting on placements.

Description:

The subcommand "%s" provides the ability to:

* delete an entire placement from a node
* remove a node from a placement
* add a new node to as existing placement
* replace a node within an existing placement
* initialize a placement

Default behaviour (no arguments) is to provide a json dump of the existing placement.

New node creation and node replacement require specification of
the desired placement parameters, which you are to provide via a yaml
file, the pathname of which is the argument for the cli option.

Specify only one action at a time.

Usage of %s:

`, placementFlags.Name(), placementFlags.Name(), placementFlags.Name())
		placementFlags.PrintDefaults()
	}
	return placementFlags
}
