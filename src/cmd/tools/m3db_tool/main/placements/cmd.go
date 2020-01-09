package placements

import (
	"flag"
	"fmt"
	"github.com/m3db/m3/src/cmd/tools/m3db_tool/main/common"
	"github.com/m3db/m3/src/cmd/tools/m3db_tool/main/flagvar"
	"github.com/m3db/m3/src/query/generated/proto/admin"
	"go.uber.org/zap"
)

const (
	defaultPath = "/api/v1/services/m3db/placement"
)

type PlacementArgs struct {
	deletePlacement *bool
	deleteNode      *string
	initFlag        flagvar.File
	newNodeFlag     flagvar.File
	replaceFlag     flagvar.File
}

func Cmd(flags *PlacementArgs, endpoint string, log *zap.SugaredLogger) {

	if len(*flags.deleteNode) > 0 {

		url := fmt.Sprintf("%s%s/%s", endpoint, defaultPath, *flags.deleteNode)

		common.DoDelete(url, log, common.DoDump)

	} else if len(flags.newNodeFlag.Value) > 0 {

		data := common.LoadYaml(flags.newNodeFlag.Value, &admin.PlacementInitRequest{}, log)

		url := fmt.Sprintf("%s%s", endpoint, defaultPath)

		common.DoPost(url, data, log, common.DoDump)

	} else if len(flags.initFlag.Value) > 0 {

		data := common.LoadYaml(flags.initFlag.Value, &admin.PlacementInitRequest{}, log)

		url := fmt.Sprintf("%s%s%s", endpoint, defaultPath, "/init")

		common.DoPost(url, data, log, common.DoDump)

	} else if len(flags.replaceFlag.Value) > 0 {

		data := common.LoadYaml(flags.replaceFlag.Value, &admin.PlacementReplaceRequest{}, log)

		url := fmt.Sprintf("%s%s%s", endpoint, defaultPath, "/replace")

		common.DoPost(url, data, log, common.DoDump)

	} else if *flags.deletePlacement {

		url := fmt.Sprintf("%s%s", endpoint, defaultPath)

		common.DoDelete(url, log, common.DoDump)

	} else {

		url := fmt.Sprintf("%s%s", endpoint, defaultPath)

		common.DoGet(url, log, common.DoDump)

	}

	return
}

func SetupPlacementFlags(flags *PlacementArgs) *flag.FlagSet {

	placementCmdFlags := flag.NewFlagSet("pl", flag.ExitOnError)
	flags.deletePlacement = placementCmdFlags.Bool("deleteAll", false, "delete all instances in the placement")
	flags.deleteNode = placementCmdFlags.String("deleteNode", "", "delete the specified node in the placement")
	placementCmdFlags.Var(&flags.initFlag, "init", "initialize a placement. Specify a yaml file.")
	placementCmdFlags.Var(&flags.newNodeFlag, "newNode", "add a new node to the placement. Specify the filename of the yaml.")
	placementCmdFlags.Var(&flags.replaceFlag, "replaceNode", "add a new node to the placement. Specify the filename of the yaml.")

	placementCmdFlags.Usage = func() {
		fmt.Fprintf(placementCmdFlags.Output(), `
This is the subcommand for acting on placements.

Description:

The subcommand "%s"" provides the ability to:

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

`, placementCmdFlags.Name(), placementCmdFlags.Name())
		placementCmdFlags.PrintDefaults()
	}

	return placementCmdFlags
}
