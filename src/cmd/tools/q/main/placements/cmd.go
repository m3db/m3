package placements

import (
	"flag"
	"fmt"
	"go.uber.org/zap"
	"os"

	"github.com/m3db/m3/src/cmd/tools/q/main/common"
	"github.com/m3db/m3/src/cmd/tools/q/main/flagvar"
	"github.com/m3db/m3/src/query/generated/proto/admin"
)

var (
	CmdFlags    *flag.FlagSet
	defaultPath = "/api/v1/services/m3db/placement"
	delete      *bool
	deleteNode  *string
	initFlag    = flagvar.File{}
	newNodeFlag = flagvar.File{}
	replaceFlag = flagvar.File{}
)

func init() {
	CmdFlags = flag.NewFlagSet("pl", flag.ExitOnError)
	delete = CmdFlags.Bool("deleteAll", false, "delete all instances in the placement")
	deleteNode = CmdFlags.String("deleteNode", "", "delete the specified node in the placement")
	CmdFlags.Var(&initFlag, "init", "initialize a placement. Specify a yaml file.")
	CmdFlags.Var(&newNodeFlag, "newNode", "add a new node to the placement. Specify the filename of the yaml.")
	CmdFlags.Var(&replaceFlag, "replaceNode", "add a new node to the placement. Specify the filename of the yaml.")

	CmdFlags.Usage = func() {
		fmt.Fprintf(CmdFlags.Output(), `
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

`, CmdFlags.Name(), CmdFlags.Name())
		CmdFlags.PrintDefaults()
	}
}

func Cmd(log *zap.SugaredLogger) {

	if err := CmdFlags.Parse(flag.Args()[1:]); err != nil {
		CmdFlags.Usage()
		os.Exit(1)
	}

	if CmdFlags.NFlag() > 1 {
		fmt.Fprintf(os.Stderr, "Please specify only one action.  There were too many cli arguments provided.\n")
		CmdFlags.Usage()
		os.Exit(1)
	}

	if len(*deleteNode) > 0 {

		url := fmt.Sprintf("%s%s/%s", *common.EndPoint, defaultPath, *deleteNode)

		common.DoDelete(url, log, common.DoDump)

	} else if len(newNodeFlag.Value) > 0 {

		data := common.LoadYaml(newNodeFlag.Value, &admin.PlacementInitRequest{}, log)

		url := fmt.Sprintf("%s%s", *common.EndPoint, defaultPath)

		common.DoPost(url, data, log, common.DoDump)

	} else if len(initFlag.Value) > 0 {

		data := common.LoadYaml(initFlag.Value, &admin.PlacementInitRequest{}, log)

		url := fmt.Sprintf("%s%s%s", *common.EndPoint, defaultPath, "/init")

		common.DoPost(url, data, log, common.DoDump)

	} else if len(replaceFlag.Value) > 0 {

		data := common.LoadYaml(replaceFlag.Value, &admin.PlacementReplaceRequest{}, log)

		url := fmt.Sprintf("%s%s%s", *common.EndPoint, defaultPath, "/replace")

		common.DoPost(url, data, log, common.DoDump)

	} else if *delete {

		url := fmt.Sprintf("%s%s", *common.EndPoint, defaultPath)

		common.DoDelete(url, log, common.DoDump)

	} else {

		url := fmt.Sprintf("%s%s", *common.EndPoint, defaultPath)

		common.DoGet(url, log, common.DoDump)

	}

	return
}
