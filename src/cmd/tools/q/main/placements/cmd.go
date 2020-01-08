package placements

import (
	"flag"
	"fmt"
	"go.uber.org/zap"
	"io"
	"io/ioutil"
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
	newNode     = flagvar.File{}
)

//curl -s -k -v 'http://localhost:7201/api/v1/placement'

//curl -v -X DELETE bmcqueen-ld1:7201/api/v1/services/m3db/placement

func init() {
	CmdFlags = flag.NewFlagSet("pl", flag.ExitOnError)
	delete = CmdFlags.Bool("deleteAll", false, "delete all instances in the placement")
	deleteNode = CmdFlags.String("deleteNode", "", "delete the specified node in the placement")
	CmdFlags.Var(&initFlag, "init", "initialize a placement. Specify a yaml file.")
	CmdFlags.Var(&newNode, "newNode", "add a new node to the placement. Specify the filename of the yaml.")
	//CmdFlags.Var(&createYaml, "create", "Path to yaml for simplified db creation with sane defaults.")

	CmdFlags.Usage = func() {
		fmt.Fprintf(CmdFlags.Output(), `
Description:

placement tasks

Usage of %s:

`, CmdFlags.Name())
		CmdFlags.PrintDefaults()
	}
}

func doShowAll(in io.Reader, log *zap.SugaredLogger) {

	dat, err := ioutil.ReadAll(in)
	if err != nil {
		panic(err)
	}

	fmt.Println(string(dat))
}

func Cmd(log *zap.SugaredLogger) {

	if err := CmdFlags.Parse(flag.Args()[1:]); err != nil {
		CmdFlags.Usage()
		os.Exit(1)
	}

	//if CmdFlags.NFlag() > 4 {
	//	CmdFlags.Usage()
	//	os.Exit(1)
	//}

	if len(*deleteNode) > 0 {

		url := fmt.Sprintf("%s%s/%s", *common.EndPoint, defaultPath, *deleteNode)

		common.DoDelete(url, log, doShowAll)

	} else if len(newNode.Value) > 0 {

		data := common.LoadYaml(newNode.Value, &admin.PlacementInitRequest{}, log)

		url := fmt.Sprintf("%s%s", *common.EndPoint, defaultPath)

		common.DoPost(url, data, log, doShowAll)

	} else if len(initFlag.Value) > 0 {

		data := common.LoadYaml(initFlag.Value, &admin.PlacementInitRequest{}, log)

		url := fmt.Sprintf("%s%s%s", *common.EndPoint, defaultPath, "/init")

		common.DoPost(url, data, log, doShowAll)

	} else if *delete {

		url := fmt.Sprintf("%s%s", *common.EndPoint, defaultPath)

		common.DoDelete(url, log, doShowAll)

	} else {

		url := fmt.Sprintf("%s%s", *common.EndPoint, defaultPath)

		common.DoGet(url, log, doShowAll)

	}

	return
}
