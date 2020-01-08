package database

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
	CmdFlags   *flag.FlagSet
	createYaml = flagvar.File{}

	defaultPath = "/api/v1/database"
	debugQS     = "debug=true"
)

//curl -X POST http://localhost:7201/api/v1/database/create -d

func init() {
	CmdFlags = flag.NewFlagSet("db", flag.ExitOnError)
	CmdFlags.Var(&createYaml, "create", "Path to yaml for simplified db creation with sane defaults.")
	CmdFlags.Usage = func() {
		fmt.Fprintf(CmdFlags.Output(), `
Description:

database tasks


Usage of %s:

`, CmdFlags.Name())
		CmdFlags.PrintDefaults()
	}
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

	if len(createYaml.Value) < 1 {
		CmdFlags.Usage()
		os.Exit(1)
	}

	data := common.LoadYaml(createYaml.Value, &admin.DatabaseCreateRequest{}, log)

	url := fmt.Sprintf("%s%s/create", *common.EndPoint, defaultPath)

	common.DoPost(url, data, log, common.DoDump)

	return
}
