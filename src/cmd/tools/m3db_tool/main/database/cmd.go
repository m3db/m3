package database

import (
	"flag"
	"fmt"
	"go.uber.org/zap"
	"os"

	"github.com/m3db/m3/src/cmd/tools/m3db_tool/main/common"
	"github.com/m3db/m3/src/cmd/tools/m3db_tool/main/flagvar"
	"github.com/m3db/m3/src/query/generated/proto/admin"
)

var (
	CmdFlags   *flag.FlagSet
	createYaml = flagvar.File{}

	defaultPath = "/api/v1/database"
	debugQS     = "debug=true"
)

func init() {
	CmdFlags = flag.NewFlagSet("db", flag.ExitOnError)
	CmdFlags.Var(&createYaml, "create", "Path to yaml for simplified db creation with sane defaults.")
	CmdFlags.Usage = func() {
		fmt.Fprintf(CmdFlags.Output(), `
This is the "%s" subcommand for database scoped operations.

Description:

This subcommand allows the creation of a new database from a yaml specification.

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

	if len(createYaml.Value) < 1 {
		CmdFlags.Usage()
		os.Exit(1)
	}

	data := common.LoadYaml(createYaml.Value, &admin.DatabaseCreateRequest{}, log)

	url := fmt.Sprintf("%s%s/create", *common.EndPoint, defaultPath)

	common.DoPost(url, data, log, common.DoDump)

	return
}
