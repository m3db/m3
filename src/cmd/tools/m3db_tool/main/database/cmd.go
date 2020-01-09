package database

import (
	"flag"
	"fmt"
	"github.com/m3db/m3/src/cmd/tools/m3db_tool/main/common"
	"github.com/m3db/m3/src/cmd/tools/m3db_tool/main/flagvar"
	"github.com/m3db/m3/src/query/generated/proto/admin"
	"go.uber.org/zap"
	"os"
	"strings"
)

const (
	defaultPath = "/api/v1/database"
)

func Cmd(createYaml string, endpoint string, log *zap.SugaredLogger) {

	data := common.LoadYaml(createYaml, &admin.DatabaseCreateRequest{}, log)

	url := fmt.Sprintf("%s%s/create", endpoint, defaultPath)

	common.DoPost(url, data, log, common.DoDump)

	return
}

func SetupDatabaseFlags(createDatabaseYaml *flagvar.File) *flag.FlagSet {

	databaseCmdFlags := flag.NewFlagSet("db", flag.ExitOnError)
	databaseCmdFlags.Var(createDatabaseYaml, "create", "Path to yaml for simplified db creation with sane defaults.")
	databaseCmdFlags.Usage = func() {
		fmt.Fprintf(databaseCmdFlags.Output(), `
This is the "%s" subcommand for database scoped operations.

Description:

This subcommand allows the creation of a new database from a yaml specification.

Usage of %s:

`, databaseCmdFlags.Name(), databaseCmdFlags.Name())
		databaseCmdFlags.PrintDefaults()
	}

	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), `
Usage of %s:

	Specify one of the following subcommands, which are shorthand for database, placement, and namespace:

	%s

Each subcommand has its own built-in help provided via "-h".

`, os.Args[0], strings.Join([]string{
			databaseCmdFlags.Name(),
			databaseCmdFlags.Name(),
			databaseCmdFlags.Name(),
		}, ", "))

		flag.PrintDefaults()
	}

	return databaseCmdFlags
}
