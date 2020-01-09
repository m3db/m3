package database

import (
	"flag"
	"fmt"
	"github.com/m3db/m3/src/cmd/tools/m3db_tool/main/http"
	"github.com/m3db/m3/src/cmd/tools/m3db_tool/main/yaml"
	"github.com/m3db/m3/src/query/generated/proto/admin"
	"github.com/m3db/m3/src/x/config/configflag"
	"go.uber.org/zap"
	"os"
	"strings"
)

const (
	defaultPath = "/api/v1/database"
)

func Command(createYAML string, endpoint string, log *zap.SugaredLogger) {
	log.Debugf("createYAML:%s:\n", createYAML)
	data := yaml.Load(createYAML, &admin.DatabaseCreateRequest{}, log)
	url := fmt.Sprintf("%s%s/create", endpoint, defaultPath)
	http.DoPost(url, data, log, http.DoDump)
	return
}

func SetupDatabaseFlags(createDatabaseYAML *configflag.FlagStringSlice) *flag.FlagSet {
	databaseFlags := flag.NewFlagSet("db", flag.ExitOnError)
	databaseFlags.Var(createDatabaseYAML, "create", "Path to yaml for simplified db creation with sane defaults.")
	databaseFlags.Usage = func() {
		fmt.Fprintf(databaseFlags.Output(), `
This is the "%s" subcommand for database scoped operations.

Description:

This subcommand allows the creation of a new database from a yaml specification.

Usage of %s:

`, databaseFlags.Name(), databaseFlags.Name())
		databaseFlags.PrintDefaults()
	}
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), `
Usage of %s:

	Specify one of the following subcommands, which are shorthand for database, placement, and namespace:

	%s

Each subcommand has its own built-in help provided via "-h".

`, os.Args[0], strings.Join([]string{
			databaseFlags.Name(),
			databaseFlags.Name(),
			databaseFlags.Name(),
		}, ", "))

		flag.PrintDefaults()
	}
	return databaseFlags
}
