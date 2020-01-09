package database

import (
	"flag"
	"fmt"
	"github.com/m3db/m3/src/x/config/configflag"
)

func SetupFlags(createDatabaseYAML *configflag.FlagStringSlice) *flag.FlagSet {
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
	return databaseFlags
}
