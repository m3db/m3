package database

import (
	"flag"
	"fmt"
	"github.com/m3db/m3/src/x/config/configflag"
)

type DatabaseFlagSets struct {
	Database *flag.FlagSet
	Create * flag.FlagSet
}
func SetupFlags(createDatabaseYAML *configflag.FlagStringSlice) DatabaseFlagSets {
	databaseFlags := flag.NewFlagSet("db", flag.ExitOnError)
	createFlags := flag.NewFlagSet("create", flag.ExitOnError)

	databaseFlags.Usage = func() {
		fmt.Fprintf(databaseFlags.Output(), `
This is the "%s" subcommand for database scoped operations.

It has the following subcommands:

	%s

`, databaseFlags.Name(), createFlags.Name())
		databaseFlags.PrintDefaults()
	}

	createFlags.Var(createDatabaseYAML, "f", "Path to yaml for simplified db creation with sane defaults.")
	createFlags.Usage = func() {
		fmt.Fprintf(databaseFlags.Output(), `
This is the "%s" subcommand for %s scoped operations.

Description:

This subcommand allows the creation of a new database from a yaml specification.

Usage of %s:

`, createFlags.Name(), databaseFlags.Name(), createFlags.Name())
		createFlags.PrintDefaults()
	}

	return DatabaseFlagSets{Database:databaseFlags, Create:createFlags}
}
