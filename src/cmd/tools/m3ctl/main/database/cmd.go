package database

import (
	"flag"
	"fmt"
	"os"

	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/errors"
	"github.com/m3db/m3/src/x/config/configflag"
)

const (
	defaultPath = "/api/v1/database"
)

type DatabaseFlagSets struct {
	Database *flag.FlagSet
	Create   *flag.FlagSet
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

	return DatabaseFlagSets{Database: databaseFlags, Create: createFlags}
}

func ParseAndDo(arg *configflag.FlagStringSlice, flags *DatabaseFlagSets, ep string) {
	args := flag.Args()
	// right here args should be like "db create -f somefile"
	if len(args) < 2 {
		flags.Database.Usage()
		os.Exit(1)
	}
	// pop and parse
	if err := parseAndDoCreate(args[1:], arg, flags, ep, Create); err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		os.Exit(1)
	}
}
func parseAndDoCreate(args []string, finalArgs *configflag.FlagStringSlice, flags *DatabaseFlagSets, ep string, doer func(string, string)) error {
	if err := flags.Database.Parse(args); err != nil {
		flags.Database.Usage()
		return err
	}
	if flags.Database.NArg() == 0 {
		flags.Database.Usage()
		return &errors.FlagsError{}
	}
	switch flags.Database.Arg(0) {
	case flags.Create.Name():
		// pop and parse
		createArgs := flags.Database.Args()[1:]
		if err := flags.Create.Parse(createArgs); err != nil {
			flags.Create.Usage()
			return err
		}
		if flags.Create.NFlag() == 0 {
			flags.Create.Usage()
			return &errors.FlagsError{}
		}
		// the below finalArgs.Value has at least one value by this time per the parser
		doer(finalArgs.Value[len(finalArgs.Value)-1], ep)
		return nil
	default:
		flags.Database.Usage()
		return &errors.FlagsError{}
	}
}
