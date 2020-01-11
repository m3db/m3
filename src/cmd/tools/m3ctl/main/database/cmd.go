package database

import (
	"flag"
	"fmt"
	"github.com/m3db/m3/src/x/config/configflag"
	"go.uber.org/zap"
	"os"
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
func ParseAndDo(arg *configflag.FlagStringSlice, flags *DatabaseFlagSets, ep string, log *zap.SugaredLogger) {
	if err := flags.Database.Parse(flag.Args()[1:]); err != nil {
		flags.Database.Usage()
		os.Exit(1)
	}
	if flags.Database.NArg() == 0 {
		flags.Database.Usage()
		os.Exit(1)
	}
	switch flag.Arg(1) {
	case flags.Create.Name():
		if err := flags.Create.Parse(flag.Args()[2:]); err != nil {
			flags.Create.Usage()
			os.Exit(1)
		}
		if flags.Create.NFlag() == 0 {
			flags.Create.Usage()
			os.Exit(1)
		}
		flags.Create.Visit(func(f *flag.Flag) {
			vals := f.Value.(*configflag.FlagStringSlice)
			for _, val := range vals.Value {
				if len(val) == 0 {
					fmt.Fprintf(os.Stderr, "%s requires a value.\n", f.Name)
					flags.Create.Usage()
					os.Exit(1)
				}
			}
		})
		// the below arg.Value has at least one value by this time per the arg parser
		Create(arg.Value[len(arg.Value)-1], ep, log)
	default:
		flags.Database.Usage()
		os.Exit(1)
	}
}