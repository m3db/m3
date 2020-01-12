package database

import (
	"flag"
	"fmt"
	"github.com/m3db/m3/src/x/config/configflag"
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

type flagsError struct {
	Message string
}

func (e *flagsError) Error() string {
	if e == nil {
		return ""
	}
	return e.Message
}
func ParseAndDo(arg *configflag.FlagStringSlice, flags *DatabaseFlagSets, ep string) {
	if err := _parseAndDo(flag.Args(), arg, flags, ep, Create); err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		os.Exit(1)
	}
}
func _parseAndDo(args []string, finalArgs *configflag.FlagStringSlice, flags *DatabaseFlagSets, ep string, doer func(string, string)) error {
	if len(args) == 0 {
		// if it gets here i want to see how it got here
		panic("parser called with no args")
	}
	if err := flags.Database.Parse(args[1:]); err != nil {
		flags.Database.Usage()
		return err
	}
	if flags.Database.NArg() == 0 {
		flags.Database.Usage()
		return &flagsError{}
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
			return &flagsError{}
		}
		// the below finalArgs.Value has at least one value by this time per the finalArgs parser
		doer(finalArgs.Value[len(finalArgs.Value)-1], ep)
		return nil
	default:
		return &flagsError{}
	}
}
