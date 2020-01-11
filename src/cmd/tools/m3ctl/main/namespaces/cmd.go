package namespaces

import (
	"flag"
	"fmt"
	"go.uber.org/zap"
	"os"
)

const (
	defaultPath = "/api/v1/namespace"
	debugQS     = "debug=true"
)

type NamespaceArgs struct {
	showAll *bool
	delete  *string
}

type NamespaceFlags struct {
	Namespace *flag.FlagSet
	Delete    *flag.FlagSet
}
func SetupFlags(flags *NamespaceArgs) NamespaceFlags {
	namespaceFlags := flag.NewFlagSet("ns", flag.ExitOnError)
	deleteFlags := flag.NewFlagSet("delete", flag.ExitOnError)
	flags.delete = deleteFlags.String("name", "", "name of namespace to delete")

	flags.showAll = namespaceFlags.Bool("all", false, "show all the standard info for namespaces (otherwise default behaviour lists only the names)")
	namespaceFlags.Usage = func() {
		fmt.Fprintf(namespaceFlags.Output(), `
This is the subcommand for acting on namespaces.

Description:

The namespaces subcommand "%s"" provides the ability to:

* list all namespaces (default)
* verbosely list all the available information about the namespaces (-all)
* delete a specific namespace (see the delete subcommand)

Default behaviour (no arguments) is to print out the names of the namespaces.

Specify only one action at a time.

It has the following subcommands:

	%s

Usage:

`, namespaceFlags.Name(), deleteFlags.Name())
		namespaceFlags.PrintDefaults()
	}
	deleteFlags.Usage = func() {
		fmt.Fprintf(deleteFlags.Output(), `
This is the "%s" subcommand for %s scoped operations.

Description:

This subcommand allows the creation of a new database from a yaml specification.

Usage of %s:

`, deleteFlags.Name(), namespaceFlags.Name(), deleteFlags.Name())
		deleteFlags.PrintDefaults()
	}
	return NamespaceFlags{Namespace: namespaceFlags, Delete: deleteFlags}
}

func ParseAndDo(args *NamespaceArgs, flags *NamespaceFlags, ep string, log *zap.SugaredLogger) {
	if err := flags.Namespace.Parse(flag.Args()[1:]); err != nil {
		flags.Namespace.Usage()
		os.Exit(1)
	}
	// maybe do the default action which is listing the names
	if flags.Namespace.NArg() == 0 {
		Show(args, ep, log)
		os.Exit(0)
	}
	switch flag.Arg(1) {
	case flags.Delete.Name():
		if err := flags.Delete.Parse(flag.Args()[2:]); err != nil {
			flags.Delete.Usage()
			os.Exit(1)
		}
		if flags.Delete.NFlag() == 0 {
			flags.Delete.Usage()
			os.Exit(1)
		}
		flags.Delete.Visit(func(f *flag.Flag) {
			if len(f.Value.String()) == 0 {
				fmt.Fprintf(os.Stderr, "%s requires a value.\n", f.Name)
				flags.Delete.Usage()
				os.Exit(1)
			}
		})
		Delete(args, ep, log)
	default:
		flags.Namespace.Usage()
		os.Exit(1)
	}
}
