package namespaces

import (
	"flag"
	"fmt"
)

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

type NamespaceArgs struct {
	showAll *bool
	delete  *string
}
