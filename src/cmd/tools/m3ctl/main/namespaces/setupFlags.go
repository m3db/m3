package namespaces

import (
	"flag"
	"fmt"
)

func SetupFlags(flags *NamespaceArgs) *flag.FlagSet {
	namespaceFlags := flag.NewFlagSet("ns", flag.ExitOnError)
	flags.delete = namespaceFlags.String("delete", "", "name of namespace to delete")
	flags.showAll = namespaceFlags.Bool("all", false, "show all the standard info for namespaces (otherwise default behaviour lists only the names)")
	namespaceFlags.Usage = func() {
		fmt.Fprintf(namespaceFlags.Output(), `
This is the subcommand for acting on placements.

Description:

The subcommand "%s"" provides the ability to:

* list all namespaces
* verbosely list all the available information about the namespaces
* delete a specific namespace

Default behaviour (no arguments) is to print out the names of the namespaces.

Specify only one action at a time.

Usage of %s:

`, namespaceFlags.Name(), namespaceFlags.Name())
		namespaceFlags.PrintDefaults()
	}
	return namespaceFlags
}
