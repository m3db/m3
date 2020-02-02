package namespaces

import (
	"flag"
	"fmt"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/checkArgs"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/errors"
	"os"
)

type namespacesArgs struct {
	showAll *bool
}

type context struct {
	vals      *namespacesArgs
	handlers  namespacesHandlers
	Globals   checkArgs.GlobalOpts
	Namespaces *flag.FlagSet
	//Add       *flag.FlagSet
	//Delete    *flag.FlagSet
	//Init      *flag.FlagSet
	//Replace   *flag.FlagSet
}
type namespacesHandlers struct {
	xget func(*namespacesArgs, checkArgs.GlobalOpts)
}

// everything needed to prep for this pl command level
// nothing that's needed below it
// just the stuff for parsing at the pl level
func InitializeFlags() context {
	return _setupFlags(
		&namespacesArgs{},
		namespacesHandlers{
			//add:     doAdd,
			//delete:  doDelete,
			//xget:    func(c *placementArgs, s string) {fmt.Print("deeper fake pl get handler")},
			xget: get,
			//xget:    placements.,
			//xinit:   doInit,
			//replace: doReplace,
		},
	)
}
func _setupFlags(finalArgs *namespacesArgs, handler namespacesHandlers) context {
	namespaceFlags := flag.NewFlagSet("ns", flag.ContinueOnError)
	finalArgs.showAll = namespaceFlags.Bool("all", false, "get all the standard info for namespaces (otherwise default behaviour lists only the names)")

	namespaceFlags.Usage = func() {
		fmt.Fprintf(os.Stderr, `
"%s" is for acting on placements.

Description:

Default behaviour (no arguments) is to provide a json dump of the existing placement.


`, namespaceFlags.Name())
		namespaceFlags.PrintDefaults()
	}
	return context{
		vals:     finalArgs,
		handlers: handler,
		//GlobalOpts:   nil,
		Namespaces: namespaceFlags,
		//Add:       addFlags,
		//Delete:    deleteFlags,
		//Init:      initFlags,
		//Replace:   replaceFlags,
	}
}

func (ctx context) PopParseDispatch(cli []string) error {
	// right here args should be like "pl delete -node someName"
	if len(cli) < 1 {
		ctx.Namespaces.Usage()
		return &errors.FlagsError{}
	}

	inArgs := cli[1:]
	if err := ctx.Namespaces.Parse(inArgs); err != nil {
		ctx.Namespaces.Usage()
		return &errors.FlagsError{}
	}
	if ctx.Namespaces.NArg() == 0 {
		ctx.handlers.xget(ctx.vals, ctx.Globals)
		return nil
	}

	if err := dispatcher(ctx); err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		return err
	}

	return nil

}

func dispatcher(ctx context) error {

	nextArgs := ctx.Namespaces.Args()
	switch nextArgs[0] {
	case "":
		ctx.handlers.xget(ctx.vals, ctx.Globals)
		return nil
	default:
		ctx.Namespaces.Usage()
		return &errors.FlagsError{}
	}
}