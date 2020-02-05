package namespaces

import (
	"flag"
	"fmt"
	"os"

	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/errors"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/globalopts"
)

// all the values from the cli args are stored in here
// for all the placement-related commands
type namespacesVals struct {
	nodeName *string
}
// this has all that the upper dispatcher needs to parse the cli
type Context struct {
	vals     *namespacesVals
	handlers namespacesHandlers
	Globals  globalopts.GlobalOpts
	Flags    *flag.FlagSet
}
type namespacesHandlers struct {
	handle func(*namespacesVals, globalopts.GlobalOpts)
}

func InitializeFlags() Context {
	return _setupFlags(
		&namespacesVals{},
		namespacesHandlers{
			handle: doDelete,
		},
	)
}

func _setupFlags(finalArgs *namespacesVals, handler namespacesHandlers) Context {
	nsFlags := flag.NewFlagSet("ns", flag.ContinueOnError)
	finalArgs.nodeName = nsFlags.String("node", "", "delete the specified node in the placement")
	nsFlags.Usage = func() {
		fmt.Fprintf(os.Stderr, `
"%s" is for acting on placements.

Description:

qqq

`, nsFlags.Name())
		nsFlags.PrintDefaults()
	}
	return Context{
		vals:     finalArgs,
		handlers: handler,
		Flags:    nsFlags,
	}
}

func (ctx Context) PopParseDispatch(cli []string) error {
	if len(cli) < 1 {
		ctx.Flags.Usage()
		return &errors.FlagsError{}
	}
	inArgs := cli[1:]
	if err := ctx.Flags.Parse(inArgs); err != nil {
		ctx.Flags.Usage()
		return err
	}
	if ctx.Flags.NFlag() == 0 {
		ctx.Flags.Usage()
		return &errors.FlagsError{}
	}
	if err := dispatcher(ctx); err != nil {
		return err
	}
	return nil
}

func dispatcher(ctx Context) error {
	nextArgs := ctx.Flags.Args()
	if len(nextArgs) != 0 {
		ctx.Flags.Usage()
		return &errors.FlagsError{"\nextra args supplied. See usage.\n"}
	}
	ctx.handlers.handle(ctx.vals, ctx.Globals)
	return nil

}
