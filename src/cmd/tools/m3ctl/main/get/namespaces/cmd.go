package namespaces

import (
	"flag"
	"fmt"
	"os"

	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/checkArgs"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/errors"
)

type namespacesArgs struct {
	showAll *bool
}

type Context struct {
	vals     *namespacesArgs
	handlers namespacesHandlers
	Globals  checkArgs.GlobalOpts
	Flags    *flag.FlagSet
}
type namespacesHandlers struct {
	handle func(*namespacesArgs, checkArgs.GlobalOpts)
}

func InitializeFlags() Context {
	return _setupFlags(
		&namespacesArgs{},
		namespacesHandlers{
			handle: doGet,
		},
	)
}
func _setupFlags(finalArgs *namespacesArgs, handler namespacesHandlers) Context {
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
	return Context{
		vals:     finalArgs,
		handlers: handler,
		Flags:    namespaceFlags,
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
		return &errors.FlagsError{}
	}
	if ctx.Flags.NArg() == 0 {
		ctx.handlers.handle(ctx.vals, ctx.Globals)
		return nil
	}
	if err := dispatcher(ctx); err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		return err
	}
	return nil
}
func dispatcher(ctx Context) error {
	nextArgs := ctx.Flags.Args()
	switch nextArgs[0] {
	case "":
		ctx.handlers.handle(ctx.vals, ctx.Globals)
		return nil
	default:
		ctx.Flags.Usage()
		return &errors.FlagsError{}
	}
}
