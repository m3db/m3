package placements

import (
	"flag"
	"fmt"
	"os"

	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/errors"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/globalopts"
)

// all the values from the cli args are stored in here
// for all the placement-related commands
type placementVals struct {
	deleteEntire *bool
	nodeName     *string
}

// this has all that the upper dispatcher needs to parse the cli
type Context struct {
	vals     *placementVals
	handlers placementHandlers
	Globals  globalopts.GlobalOpts
	Flags    *flag.FlagSet
}
type placementHandlers struct {
	handle func(*placementVals, globalopts.GlobalOpts) error
}

func InitializeFlags() Context {
	return _setupFlags(
		&placementVals{},
		placementHandlers{
			handle: doDelete,
		},
	)
}
func _setupFlags(finalArgs *placementVals, handler placementHandlers) Context {
	placementFlags := flag.NewFlagSet("pl", flag.ContinueOnError)
	finalArgs.deleteEntire = placementFlags.Bool("all", false, "delete the entire placement")
	finalArgs.nodeName = placementFlags.String("node", "", "delete the specified node in the placement")
	placementFlags.Usage = func() {
		fmt.Fprintf(os.Stderr, `
The delete "%s" subcommand will delete an entire placement, or the specified node from the placement.

`, placementFlags.Name())
		placementFlags.PrintDefaults()
	}
	return Context{
		vals:     finalArgs,
		handlers: handler,
		Flags:    placementFlags,
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
		return &errors.FlagsError{"\nextra args supplied. See usage\n"}
	}
	return ctx.handlers.handle(ctx.vals, ctx.Globals)
}
