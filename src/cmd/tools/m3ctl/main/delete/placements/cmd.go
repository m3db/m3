package placements

import (
	"flag"
	"fmt"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/checkArgs"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/errors"
	"os"
)

// all the values from the cli args are stored in here
// for all the placement-related commands
type placementArgs struct {
	deleteEntire *bool
	nodeName     *string
}

// this has all that the upper dispatcher needs to parse the cli
type Context struct {
	vals      *placementArgs
	handlers  placementHandlers
	Globals   checkArgs.GlobalOpts
	Placement *flag.FlagSet
}
type placementHandlers struct {
	xdel func(*placementArgs, checkArgs.GlobalOpts)
}

func InitializeFlags() Context {
	return _setupFlags(
		&placementArgs{},
		placementHandlers{
			xdel: doDelete,
		},
	)
}
func _setupFlags(finalArgs *placementArgs, handler placementHandlers) Context {

	placementFlags := flag.NewFlagSet("pl", flag.ContinueOnError)

	finalArgs.deleteEntire = placementFlags.Bool("all", false, "delete the entire placement")
	finalArgs.nodeName = placementFlags.String("node", "", "delete the specified node in the placement")
	placementFlags.Usage = func() {
		fmt.Fprintf(os.Stderr, `
"%s" is for acting on placements.

Description:

qqq

`, placementFlags.Name())
		placementFlags.PrintDefaults()
	}

	return Context{
		vals:      finalArgs,
		handlers:  handler,
		Placement: placementFlags,
	}
}
func (ctx Context) PopParseDispatch(cli []string) error {
	if len(cli) < 1 {
		ctx.Placement.Usage()
		return &errors.FlagsError{}
	}

	inArgs := cli[1:]
	if err := ctx.Placement.Parse(inArgs); err != nil {
		ctx.Placement.Usage()
		return &errors.FlagsError{}
	}
	if ctx.Placement.NFlag() == 0 {
		fmt.Printf("3:%v:%v:\n", cli, ctx.Placement.NArg())

		ctx.Placement.Usage()
		return &errors.FlagsError{}
	}

	if err := dispatcher(ctx); err != nil {
		return err
	}

	return nil

}

func dispatcher(ctx Context) error {
	nextArgs := ctx.Placement.Args()

	if len(nextArgs) != 0 {
		ctx.Placement.Usage()
		return &errors.FlagsError{"\nextra args supplied. See usage\n"}
	}
	ctx.handlers.xdel(ctx.vals, ctx.Globals)
	return nil

}
