package placements

import (
	"flag"
	"fmt"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/checkArgs"
	//"github.com/m3db/m3/src/cmd/tools/m3ctl/main/client"
	"os"

	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/errors"
	//"github.com/m3db/m3/src/x/config/configflag"
)

type placementArgs struct {
}

type Context struct {
	vals      *placementArgs
	handlers  placementHandlers
	Globals   checkArgs.GlobalOpts
	Placement *flag.FlagSet
}
type placementHandlers struct {
	xget func(*placementArgs, checkArgs.GlobalOpts)
}

func InitializeFlags() Context {
	return _setupFlags(
		&placementArgs{},
		placementHandlers{
			xget: doGet,
		},
	)
}
func _setupFlags(finalArgs *placementArgs, handler placementHandlers) Context {

	placementFlags := flag.NewFlagSet("pl", flag.ContinueOnError)
	placementFlags.Usage = func() {
		fmt.Fprintf(os.Stderr, `
"%s" is for acting on placements.

Description:

Default behaviour (no arguments) is to provide a json dump of the existing placement.


`, placementFlags.Name())
		placementFlags.PrintDefaults()
	}
	return Context{
		vals:     finalArgs,
		handlers: handler,
		//GlobalOpts:   nil,
		Placement: placementFlags,
	}
}

func (ctx Context) PopParseDispatch(cli []string) error {
	// right here args should be like "pl delete -node someName"
	if len(cli) < 1 {
		ctx.Placement.Usage()
		return &errors.FlagsError{}
	}

	inArgs := cli[1:]
	if err := ctx.Placement.Parse(inArgs); err != nil {
		ctx.Placement.Usage()
		return &errors.FlagsError{}
	}
	if ctx.Placement.NArg() == 0 {
		ctx.handlers.xget(ctx.vals, ctx.Globals)
		return nil
	}

	if err := dispatcher(ctx); err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		return err
	}

	return nil

}

func dispatcher(ctx Context) error {
	nextArgs := ctx.Placement.Args()
	switch nextArgs[0] {
	case "":
		ctx.handlers.xget(ctx.vals, ctx.Globals)
		return nil
	default:
		return &errors.FlagsError{}
	}

}
