package placements

import (
	"flag"
	"fmt"
	"os"

	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/errors"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/globalopts"
)

type placementVals struct {
}
type Context struct {
	vals     *placementVals
	handlers placementHandlers
	Globals  globalopts.GlobalOpts
	Flags    *flag.FlagSet
}
type placementHandlers struct {
	handle func(*placementVals, globalopts.GlobalOpts)
}

func InitializeFlags() Context {
	return _setupFlags(
		&placementVals{},
		placementHandlers{
			handle: doGet,
		},
	)
}

func _setupFlags(finalArgs *placementVals, handler placementHandlers) Context {
	placementFlags := flag.NewFlagSet("pl", flag.ContinueOnError)
	placementFlags.Usage = func() {
		fmt.Fprintf(os.Stderr, `
"%s" is for displaying the existing placement.

`, placementFlags.Name())
		placementFlags.PrintDefaults()
	}
	return Context{
		vals:     finalArgs,
		handlers: handler,
		Flags: placementFlags,
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
		return &errors.FlagsError{}
	}
}
