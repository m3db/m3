package namespaces

import (
	"flag"
	"fmt"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/checkArgs"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/errors"
	"os"
)

// all the values from the cli args are stored in here
// for all the placement-related commands
type namespacesArgs struct {
	nodeName *string
}

// this has all that the upper dispatcher needs to parse the cli
type Context struct {
	vals       *namespacesArgs
	handlers   namespacesHandlers
	Globals    checkArgs.GlobalOpts
	Namespaces *flag.FlagSet
}
type namespacesHandlers struct {
	xdel func(*namespacesArgs, checkArgs.GlobalOpts)
}

func InitializeFlags() Context {
	return _setupFlags(
		&namespacesArgs{},
		namespacesHandlers{
			xdel: doDelete,
		},
	)
}
func _setupFlags(finalArgs *namespacesArgs, handler namespacesHandlers) Context {

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
		vals:       finalArgs,
		handlers:   handler,
		Namespaces: nsFlags,
	}
}
func (ctx Context) PopParseDispatch(cli []string) error {
	if len(cli) < 1 {
		fmt.Printf("1:%v:\n", cli)
		ctx.Namespaces.Usage()
		return &errors.FlagsError{}
	}

	inArgs := cli[1:]
	if err := ctx.Namespaces.Parse(inArgs); err != nil {
		fmt.Printf("2:%v:\n", cli)

		ctx.Namespaces.Usage()
		return err
	}
	if ctx.Namespaces.NFlag() == 0 {
		fmt.Printf("3:%v:%v:\n", cli, ctx.Namespaces.NArg())

		ctx.Namespaces.Usage()
		return &errors.FlagsError{}
	}

	if err := dispatcher(ctx); err != nil {
		fmt.Printf("4:%v:\n", cli)

		return err
	}

	return nil

}

func dispatcher(ctx Context) error {
	nextArgs := ctx.Namespaces.Args()

	fmt.Printf("nextArgs:%v:%v:\n", nextArgs, len(nextArgs))

	if len(nextArgs) != 0 {
		ctx.Namespaces.Usage()
		return &errors.FlagsError{"\nextra args supplied. See usage.\n"}
	}

	ctx.handlers.xdel(ctx.vals, ctx.Globals)
	return nil

}
