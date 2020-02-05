package get

import (
	"flag"
	"fmt"
	"os"

	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/errors"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/get/namespaces"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/get/placements"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/globalopts"
)

type Context struct {
	GlobalOpts globalopts.GlobalOpts
	Flags      *flag.FlagSet
}

func InitializeFlags() Context {
	return _setupFlags()
}

func _setupFlags() Context {
	getFlags := flag.NewFlagSet("get", flag.ContinueOnError)
	getFlags.Usage = func() {
		fmt.Fprintf(os.Stderr, `
"%s" is for displaying information about the namespaces and placements.

`)
		getFlags.PrintDefaults()
	}
	return Context{Flags: getFlags}
}

func (ctx Context) PopParseDispatch(cli []string) error {
	thisFlagset := ctx.Flags
	if len(cli) < 1 {
		thisFlagset.Usage()
		return &errors.FlagsError{}
	}
	inArgs := cli[1:]
	if err := thisFlagset.Parse(inArgs); err != nil {
		thisFlagset.Usage()
		return err
	}
	if thisFlagset.NArg() == 0 {
		thisFlagset.Usage()
		return &errors.FlagsError{}
	}

	plctx := placements.InitializeFlags()
	nsctx := namespaces.InitializeFlags()
	nextArgs := thisFlagset.Args()
	switch nextArgs[0] {
	case plctx.Flags.Name():
		plctx.Globals = ctx.GlobalOpts
		if err := plctx.PopParseDispatch(nextArgs); err != nil {
			return err
		}
	case nsctx.Flags.Name():
		nsctx.Globals = ctx.GlobalOpts
		if err := nsctx.PopParseDispatch(nextArgs); err != nil {
			return err
		}
	default:
		thisFlagset.Usage()
		return &errors.FlagsError{}
	}
	return nil
}
