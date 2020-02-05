package delete

import (
	"flag"
	"fmt"
	"os"

	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/delete/namespaces"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/delete/placements"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/errors"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/globalopts"
)

// this has all that the upper level needs to dispatch to here
type Context struct {
	GlobalOpts globalopts.GlobalOpts
	Flags      *flag.FlagSet
}

func InitializeFlags() Context {
	return _setupFlags()
}
func _setupFlags() Context {
	deleteFlags := flag.NewFlagSet("delete", flag.ContinueOnError)
	deleteFlags.Usage = func() {
		fmt.Fprintf(os.Stderr, `
"%s" is for deletiion operations for namespaces and placements.

`)
		deleteFlags.PrintDefaults()
	}
	return Context{Flags: deleteFlags}
}
func (ctx Context) PopParseDispatch(cli []string) error {
	thisFlagset := ctx.Flags
	if len(cli) < 1 {
		thisFlagset.Usage()
		return &errors.FlagsError{}
	}
	// pop and parse
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
