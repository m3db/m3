package apply

import (
	"flag"
	"fmt"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/checkArgs"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/errors"

	"github.com/m3db/m3/src/x/config/configflag"
	"os"
)

type applyArgs struct {
	yamlFlag *configflag.FlagStringSlice
}

type Context struct {
	vals       *applyArgs
	handlers   applyHandlers
	GlobalOpts checkArgs.GlobalOpts
	Apply      *flag.FlagSet
}
type applyHandlers struct {
	xget func(*applyArgs, checkArgs.GlobalOpts)
}

func InitializeFlags() Context {
	return _setupFlags(
		&applyArgs{
			yamlFlag: &configflag.FlagStringSlice{},
		},
		applyHandlers{
			xget: Apply,
		})
}
func _setupFlags(finalArgs *applyArgs, handlers applyHandlers) Context {

	applyFlags := flag.NewFlagSet("apply", flag.ContinueOnError)
	applyFlags.Var(finalArgs.yamlFlag, "f", "Path to yaml.")

	applyFlags.Usage = func() {
		fmt.Fprintf(os.Stderr, "help msg here for apply\n")
		applyFlags.PrintDefaults()
	}

	return Context{
		vals:     finalArgs,
		Apply:    applyFlags,
		handlers: handlers,
	}
}

func (ctx Context) PopParseDispatch(cli []string) error {
	if len(cli) < 1 {
		ctx.Apply.Usage()
		return &errors.FlagsError{}
	}

	inArgs := cli[1:]
	if err := ctx.Apply.Parse(inArgs); err != nil {
		ctx.Apply.Usage()
		return err
	}
	if ctx.Apply.NFlag() != 1 {
		ctx.Apply.Usage()
		return &errors.FlagsError{}
	}

	if err := dispatcher(ctx); err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		return err
	}

	return nil

}

// no dispatching here
// there are no subcommands
func dispatcher(ctx Context) error {
	ctx.handlers.xget(ctx.vals, ctx.GlobalOpts)
	return nil
}
