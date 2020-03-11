package apply

import (
	"flag"
	"fmt"
	"os"

	"github.com/m3db/m3/src/cmd/tools/m3ctl/errors"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/globalopts"
	"github.com/m3db/m3/src/x/config/configflag"
)

type applyVals struct {
	yamlFlag *configflag.FlagStringSlice
}
type Context struct {
	vals       *applyVals
	handlers   applyHandlers
	GlobalOpts globalopts.GlobalOpts
	Flags      *flag.FlagSet
}
type applyHandlers struct {
	handle func(*applyVals, globalopts.GlobalOpts) error
}

func InitializeFlags() Context {
	return _setupFlags(
		&applyVals{
			yamlFlag: &configflag.FlagStringSlice{},
		},
		applyHandlers{
			handle: doApply,
		})
}

func _setupFlags(finalArgs *applyVals, handlers applyHandlers) Context {
	applyFlags := flag.NewFlagSet("apply", flag.ContinueOnError)
	applyFlags.Var(finalArgs.yamlFlag, "f", "Path to yaml.")
	applyFlags.Usage = func() {
		fmt.Fprintf(os.Stderr, `
Usage: m3ctl %s -f somedir/somename.yaml

The "%s" subcommand takes its inputs from a yaml file and knows how to do the
following operations:

 * create - create a namespace and initialize a topology
 * init - initializes a placement
 * newNode - adds a node to a placement
 * replaceNode - replaces a node in a placement

Example yaml files are included in the yaml/examples directory. Here's an
example for database creation:

---
operation: create
type: cluster
namespace_name: default
retention_time: 168h
num_shards: 64
replication_factor: 1
hosts:
- id: m3db_seed
  isolation_group: rack-a
  zone: embedded
  weight: 1024
  endpoint: m3db_seed:9000
  hostname: m3db_seed
  port: 9000

'


`, applyFlags.Name(), applyFlags.Name())
		applyFlags.PrintDefaults()
	}
	return Context{
		vals:     finalArgs,
		Flags:    applyFlags,
		handlers: handlers,
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
	if ctx.Flags.NFlag() != 1 {
		ctx.Flags.Usage()
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
	return ctx.handlers.handle(ctx.vals, ctx.GlobalOpts)
}
