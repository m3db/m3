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

//const (
//	defaultPath = "/api/v1/services/m3db/placement"
//)

// all the values from the cli args are stored in here
// for all the placement-related commands
type placementArgs struct {
	//deletePlacement *bool
	//deleteNode      *string
	//initFlag        configflag.FlagStringSlice
	//newNodeFlag     configflag.FlagStringSlice
	//replaceFlag     configflag.FlagStringSlice
}

// this has all that the upper dispatcher needs to parse the cli
type Context struct {
	vals      *placementArgs
	handlers  placementHandlers
	Globals   checkArgs.GlobalOpts
	Placement *flag.FlagSet
	//Add       *flag.FlagSet
	//Delete    *flag.FlagSet
	//Init      *flag.FlagSet
	//Replace   *flag.FlagSet
}
type placementHandlers struct {
	xget func(*placementArgs, checkArgs.GlobalOpts)
}

// everything needed to prep for this pl command level
// nothing that's needed below it
// just the stuff for parsing at the pl level
func InitializeFlags() Context {
	return _setupFlags(
		&placementArgs{},
		placementHandlers{
			//add:     doAdd,
			//delete:  doDelete,
			//xget:    func(c *placementArgs, s string) {fmt.Print("deeper fake pl get handler")},
			xget: doGet,
			//xget:    placements.,
			//xinit:   doInit,
			//replace: doReplace,
		},
	)
}
func _setupFlags(finalArgs *placementArgs, handler placementHandlers) Context {

	placementFlags := flag.NewFlagSet("pl", flag.ContinueOnError)
	//deleteFlags := flag.NewFlagSet("delete", flag.ExitOnError)
	//addFlags := flag.NewFlagSet("add", flag.ExitOnError)
	//initFlags := flag.NewFlagSet("init", flag.ExitOnError)
	//replaceFlags := flag.NewFlagSet("replace", flag.ExitOnError)

	//finalArgs.deletePlacement = deleteFlags.Bool("all", false, "delete the entire placement")
	//finalArgs.deleteNode = deleteFlags.String("node", "", "delete the specified node in the placement")
	//initFlags.Var(&finalArgs.initFlag, "f", "initialize a placement. Specify a yaml file.")
	//addFlags.Var(&finalArgs.newNodeFlag, "f", "add a new node to the placement. Specify the filename of the yaml.")
	//replaceFlags.Var(&finalArgs.replaceFlag, "f", "add a new node to the placement. Specify the filename of the yaml.")
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
		//Add:       addFlags,
		//Delete:    deleteFlags,
		//Init:      initFlags,
		//Replace:   replaceFlags,
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
