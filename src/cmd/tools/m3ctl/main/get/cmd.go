package get

import (
	"flag"
	"fmt"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/errors"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/get/placements"

	//"github.com/m3db/m3/src/cmd/tools/m3ctl/main/placements"
	"os"

	//"github.com/m3db/m3/src/x/config/configflag"
)

// all the values from the cli args are stored in here
// for all the placement-related commands
//type getArgs struct {
//	deletePlacement *bool
//	deleteNode      *string
//	initFlag        configflag.FlagStringSlice
//	newNodeFlag     configflag.FlagStringSlice
//	replaceFlag     configflag.FlagStringSlice
//}

type GlobalOpts struct {
	Get placements.Globals
}
// this has all that the upper level needs to dispatch to here
type Context struct {
	//vals     *placementArgs
	//handlers placementHandler
	GlobalOpts
	Get *flag.FlagSet
	//Add       *flag.FlagSet
	//Delete    *flag.FlagSet
	//Init      *flag.FlagSet
	//Replace   *flag.FlagSet
}
//type placementHandler struct {
//	add func(placementArgs, string)
//	delete func(placementArgs, string)
//	xget func(placementArgs, string)
//	xinit func(placementArgs, string)
//	replace func(placementArgs, string)
//}

// setup hooks and context for this level
// everything needed to prep for this get command leve
// nothing that's needed below it
// just the stuff for parsing at the get level
func InitializeFlags() Context {
	//return _setupFlags(
		//&placementArgs{},
		//placementHandler{
	//		add:     doAdd,
	//		delete:  doDelete,
	//		xget:    doGet,
	//		xinit:   doInit,
	//		replace: doReplace,
	//	},
	//)

	return _setupFlags()
}
func _setupFlags() Context {

	getFlags := flag.NewFlagSet("get", flag.ExitOnError)
	getFlags.Usage = func() {
		fmt.Fprintf(os.Stderr, "help msg here\n")
		getFlags.PrintDefaults()
	}

	return Context{Get:getFlags}
}

// parse this level
// get hooks for the next level
// dispatch
func (ctx Context) PopParseDispatch(cli []string) error {

	thisFlagset := ctx.Get
	// right here args should be like "get ns -all"
	if len(cli) < 1 {
		thisFlagset.Usage()
		return &errors.FlagsError{}
	}

	// pop and parse
	inArgs := cli[1:]
	if err := thisFlagset.Parse(inArgs); err != nil {
		thisFlagset.Usage()
		return &errors.FlagsError{}
	}
	if thisFlagset.NArg() == 0 {
		//ctx.handlers.xget(*ctx.vals, ep)
		thisFlagset.Usage()
		//fmt.Print("stub get default action or error whatever is appropriate\n")
		return &errors.FlagsError{}
	}

	// contexts for next level
	//plctx := placements.InitializeFlags()
	plctx := placements.InitializeFlags()

	nextArgs := thisFlagset.Args()
	fmt.Print(nextArgs)
	switch nextArgs[0] {
	case plctx.Placement.Name():
		fmt.Print("pl case")
		plctx.Globals = ctx.GlobalOpts.Get
		if err := plctx.PopParseDispatch(nextArgs); err != nil {
			return err
		}
	//case plctx.Placement.Name():
	//	fmt.Print("pl case")
	//	if err := plctx.PopParseDispatch(nextArgs, "fake endpoint"); err != nil {
	//		return err
	//	}
	default:
		fmt.Print("default case")
		thisFlagset.Usage()
		return &errors.FlagsError{}
	}

	fmt.Print("done with case")
	return nil

}