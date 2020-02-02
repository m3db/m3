package delete

import (
	"flag"
	"fmt"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/checkArgs"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/delete/namespaces"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/delete/placements"
	"github.com/m3db/m3/src/cmd/tools/m3ctl/main/errors"
	"os"
)

// this has all that the upper level needs to dispatch to here
type Context struct {
	//vals     *placementArgs
	//handlers placementHandler
	GlobalOpts checkArgs.GlobalOpts
	Delete *flag.FlagSet
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

	deleteFlags := flag.NewFlagSet("delete", flag.ContinueOnError)
	deleteFlags.Usage = func() {
		fmt.Fprintf(os.Stderr, "delete help msg here\n")
		deleteFlags.PrintDefaults()
	}

	return Context{Delete:deleteFlags}
}

func (ctx Context) PopParseDispatch(cli []string) error {

	thisFlagset := ctx.Delete
	if len(cli) < 1 {
		thisFlagset.Usage()
		return &errors.FlagsError{}
	}

	// pop and parse
	inArgs := cli[1:]
	//if err := thisFlagset.Parse(inArgs); err != nil {
	//	thisFlagset.Usage()
	//	return err
	//}
	thisFlagset.Parse(inArgs)
	if thisFlagset.NArg() == 0 {
		thisFlagset.Usage()
		return &errors.FlagsError{}
	}

	plctx := placements.InitializeFlags()
	nsctx := namespaces.InitializeFlags()

	nextArgs := thisFlagset.Args()
	fmt.Print(nextArgs)
	switch nextArgs[0] {
	case plctx.Placement.Name():
		fmt.Print("delete pl case")
		plctx.Globals = ctx.GlobalOpts
		if err := plctx.PopParseDispatch(nextArgs); err != nil {
			return err
		}
	case nsctx.Namespaces.Name():
		fmt.Print("delete ns case")
		nsctx.Globals = ctx.GlobalOpts
		if err := nsctx.PopParseDispatch(nextArgs); err != nil {
			return err
		}
	default:
		fmt.Print("delete default case")
		thisFlagset.Usage()
		return &errors.FlagsError{}
	}

	fmt.Print("done delete with case")
	return nil

}