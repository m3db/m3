package namespaces

import (
	"flag"
	"fmt"
	"go.uber.org/zap"
	"io"
	"os"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/m3db/m3/src/cmd/tools/m3db_tool/main/common"
	"github.com/m3db/m3/src/query/generated/proto/admin"
)

var (
	CmdFlags    *flag.FlagSet
	showAll     *bool
	defaultPath = "/api/v1/namespace"
	debugQS     = "debug=true"
	delete      *string
)

func init() {
	CmdFlags = flag.NewFlagSet("ns", flag.ExitOnError)
	delete = CmdFlags.String("delete", "", "name of namespace to delete")
	showAll = CmdFlags.Bool("all", false, "show all the standard info for namespaces (otherwise default behaviour lists only the names)")
	CmdFlags.Usage = func() {
		fmt.Fprintf(CmdFlags.Output(), `
This is the subcommand for acting on placements.

Description:

The subcommand "%s"" provides the ability to:

* list all namespaces
* verbosely list all the available information about the namespaces
* delete a specific namespace

Default behaviour (no arguments) is to print out the names of the namespaces.

Specify only one action at a time.

Usage of %s:

`, CmdFlags.Name(), CmdFlags.Name())
		CmdFlags.PrintDefaults()
	}
}

func doShowNames(in io.Reader, log *zap.SugaredLogger) {

	registry := admin.NamespaceGetResponse{}

	unmarshaller := &jsonpb.Unmarshaler{AllowUnknownFields: true}

	if err := unmarshaller.Unmarshal(in, &registry); err != nil {
		log.Fatal(err)
	}

	for k, _ := range registry.Registry.Namespaces {
		fmt.Println(k)
	}
}

func Cmd(log *zap.SugaredLogger) {

	if err := CmdFlags.Parse(flag.Args()[1:]); err != nil {
		CmdFlags.Usage()
		os.Exit(1)
	}

	if CmdFlags.NFlag() > 1 {
		fmt.Fprintf(os.Stderr, "Please specify only one action.  There were too many cli arguments provided.\n")
		CmdFlags.Usage()
		os.Exit(1)
	}

	url := fmt.Sprintf("%s%s?%s", *common.EndPoint, defaultPath, debugQS)

	if *showAll {

		common.DoGet(url, log, common.DoDump)

	} else if len(*delete) > 0 {

		url = fmt.Sprintf("%s%s/%s", *common.EndPoint, "/api/v1/services/m3db/namespace", *delete)

		common.DoDelete(url, log, common.DoDump)

	} else {

		common.DoGet(url, log, doShowNames)

	}

	return
}
