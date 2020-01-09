package namespaces

import (
	"flag"
	"fmt"
	"go.uber.org/zap"
	"io"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/m3db/m3/src/cmd/tools/m3db_tool/main/common"
	"github.com/m3db/m3/src/query/generated/proto/admin"
)

const (
	defaultPath = "/api/v1/namespace"
	debugQS     = "debug=true"
)

type NamespaceArgs struct {
	showAll *bool
	delete  *string
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

func Cmd(flags *NamespaceArgs, endpoint string, log *zap.SugaredLogger) {

	url := fmt.Sprintf("%s%s?%s", endpoint, defaultPath, debugQS)

	if *flags.showAll {

		common.DoGet(url, log, common.DoDump)

	} else if len(*flags.delete) > 0 {

		url = fmt.Sprintf("%s%s/%s", endpoint, "/api/v1/services/m3db/namespace", *flags.delete)

		common.DoDelete(url, log, common.DoDump)

	} else {

		common.DoGet(url, log, doShowNames)

	}

	return
}

func SetupNamespaceFlags(flags *NamespaceArgs) *flag.FlagSet {

	namespaceCmdFlags := flag.NewFlagSet("ns", flag.ExitOnError)
	flags.delete = namespaceCmdFlags.String("delete", "", "name of namespace to delete")
	flags.showAll = namespaceCmdFlags.Bool("all", false, "show all the standard info for namespaces (otherwise default behaviour lists only the names)")
	namespaceCmdFlags.Usage = func() {
		fmt.Fprintf(namespaceCmdFlags.Output(), `
This is the subcommand for acting on placements.

Description:

The subcommand "%s"" provides the ability to:

* list all namespaces
* verbosely list all the available information about the namespaces
* delete a specific namespace

Default behaviour (no arguments) is to print out the names of the namespaces.

Specify only one action at a time.

Usage of %s:

`, namespaceCmdFlags.Name(), namespaceCmdFlags.Name())
		namespaceCmdFlags.PrintDefaults()
	}

	return namespaceCmdFlags
}
