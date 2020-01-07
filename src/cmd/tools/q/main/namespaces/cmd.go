package namespaces

import (
	"flag"
	"fmt"
	"go.uber.org/zap"
	"io"
	"io/ioutil"
	"os"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/m3db/m3/src/cmd/tools/q/main/common"
	"github.com/m3db/m3/src/query/generated/proto/admin"
)

var (
	CmdFlags    *flag.FlagSet
	//endpoint    *string
	showAll     *bool
	defaultPath  = "/api/v1/namespace"
	debugQS      = "debug=true"
	delete      *string
)

func init() {
	CmdFlags = flag.NewFlagSet("ns", flag.ExitOnError)
	delete = CmdFlags.String("delete", "", "name of namespace to delete")
	showAll = CmdFlags.Bool("all", false, "show all the standard info for namespaces (otherwise default behaviour lists only the names)")
	CmdFlags.Usage = func() {
		fmt.Fprintf(CmdFlags.Output(), `
Description:

namespace tasks

Usage of %s:

`, CmdFlags.Name())
		CmdFlags.PrintDefaults()
	}
}


func doShowAll(in io.Reader, log *zap.SugaredLogger) {

	dat, err := ioutil.ReadAll(in)
	if err != nil {
		panic(err)
	}

	fmt.Println(string(dat))
}

//curl -s -k -v 'http://localhost:7201/api/v1/namespace?debug=true' | jq .registry.namespaces\|keys
func doShowNames(in io.Reader, log *zap.SugaredLogger) {

	registry := admin.NamespaceGetResponse{}

	unmarshaller := &jsonpb.Unmarshaler{AllowUnknownFields: true}

	if err := unmarshaller.Unmarshal(in, &registry); err != nil {
		panic(err)
	}

	log.Debug(registry)

	for k, _ := range registry.Registry.Namespaces {
		fmt.Println(k)
	}
}

//curl -v -X DELETE bmcqueen-ld1:7201/api/v1/services/m3db/namespace/metrics_10s_48h
func doDelete(reader io.Reader, logger *zap.SugaredLogger) {

	dat, err := ioutil.ReadAll(reader)
	if err != nil {
		panic(err)
	}

	fmt.Println(string(dat))

	return
}

func Cmd(log *zap.SugaredLogger) {

	if err := CmdFlags.Parse(flag.Args()[1:]); err != nil {
		CmdFlags.Usage()
		os.Exit(1)
	}

	//if CmdFlags.NFlag() > 4 {
	//	CmdFlags.Usage()
	//	os.Exit(1)
	//}

	url := fmt.Sprintf("%s%s?%s", *common.EndPoint, defaultPath, debugQS)

	if *showAll {

		common.DoGet(url, log, doShowAll)

	} else if len(*delete) > 0 {

		url = fmt.Sprintf("%s%s/%s", *common.EndPoint, "/api/v1/services/m3db/namespace", *delete)

		common.DoDelete(url, log, doDelete)

	} else {

		common.DoGet(url, log, doShowNames)

	}


	return
}
