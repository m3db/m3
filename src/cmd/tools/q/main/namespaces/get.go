package namespaces

import (
	"flag"
	"fmt"
	"go.uber.org/zap"
	"io/ioutil"
	"net/http"
	"os"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/m3db/m3/src/query/generated/proto/admin"
)

var (
	GetFlags    *flag.FlagSet
	endpoint    *string
	showAll     *bool
	defaultPath = "/api/v1/namespace"
	debugQS = "debug=true"
	delete *string
)

//curl -s -k -v 'http://localhost:7201/api/v1/namespace?debug=true' | jq .registry.namespaces\|keys

//curl -v -X DELETE bmcqueen-ld1:7201/api/v1/services/m3db/namespace/metrics_10s_48h

func init() {
	GetFlags = flag.NewFlagSet("namespaces", flag.ExitOnError)
	endpoint = GetFlags.String("endpoint", "http://bmcqueen-ld1:7201", "url for endpoint")
	endpoint = GetFlags.String("delete", "", "name of namespace to delete")
	showAll = GetFlags.Bool("all", false, "show all the standard info for namespaces (otherwise default behaviour lists only the names)")
	GetFlags.Usage = func() {
		fmt.Fprintf(GetFlags.Output(), `
Description:

blah blah


Usage of %s:

`, GetFlags.Name())
		GetFlags.PrintDefaults()
	}
}

func Get(log *zap.SugaredLogger) {

	if err := GetFlags.Parse(flag.Args()[1:]); err != nil {
		GetFlags.Usage()
		os.Exit(1)
	}

	//if GetFlags.NFlag() > 4 {
	//	GetFlags.Usage()
	//	os.Exit(1)
	//}

	registry := admin.NamespaceGetResponse{}

	url := fmt.Sprintf("%s%s?%s", *endpoint, defaultPath, debugQS)

	log.Debugf("url:%s:\n", url)

	resp, err := http.Get(url)
	if err != nil {
		panic(err)
	}

	defer func() {
		ioutil.ReadAll(resp.Body)
		resp.Body.Close()
	}()

	if err != nil {
		panic(err)
	}

	// printout the json
	// no need to unmarshal it
	if *showAll {
		dat, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			panic(err)
		}

		fmt.Println(string(dat))

		return
	}

	unmarshaller := &jsonpb.Unmarshaler{AllowUnknownFields: true}

	err = unmarshaller.Unmarshal(resp.Body, &registry)
	if err != nil {
		panic(err)
	}

	log.Debug(registry)
	log.Debugf("%#v\n", *showAll)

	for k, _ := range registry.Registry.Namespaces {
		fmt.Println(k)
	}

	return
}
