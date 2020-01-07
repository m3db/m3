package placements

import (
	"flag"
	"fmt"
	"go.uber.org/zap"
	"io/ioutil"
	"net/http"
	"os"

	//"github.com/gogo/protobuf/jsonpb"
	//"github.com/m3db/m3/src/query/generated/proto/admin"
)

var (
	GetFlags    *flag.FlagSet
	endpoint    *string
	defaultPath = "/api/v1/placement"
)

//curl -s -k -v 'http://localhost:7201/api/v1/placement'

//curl -v -X DELETE bmcqueen-ld1:7201/api/v1/services/m3db/placement

func init() {
	GetFlags = flag.NewFlagSet("placements", flag.ExitOnError)
	endpoint = GetFlags.String("endpoint", "http://bmcqueen-ld1:7201", "url for endpoint")
	//showAll = GetFlags.Bool("all", false, "show standard info for namespaces (default is to list only the names)")
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

	//registry := admin.PlacementGetResponse{}

	url := fmt.Sprintf("%s%s", *endpoint, defaultPath)

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
	//if *showAll {
		dat, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			panic(err)
		}

		fmt.Println(string(dat))

		return
	//}

	//unmarshaller := &jsonpb.Unmarshaler{AllowUnknownFields: true}
	//
	//err = unmarshaller.Unmarshal(resp.Body, &registry)
	//if err != nil {
	//	panic(err)
	//}
	//
	//log.Debug(registry)
	//log.Debugf("%#v\n", *showAll)
	//
	//for k, _ := range registry.Registry.Namespaces {
	//	fmt.Println(k)
	//}

	return
}
