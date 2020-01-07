package database

import (
	"bytes"
	"net/http"

	//"bytes"
	//"encoding/json"
	"flag"
	"fmt"
	"go.uber.org/zap"
	"io/ioutil"

	//"io/ioutil"
	//"net/http"

	//"k8s.io/gengo/testdata/a/b"

	"github.com/ghodss/yaml"

	//"io/ioutil"
	//"net/http"
	"os"

	"github.com/gogo/protobuf/jsonpb"
	//"gopkg.in/yaml.v2"
	"github.com/m3db/m3/src/query/generated/proto/admin"
	"github.com/m3db/m3/src/cmd/tools/q/main/flagvar"
)

var (
	DatabaseFlags *flag.FlagSet
	endpoint      *string
	createYaml     = flagvar.File{}

	defaultPath    = "/api/v1/database"
	debugQS        = "debug=true"
)

//curl -X POST http://localhost:7201/api/v1/database/create -d

func init() {
	DatabaseFlags = flag.NewFlagSet("database", flag.ExitOnError)
	endpoint = DatabaseFlags.String("endpoint", "http://bmcqueen-ld1:7201", "url for endpoint")
	DatabaseFlags.Var(&createYaml, "create", "Path to yaml for simplified db creation with sane defaults.")
	DatabaseFlags.Usage = func() {
		fmt.Fprintf(DatabaseFlags.Output(), `
Description:

blah blah


Usage of %s:

`, DatabaseFlags.Name())
		DatabaseFlags.PrintDefaults()
	}
}

func Cmd(log *zap.SugaredLogger) {

	if err := DatabaseFlags.Parse(flag.Args()[1:]); err != nil {
		DatabaseFlags.Usage()
		os.Exit(1)
	}

	//if DatabaseFlags.NFlag() > 4 {
	//	DatabaseFlags.Usage()
	//	os.Exit(1)
	//}

	if len(createYaml.Value) < 1 {
		DatabaseFlags.Usage()
		os.Exit(1)
	}

	registry := admin.DatabaseCreateRequest{}

	content, err := ioutil.ReadFile(createYaml.Value)
	//content, err := os.Open(createYaml.Value)
	if err != nil {
		log.Fatal(err)
	}

	//fmt.Printf("File contents: %s\n\n", content)

	if err = yaml.Unmarshal([]byte(content), &registry); err != nil {
		log.Fatalf("cannot unmarshal data: %v", err)
	}

	//unmarshaller := &jsonpb.Unmarshaler{AllowUnknownFields: true}

	//if err = unmarshaller.Unmarshal(content, &registry); err != nil {
	//	panic(err)
	//}

	//fmt.Printf("%+v:\n", registry)

	var data *bytes.Buffer
	data = bytes.NewBuffer(nil)

	marshaller := &jsonpb.Marshaler{}
	if err = marshaller.Marshal(data, &registry); err != nil {
		log.Fatal(err)
	}

	url := fmt.Sprintf("%s%s/create", *endpoint, defaultPath)

	log.Debugf("url:%s:\n", url)

	client := &http.Client{}

	req, err := http.NewRequest(http.MethodPost, url, data)
	req.Header.Add("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		panic(err)

	}
	defer func() {
		ioutil.ReadAll(resp.Body)
		resp.Body.Close()
	}()

	//// printout the json
	//// no need to unmarshal it
	//if *showAll {
		dat, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			panic(err)
		}

		fmt.Println(string(dat))

		return
	//}


	//old stuff
	//
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
