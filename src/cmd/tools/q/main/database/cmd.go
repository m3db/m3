package database

import (
	"bytes"
	"io"
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
	"github.com/m3db/m3/src/cmd/tools/q/main/common"
	//"gopkg.in/yaml.v2"
	"github.com/m3db/m3/src/cmd/tools/q/main/flagvar"
	"github.com/m3db/m3/src/query/generated/proto/admin"
)

var (
	CmdFlags   *flag.FlagSet
	createYaml = flagvar.File{}

	defaultPath = "/api/v1/database"
	debugQS     = "debug=true"
)

//curl -X POST http://localhost:7201/api/v1/database/create -d

func init() {
	CmdFlags = flag.NewFlagSet("db", flag.ExitOnError)
	CmdFlags.Var(&createYaml, "create", "Path to yaml for simplified db creation with sane defaults.")
	CmdFlags.Usage = func() {
		fmt.Fprintf(CmdFlags.Output(), `
Description:

database tasks


Usage of %s:

`, CmdFlags.Name())
		CmdFlags.PrintDefaults()
	}
}

func doShow (reader io.Reader, logger *zap.SugaredLogger) {

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

	if len(createYaml.Value) < 1 {
		CmdFlags.Usage()
		os.Exit(1)
	}

	registry := admin.DatabaseCreateRequest{}

	content, err := ioutil.ReadFile(createYaml.Value)
	if err != nil {
		log.Fatal(err)
	}

	if err = yaml.Unmarshal(content, &registry); err != nil {
		log.Fatalf("cannot unmarshal data: %v", err)
	}

	var data *bytes.Buffer
	data = bytes.NewBuffer(nil)

	marshaller := &jsonpb.Marshaler{}
	if err = marshaller.Marshal(data, &registry); err != nil {
		log.Fatal(err)
	}

	url := fmt.Sprintf("%s%s/create", *common.EndPoint, defaultPath)

	common.DoPost(url, data, log, doShow)

	return
}
