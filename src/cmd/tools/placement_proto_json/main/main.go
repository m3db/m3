package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"

	"go.uber.org/zap"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
	"github.com/m3db/m3/src/cluster/generated/proto/placementpb"
)

var flags struct {
	filename string
}

func main() {
	flag.Parse()

	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}

	data, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		logger.Fatal("could not read stdin", zap.Error(err))
	}

	placement := &placementpb.Placement{}
	if err := proto.Unmarshal(data, placement); err != nil {
		logger.Fatal("could not unmarshal placement", zap.Error(err))
	}

	m := jsonpb.Marshaler{}
	str, err := m.MarshalToString(placement)
	if err != nil {
		logger.Fatal("could not marshal placement to json string", zap.Error(err))
	}

	fmt.Print(str)
}
