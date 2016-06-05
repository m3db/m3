package main

import (
	"flag"
	"fmt"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"code.uber.internal/infra/memtsdb/encoding"
	"code.uber.internal/infra/memtsdb/encoding/tsz"
	"code.uber.internal/infra/memtsdb/services/mdbnode/serve/httpjson"
	"code.uber.internal/infra/memtsdb/services/mdbnode/serve/tchannelthrift"
	"code.uber.internal/infra/memtsdb/sharding"
	"code.uber.internal/infra/memtsdb/storage"

	log "github.com/Sirupsen/logrus"
	"github.com/spaolacci/murmur3"
)

var (
	tchannelAddrArg = flag.String("tchanneladdr", "0.0.0.0:9000", "TChannel server address")
	httpAddrArg     = flag.String("httpaddr", "0.0.0.0:9001", "HTTP server address")
)

func main() {
	flag.Parse()

	if *tchannelAddrArg == "" || *httpAddrArg == "" {
		flag.Usage()
		os.Exit(1)
	}

	tchannelAddr := *tchannelAddrArg
	httpAddr := *httpAddrArg

	shards := uint32(1024)
	shardingScheme, err := sharding.NewShardScheme(0, shards-1, func(id string) uint32 {
		return murmur3.Sum32([]byte(id)) % shards
	})
	if err != nil {
		log.Fatalf("could not create sharding scheme: %v", err)
	}

	options := tsz.NewOptions()
	newEncoderFn := func(start time.Time, bytes []byte) encoding.Encoder {
		return tsz.NewEncoder(start, bytes, options)
	}
	newDecoderFn := func() encoding.Decoder {
		return tsz.NewDecoder(options)
	}
	opts := storage.NewDatabaseOptions().
		BlockSize(2 * time.Hour).
		BufferResolution(1 * time.Second).
		NewEncoderFn(newEncoderFn).
		NewDecoderFn(newDecoderFn).
		NowFn(time.Now).
		BufferFuture(1 * time.Minute).
		BufferPast(10 * time.Minute).
		BufferFlush(1 * time.Minute)

	db := storage.NewDatabase(shardingScheme.All(), opts)
	if err := db.Open(); err != nil {
		log.Fatalf("could not open database: %v", err)
	}
	defer db.Close()

	tchannelthriftClose, err := tchannelthrift.NewServer(db, tchannelAddr, nil).ListenAndServe()
	if err != nil {
		log.Fatalf("could not open tchannelthrift interface: %v", err)
	}
	defer tchannelthriftClose()
	log.Infof("tchannelthrift: listening on %v", tchannelAddr)

	httpjsonClose, err := httpjson.NewServer(db, httpAddr, nil).ListenAndServe()
	if err != nil {
		log.Fatalf("could not open httpjson interface: %v", err)
	}
	defer httpjsonClose()
	log.Infof("httpjson: listening on %v", httpAddr)

	log.Fatalf("interrupt: %v", interrupt())
}

func interrupt() error {
	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	return fmt.Errorf("%s", <-c)
}
