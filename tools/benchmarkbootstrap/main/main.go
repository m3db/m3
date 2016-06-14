package main

import (
	"flag"
	"os"
	"strconv"
	"strings"
	"time"

	"code.uber.internal/infra/memtsdb"
	"code.uber.internal/infra/memtsdb/bootstrap"
	"code.uber.internal/infra/memtsdb/bootstrap/bootstrapper/fs"
	"code.uber.internal/infra/memtsdb/storage"
)

var (
	bootstrapShards  = flag.String("bootstrapShards", "", "shards that need to be bootstrapped, separated by comma")
	bootstrapperList = flag.String("bootstrapperList", "filesystem", "data sources used for bootstrapping")
	filePathPrefix   = flag.String("filePathPrefix", "", "file path prefix for the raw data files stored on disk")
)

func main() {
	flag.Parse()

	if len(*bootstrapShards) == 0 || len(*bootstrapperList) == 0 {
		flag.PrintDefaults()
		os.Exit(1)
	}

	dbOptions := storage.NewDatabaseOptions()

	log := dbOptions.GetLogger()

	shardStrings := strings.Split(*bootstrapShards, ",")
	shards := make([]uint32, len(shardStrings))
	for i, ss := range shardStrings {
		sn, err := strconv.Atoi(ss)
		if err != nil {
			log.Fatalf("illegal shard string %d: %v", sn, err)
		}
		shards[i] = uint32(sn)
	}

	bootstrapperNames := strings.Split(*bootstrapperList, ",")
	var bs memtsdb.Bootstrapper
	for i := len(bootstrapperNames) - 1; i >= 0; i-- {
		switch bootstrapperNames[i] {
		case fs.FileSystemBootstrapperName:
			bs = fs.NewFileSystemBootstrapper(*filePathPrefix, dbOptions, bs)
		default:
			log.Fatalf("unrecognized bootstrapper name %s", bootstrapperNames[i])
		}
	}

	writeStart := time.Now()
	opts := bootstrap.NewOptions()
	b := bootstrap.NewBootstrapProcess(opts, dbOptions, bs)

	for _, shard := range shards {
		res, err := b.Run(writeStart, shard)
		if err != nil {
			log.Fatalf("unable to bootstrap shard %d: %v", shard, err)
		}
		log.Infof("shard %d has %d series", shard, len(res.GetAllSeries()))
	}

	log.Info("bootstrapping is done")
}
