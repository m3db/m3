package fs

import (
	"os"
	"time"

	"code.uber.internal/infra/memtsdb"
	"code.uber.internal/infra/memtsdb/bootstrap"
	"code.uber.internal/infra/memtsdb/persist/fs"
	"code.uber.internal/infra/memtsdb/storage"
	xtime "code.uber.internal/infra/memtsdb/x/time"

	log "github.com/Sirupsen/logrus"
)

// fileSystemSource provides information about TSDB data stored on disk.
type fileSystemSource struct {
	opts           memtsdb.DatabaseOptions
	filePathPrefix string
}

// newFileSystemSource creates a new filesystem based database.
func newFileSystemSource(prefix string, opts memtsdb.DatabaseOptions) memtsdb.Source {
	return &fileSystemSource{
		opts:           opts,
		filePathPrefix: prefix,
	}
}

func (fss *fileSystemSource) getInfoFiles(shard uint32) ([]string, error) {
	shardDir := fs.ShardDirPath(fss.filePathPrefix, shard)
	return fs.InfoFiles(shardDir)
}

// GetAvailability returns what time ranges are available for a given shard.
func (fss *fileSystemSource) GetAvailability(shard uint32, targetRangesForShard xtime.Ranges) xtime.Ranges {
	if targetRangesForShard == nil {
		return nil
	}
	files, err := fss.getInfoFiles(shard)
	if err != nil {
		log.Errorf("unable to get info files for shard %d: %v", shard, err)
		return nil
	}
	numFiles := len(files)
	tr := xtime.NewRanges()
	for i := 0; i < numFiles; i++ {
		f, err := os.Open(files[i])
		if err != nil {
			log.Errorf("unable to open info file %s: %v", files[i], err)
			continue
		}
		info, err := fs.ReadInfo(f)
		f.Close()
		if err != nil {
			log.Errorf("unable to read info file %s: %v", files[i], err)
			continue
		}
		t := xtime.FromNanoseconds(info.Start)
		w := time.Duration(info.Window)
		curRange := xtime.Range{t, t.Add(w)}
		if targetRangesForShard.Contains(curRange) {
			tr.AddRange(curRange)
		}
	}
	return tr
}

// ReadData returns raw series for a given shard within certain time ranges.
func (fss *fileSystemSource) ReadData(shard uint32, tr xtime.Ranges) (memtsdb.ShardResult, xtime.Ranges) {
	if xtime.IsEmpty(tr) {
		return nil, tr
	}
	files, err := fss.getInfoFiles(shard)
	if err != nil {
		log.Errorf("unable to get info files for shard %d: %v", shard, err)
		return nil, tr
	}
	seriesMap := bootstrap.NewShardResult(fss.opts)
	r := fs.NewReader(fss.filePathPrefix)
	for i := 0; i < len(files); i++ {
		v, err := fs.VersionFromName(files[i])
		if err != nil {
			log.Errorf("unable to get version from info file name %s: %v", files[i], err)
			continue
		}
		if err := r.Open(shard, v); err != nil {
			log.Errorf("unable to open info file for shard %d version %d: %v", shard, v, err)
			continue
		}
		timeRange := r.Range()
		if !tr.Contains(timeRange) {
			r.Close()
			continue
		}
		hasError := false
		curMap := bootstrap.NewShardResult(fss.opts)
		for i := 0; i < r.Entries(); i++ {
			id, data, err := r.Read()
			if err != nil {
				log.Errorf("error reading data file for shard %d version %d: %v", shard, v, err)
				hasError = true
				break
			}
			encoder := fss.opts.GetEncoderPool().Get()
			encoder.ResetSetData(timeRange.Start, data, false)
			block := storage.NewDatabaseBlock(timeRange.Start, encoder, fss.opts)
			curMap.AddBlock(id, block)
		}
		r.Close()

		if !hasError {
			seriesMap.AddResult(curMap)
			tr = tr.RemoveRange(timeRange)
		}
	}
	return seriesMap, tr
}
