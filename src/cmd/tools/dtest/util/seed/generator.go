package seed

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/m3db/m3/src/dbnode/integration/generate"
	"github.com/m3db/m3/src/dbnode/sharding"
	"github.com/m3db/m3cluster/shard"
	"github.com/m3db/m3x/ident"
	xlog "github.com/m3db/m3x/log"
)

// specific to data generation
const (
	letterBytes   = "`~<=>_-,/.[]{}@$#%+ݿ⬧	0123456789aAbBcCdDeEfFgGhHiIjJkKlLmMnNŋoOpPqQrRsStTuUvVwWxXyYzZ"
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

type generator struct {
	opts      Options
	logger    xlog.Logger
	r         *rand.Rand
	numPoints unifStats
	idLength  normStats
	ids       []string
}

// NewGenerator returns a new generator
func NewGenerator(opts Options) Generator {
	g := &generator{
		opts:   opts,
		logger: opts.InstrumentOptions().Logger(),
		r:      rand.New(opts.RandSource()),
		numPoints: unifStats{
			min: opts.MinNumPointsPerID(),
			max: opts.MaxNumPointsPerID(),
		},
		idLength: normStats{
			mean:   opts.IDLengthMean(),
			stddev: opts.IDLengthStddev(),
		},
	}

	for i := 0; i < opts.NumIDs(); i++ {
		idLen := g.idLength.sample(g.r)
		g.ids = append(g.ids, randStringBytesMaskImprSrc(idLen, opts.RandSource()))
	}
	return g
}

func (g *generator) Generate(namespace ident.ID, shard uint32) error {
	var (
		shardSet     = &fakeShardSet{shard}
		gOpts        = g.opts.GenerateOptions()
		blockSize    = gOpts.BlockSize()
		now          = gOpts.ClockOptions().NowFn()().Truncate(blockSize)
		start        = now.Add(-blockSize)
		earliest     = now.Add(-1 * gOpts.RetentionPeriod())
		blockConfigs []generate.BlockConfig
	)
	for start := start; !start.Before(earliest); start = start.Add(-gOpts.BlockSize()) {
		blockConfigs = append(blockConfigs, g.generateConf(start))
	}
	g.logger.Debug("created block configs")

	data := generate.BlocksByStart(blockConfigs)
	g.logger.Debug("created fake data")

	writer := generate.NewWriter(gOpts)
	err := writer.WriteData(namespace, shardSet, data)
	if err != nil {
		return fmt.Errorf("unable to write data: %v", err)
	}

	g.logger.Debug("data written to local fs")
	return nil
}

func (g *generator) generateConf(start time.Time) generate.BlockConfig {
	numPoints := g.numPoints.sample(g.r)
	return generate.BlockConfig{
		Start:     start,
		NumPoints: numPoints,
		IDs:       g.ids,
	}
}

// Taken from: https://stackoverflow.com/questions/22892120/how-to-generate-a-random-string-of-a-fixed-length-in-golang
func randStringBytesMaskImprSrc(n int, src rand.Source) string {
	b := make([]byte, n)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return string(b)
}

type unifStats struct {
	min int
	max int
}

func (s unifStats) sample(r *rand.Rand) int {
	return int(r.Int31n(int32(s.max-s.min))) + s.min
}

type normStats struct {
	mean   float64
	stddev float64
}

func (s normStats) sample(r *rand.Rand) int {
	n := int(r.NormFloat64()*s.stddev + s.mean)
	if n < 0 {
		n = n * -1
	}
	return n
}

type fakeShardSet struct {
	shardID uint32
}

func (f *fakeShardSet) All() []shard.Shard {
	sh := shard.NewShard(f.shardID)
	return []shard.Shard{sh}
}

func (f *fakeShardSet) AllIDs() []uint32 {
	return []uint32{f.shardID}
}

func (f *fakeShardSet) Lookup(id ident.ID) uint32 {
	return f.shardID
}

func (f *fakeShardSet) LookupStateByID(shardID uint32) (shard.State, error) {
	return shard.Available, nil
}

func (f *fakeShardSet) Min() uint32 {
	return f.shardID
}

func (f *fakeShardSet) Max() uint32 {
	return f.shardID
}

func (f *fakeShardSet) HashFn() sharding.HashFn {
	return nil
}
