package common

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/m3db/m3/src/query/graphite/errors"
	"github.com/m3db/m3/src/query/graphite/ts"
)

type bucketType int

const (
	histogramUnknown bucketType = iota
	histogramValue
	histogramDuration

	infinity         = "infinity"
	negativeInfinity = "-infinity"
)

var (
	errHistogramSeriesInfinite = errors.New("cannot draw a distribution of 0-infinity")
	errHistogramSeriesNil      = errors.New("cannot use nil series")
	errHistogramCollectionSize = errors.New("histograms must have at least one bucket")
	errUnknownHistogramType    = errors.New("cannot use an unknown histogram type in a collection")
)

// bag o' tag names to avoid argument ordering issues
type histogramTagInfo struct {
	id     string
	bucket string
}

type histogramCollection struct {
	ctx     *Context
	hs      []*histogramSeries
	tagInfo histogramTagInfo
}

func newHistogramCollection(ctx *Context, in []*ts.Series, ti histogramTagInfo) (*histogramCollection, error) {
	if len(in) == 0 {
		return nil, errHistogramCollectionSize
	}
	hc := &histogramCollection{
		ctx:     ctx,
		hs:      make([]*histogramSeries, len(in)),
		tagInfo: ti,
	}

	type bucketKey struct {
		low, high float64
	}

	seenRanges := make(map[bucketKey]struct{}, len(in))
	seenType := histogramUnknown

	for i, s := range in {
		ms, err := newHistogramSeries(s, ti)
		if err != nil {
			return nil, fmt.Errorf("cannot convert %q to histogram series: %v", s.Name(), err)
		}
		bt := ms.BucketType()
		if bt == histogramUnknown {
			return nil, errUnknownHistogramType
		}
		if seenType == histogramUnknown {
			seenType = bt
		} else if seenType != bt {
			return nil, fmt.Errorf("cannot mix bucketTypes, previously %v now %v", seenType, bt)
		}

		// TODO: detect overlapping buckets
		tr := bucketKey{ms.Lower(), ms.Upper()}
		if _, ok := seenRanges[tr]; ok {
			return nil, fmt.Errorf("already seen range %q", ms.String())
		}
		seenRanges[tr] = struct{}{}

		hc.hs[i] = ms
	}

	first := hc.seriesAt(0)
	for i := 1; i < hc.Len(); i++ {
		s := hc.seriesAt(i)
		if s.Len() != first.Len() {
			return nil, fmt.Errorf("length of all series must be equal to %v, %q is %v", first.Len(), s.Name(), s.Len())
		}
		if s.MillisPerStep() != first.MillisPerStep() {
			return nil, fmt.Errorf("millisPerStep for all series must be equal to %v, %q is %v", first.MillisPerStep(), s.Name(), s.MillisPerStep())
		}
	}
	sort.Sort(hc)

	return hc, nil
}

// Len returns the number of series in collection
func (hc histogramCollection) Len() int { return len(hc.hs) }

// Swap swaps two series in the collection
func (hc histogramCollection) Swap(i, j int) { hc.hs[i], hc.hs[j] = hc.hs[j], hc.hs[i] }

// Less determines if a series is ordered before another series by
// comparing the midpoint of the range covered by the series.
func (hc histogramCollection) Less(i, j int) bool {
	ii, jj := hc.hs[i], hc.hs[j]

	im := ii.Upper() + ii.Lower()
	jm := jj.Upper() + jj.Lower()
	if im != jm {
		return im < jm
	}
	return ii.bucketID < jj.bucketID
}

func (hc histogramCollection) seriesAt(i int) *ts.Series {
	return hc.hs[i].s
}

func (hc histogramCollection) finish(
	v ts.Values,
	source string,
	value string,
) *ts.Series {
	query := fmt.Sprintf(" | %s %v %v %v", source, hc.tagInfo.id, hc.tagInfo.bucket, value)
	first := hc.seriesAt(0)
	series := ts.NewSeries(hc.ctx, first.Name()+query, first.StartTime(), v)
	ft := make(map[string]string, len(first.Tags)-1)
	ft[source] = value
	for k, v := range first.Tags {
		// Strip the tags we are using to aggregate, propagate the remaining.
		if k != hc.tagInfo.id && k != hc.tagInfo.bucket {
			ft[k] = v
		}
	}
	series.Tags = ft
	return series
}

type groupBucket struct {
	series []*ts.Series
	values map[string]string
}

func (g *groupBucket) insert(in *ts.Series, exempt histogramTagInfo) bool {
	for k, v := range g.values {
		if k == exempt.id || k == exempt.bucket {
			continue
		}
		if tag, exists := in.Tags[k]; !exists || tag != v {
			return false
		}
	}
	g.series = append(g.series, in)
	return true
}

type histogramCollections []*histogramCollection

func createCollections(
	ctx *Context,
	in []*ts.Series,
	hti histogramTagInfo,
) (histogramCollections, error) {
	var buckets []*groupBucket
	for _, s := range in {
		inserted := false
		for _, b := range buckets {
			inserted = b.insert(s, hti)
			if inserted {
				break
			}
		}
		if inserted {
			continue
		}
		// Failed to insert to any existing buckets, create a new one.
		gb := &groupBucket{
			series: []*ts.Series{s},
			values: make(map[string]string, len(s.Tags)),
		}
		for k, v := range s.Tags {
			gb.values[k] = v
		}
		buckets = append(buckets, gb)
	}
	hc := make(histogramCollections, len(buckets))
	for i, b := range buckets {
		var err error
		hc[i], err = newHistogramCollection(ctx, b.series, hti)
		if err != nil {
			return nil, err
		}
	}
	return hc, nil
}

// modifies the slice in place
func bucketedHistogram(hc *histogramCollection, buckets []float64, pos int) float64 {
	total := 0.0
	for i := 0; i < hc.Len(); i++ {
		if v := hc.seriesAt(i).ValueAt(pos); !math.IsNaN(v) {
			total += v
		}
		buckets[i] = total
	}
	return total
}

// HistogramPercentile returns a single series containing the value of the
// specific percentile at that point in time.  It expects a list of series
// named using a standard defined by Histogram.
func HistogramPercentile(
	ctx *Context,
	in ts.SeriesList,
	bucketID, bucketName string,
	values []float64,
) (ts.SeriesList, error) {
	if len(values) == 0 {
		return ts.SeriesList{}, fmt.Errorf("invalid values arg: %v", values)
	}

	hcs, err := createCollections(
		ctx,
		in.Values,
		histogramTagInfo{id: bucketID, bucket: bucketName},
	)
	if err != nil {
		return ts.SeriesList{}, err
	}

	// remove duplicates if they exist.
	dedup := make(map[float64]struct{}, len(values))
	sorted := values[:0]
	for _, value := range values {
		if _, ok := dedup[value]; !ok {
			dedup[value] = struct{}{}
			sorted = append(sorted, value)
		}
	}

	sort.Float64s(sorted)

	ret := make([]*ts.Series, 0, len(hcs)*len(sorted))
	for i := range hcs {
		for _, value := range sorted {
			percentile, err := hcs[i].percentile(value)
			if err != nil {
				return ts.SeriesList{}, err
			}
			ret = append(ret, percentile)
		}
	}

	in.Values = ret
	in.SortApplied = true
	return in, nil
}

func (hc *histogramCollection) percentile(value float64) (*ts.Series, error) {
	if math.IsNaN(value) || value < 0.0 || value > 100.0 {
		return nil, fmt.Errorf("percentile must be [0..100], not %v", value)
	}

	first := hc.seriesAt(0)
	values := ts.NewValues(hc.ctx, first.MillisPerStep(), first.Len())

	// We reuse buckets each time w/o reallocating.
	buckets := make([]float64, hc.Len())
	for i := 0; i < first.Len(); i++ {
		var (
			val   float64
			total = bucketedHistogram(hc, buckets, i)
		)

		if total == 0 {
			val = math.NaN()
		} else {
			// We don't need to sort first, values are monotonically increasing.
			idx := sort.SearchFloat64s(buckets, total*(value/100.0))
			if idx < 0 || idx >= hc.Len() {
				return nil, errors.New("index out of range")
			}
			loc := hc.hs[idx]
			val = loc.DisplayScale(loc.Upper())
		}
		values.SetValueAt(i, val)
	}

	return hc.finish(values, "histogramPercentile", fmt.Sprint("p", value)), nil
}

// HistogramCDF returns the proportion of values in the histogram that were less
// than or equal to the supplied value. A histogram series is defined as two or
// more series with tags that define the boundaries.
func HistogramCDF(
	ctx *Context,
	in ts.SeriesList,
	bucketID, bucketTag string,
	values []string,
) (ts.SeriesList, error) {
	if len(values) == 0 {
		return ts.SeriesList{}, fmt.Errorf("invalid values arg: %v", values)
	}

	hcs, err := createCollections(
		ctx,
		in.Values,
		histogramTagInfo{id: bucketID, bucket: bucketTag},
	)
	if err != nil {
		return ts.SeriesList{}, err
	}

	// remove duplicates if they exist.
	dedup := make(map[string]struct{}, len(values))
	sorted := values[:0]
	for _, value := range values {
		if _, ok := dedup[value]; !ok {
			dedup[value] = struct{}{}
			sorted = append(sorted, value)
		}
	}

	sort.Strings(sorted)

	ret := make([]*ts.Series, 0, len(hcs)*len(sorted))
	for _, hc := range hcs {
		for _, value := range sorted {
			cdf, err := hc.cdf(value)
			if err != nil {
				return ts.SeriesList{}, err
			}
			ret = append(ret, cdf)
		}
	}

	in.Values = ret
	in.SortApplied = true
	return in, nil
}

func (hc *histogramCollection) cdf(value string) (*ts.Series, error) {
	var search float64
	if ns, err := time.ParseDuration(value); err == nil {
		search = float64(ns)
	} else {
		const searchScale = float64(time.Millisecond)
		var conversionError error
		if search, conversionError = strconv.ParseFloat(value, 64); conversionError != nil {
			return nil, fmt.Errorf("cannot parse duration (%v) or number (%v)", err, conversionError)
		}
		// Assume the user wanted milliseconds if the series is time-based.
		if hc.hs[0].BucketType() == histogramDuration {
			search *= searchScale
		}
	}
	first := hc.seriesAt(0)

	// Find the boundary where the search value is located. We'll sum up everything
	// before this location.
	loc := sort.Search(hc.Len(), func(i int) bool { return hc.hs[i].Upper() > search })

	cdfs := ts.NewValues(hc.ctx, first.MillisPerStep(), first.Len())
	for i := 0; i < first.Len(); i++ {
		before, after := 0.0, 0.0
		for j := 0; j < hc.Len(); j++ {
			v := hc.seriesAt(j).ValueAt(i)

			// Skip negative values? Error on negative values?
			if math.IsNaN(v) {
				continue
			}
			if j < loc {
				before += v
			} else {
				after += v
			}
		}
		if before+after == 0.0 {
			// We either skipped everything or negative numbers are offsetting.
			cdfs.SetValueAt(i, math.NaN())
		} else {
			cdfs.SetValueAt(i, before/(before+after))
		}
	}

	return hc.finish(cdfs, "histogramCDF", value), nil
}

type histogramRange interface {
	Lower() float64
	Upper() float64
	String() string
	BucketType() bucketType
	DisplayScale(f float64) float64
}

type histogramValueRange struct{ low, high float64 }

func (hvr histogramValueRange) Lower() float64             { return hvr.low }
func (hvr histogramValueRange) Upper() float64             { return hvr.high }
func (hvr histogramValueRange) String() string             { return fmt.Sprintf("%v-%v", hvr.low, hvr.high) }
func (histogramValueRange) BucketType() bucketType         { return histogramValue }
func (histogramValueRange) DisplayScale(f float64) float64 { return f }

type histogramDurationRange struct{ low, high time.Duration }

func (hdr histogramDurationRange) Lower() float64             { return float64(hdr.low) }
func (hdr histogramDurationRange) Upper() float64             { return float64(hdr.high) }
func (hdr histogramDurationRange) String() string             { return fmt.Sprintf("%v-%v", hdr.low, hdr.high) }
func (histogramDurationRange) BucketType() bucketType         { return histogramDuration }
func (histogramDurationRange) DisplayScale(f float64) float64 { return f / float64(time.Millisecond) }

type seriesByNumericName []*ts.Series

func (a seriesByNumericName) Len() int      { return len(a) }
func (a seriesByNumericName) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a seriesByNumericName) Less(i, j int) bool {
	left, errLeft := strconv.ParseFloat(a[i].Name(), 64)
	right, errRight := strconv.ParseFloat(a[j].Name(), 64)
	if errLeft != nil && errRight != nil {
		return a[i].Name() < a[j].Name() // Fall back to normal string comparison
	}
	if errLeft != nil {
		return false // Order alphanumeric below numeric sorted names
	}
	if errRight != nil {
		return true // Order alphanumeric below numeric sorted names
	}
	return left < right
}

// AliasByHistogramBucket will alias each series by a histogram bucket tag's high value
func AliasByHistogramBucket(
	_ *Context,
	in ts.SeriesList,
	bucketTag string,
) (ts.SeriesList, error) {
	renamed := make([]*ts.Series, in.Len())
	for i, s := range in.Values {
		value, exists := s.Tags[bucketTag]
		if !exists {
			err := fmt.Errorf("no bucket tag for: %s", s.Name())
			return ts.SeriesList{}, errors.NewInvalidParamsError(err)
		}

		left, right, err := parseBucket(value)
		if err != nil {
			return ts.SeriesList{}, errors.NewInvalidParamsError(err)
		}

		if drange, de := parseDurationRange(left, right); de == nil {
			ms := drange.high / time.Millisecond
			renamed[i] = s.RenamedTo(strconv.Itoa(int(ms)))
			continue
		}
		if vrange, re := parseValueRange(left, right); re == nil {
			renamed[i] = s.RenamedTo(strconv.Itoa(int(vrange.high)))
			continue
		}

		err = fmt.Errorf("bad bucket tag '%s' for: %s",
			value, s.Name())
		return ts.SeriesList{}, errors.NewInvalidParamsError(err)
	}

	sort.Sort(seriesByNumericName(renamed))

	in.Values = renamed
	in.SortApplied = true
	return in, nil
}

type histogramSeries struct {
	s *ts.Series

	bucketID int
	bucket   string

	histogramRange
}

func newHistogramSeries(s *ts.Series, ti histogramTagInfo) (*histogramSeries, error) {
	if s == nil {
		return nil, errHistogramSeriesNil
	}
	hs := &histogramSeries{s: s}
	if err := hs.bucketTags(ti); err != nil {
		return nil, err
	}
	left, right, err := parseBucket(hs.bucket)
	if err != nil {
		return nil, err
	}
	var de, re error
	if hs.histogramRange, de = parseDurationRange(left, right); de == nil {
		return hs, nil
	}
	if hs.histogramRange, re = parseValueRange(left, right); re == nil {
		return hs, nil
	}
	return nil, fmt.Errorf("cannot parse as duration range: %v; cannot parse as value range: %v", de, re)
}

func (hs *histogramSeries) bucketTags(ti histogramTagInfo) error {
	bucket, ok := hs.s.Tags[ti.bucket]
	if !ok || bucket == "" {
		return fmt.Errorf("cannot find %q in %v", ti.bucket, hs.s.Tags)
	}
	hs.bucket = bucket

	bucketid, ok := hs.s.Tags[ti.id]
	if !ok || bucketid == "" {
		return fmt.Errorf("cannot find %q in %v", ti.id, hs.s.Tags)
	}
	var err error
	hs.bucketID, err = strconv.Atoi(bucketid)
	return err
}

func parseBucket(label string) (string, string, error) {
	const delimiter = '-'
	if label == "0"+string(delimiter)+infinity {
		return "", "", errHistogramSeriesInfinite
	}

	idx := strings.IndexByte(label, delimiter)
	if idx == -1 || len(label) == idx+1 {
		return "", "", fmt.Errorf("cannot split the label %q", label)
	}

	secIdx := strings.IndexByte(label[idx+1:], delimiter)
	if secIdx == -1 {
		return label[:idx], label[idx+1:], nil
	}

	secIdx += idx + 1
	if len(label) == secIdx+1 {
		return "", "", fmt.Errorf("cannot split the label %q", label)
	}

	if secIdx == idx+1 {
		// case where the first 2 instances of '-' are in a row. eg 1--2
		return "", "", fmt.Errorf("invalid range %q", label)
	}

	return label[:secIdx], label[secIdx+1:], nil
}

func parseValueRange(left, right string) (histogramValueRange, error) {
	hs := histogramValueRange{}
	var err error
	// Secretly support float ranges in buckets.
	if hs.low, err = strconv.ParseFloat(left, 64); err != nil {
		return hs, fmt.Errorf("cannot parse low value %q: %v", left, err)
	}
	if right == infinity {
		hs.high = hs.low
		return hs, nil
	}
	if hs.high, err = strconv.ParseFloat(right, 64); err != nil {
		return hs, fmt.Errorf("cannot parse high value %q: %v", right, err)
	}
	if hs.low >= hs.high {
		return hs, fmt.Errorf("high value %v must exceed low value %v", hs.low, hs.high)
	}
	return hs, nil
}

func parseDurationRange(left, right string) (histogramDurationRange, error) {
	hs := histogramDurationRange{}
	// If the bucket begins with the zero value, it does not need a duration.
	earliest := time.Duration(0)

	if left != "0" && left != negativeInfinity {
		var err error
		if earliest, err = time.ParseDuration(left); err != nil || earliest == time.Duration(0) {
			return hs, fmt.Errorf("cannot parse low value %q: %v", left, err)
		}
	}

	if left == negativeInfinity {
		earliest = time.Duration(math.MinInt64)
	}

	hs.low = earliest
	if right == infinity {
		hs.high = earliest
		return hs, nil
	}

	latest, err := time.ParseDuration(right)
	if err != nil {
		return hs, fmt.Errorf("cannot parse high value %q: %v", right, err)
	}
	hs.high = latest

	if hs.low > hs.high {
		return hs, fmt.Errorf("high duration %v must exceed low duration %v", hs.low, hs.high)
	}
	if hs.low == hs.high {
		// It's ok if we set low=high when infinity is involved, but not any other time.
		// We treat this as a different error than low > high because two different strings
		// can evaluate to the same duration and we want surface that in the error.
		return hs, fmt.Errorf("durations cannot both be %v, %q == %q", hs.low, left, right)
	}

	return hs, nil
}
