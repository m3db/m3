package tsz

var (
	// default encoding options
	defaultOptions = newOptions()
)

// Options represents different options for encoding time as well as markers.
type Options interface {

	// GetTimeEncodingSchemes returns the time encoding schemes for different time units.
	GetTimeEncodingSchemes() TimeEncodingSchemes

	// TimeEncodingSchemes sets the time encoding schemes for different time units.
	TimeEncodingSchemes(value TimeEncodingSchemes) Options

	// GetMarkerEncodingScheme returns the marker encoding scheme.
	GetMarkerEncodingScheme() MarkerEncodingScheme

	// MarkerEncodingScheme sets the marker encoding scheme.
	MarkerEncodingScheme(value MarkerEncodingScheme) Options
}

type options struct {
	timeEncodingSchemes  TimeEncodingSchemes
	markerEncodingScheme MarkerEncodingScheme
}

func newOptions() Options {
	return &options{
		timeEncodingSchemes:  defaultTimeEncodingSchemes,
		markerEncodingScheme: defaultMarkerEncodingScheme,
	}
}

// GetTimeEncodingScheme returns the time encoding schemes for different time units.
func (o *options) GetTimeEncodingSchemes() TimeEncodingSchemes {
	return o.timeEncodingSchemes
}

// TimeEncodingSchemes sets the time encoding schemes for different time units.
func (o *options) TimeEncodingSchemes(value TimeEncodingSchemes) Options {
	opts := *o
	opts.timeEncodingSchemes = value
	return &opts
}

// GetMarkerEncodingScheme returns the marker encoding scheme.
func (o *options) GetMarkerEncodingScheme() MarkerEncodingScheme {
	return o.markerEncodingScheme
}

// MarkerEncodingScheme sets the marker encoding scheme.
func (o *options) MarkerEncodingScheme(value MarkerEncodingScheme) Options {
	opts := *o
	opts.markerEncodingScheme = value
	return &opts
}

// NewOptions creates a new options.
func NewOptions() Options {
	return defaultOptions
}
