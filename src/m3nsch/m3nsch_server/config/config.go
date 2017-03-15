package config

import (
	"github.com/spf13/viper"
	tallym3 "github.com/uber-go/tally/m3"
	validator "gopkg.in/validator.v2"
)

// Configuration represents the knobs available to configure a m3nsch_server
type Configuration struct {
	Server  ServerConfiguration  `yaml:"server"`
	M3nsch  M3nschConfiguration  `yaml:"m3nsch" validate:"nonzero"`
	Metrics MetricsConfiguration `yaml:"metrics" validate:"nonzero"`
}

// ServerConfiguration represents the knobs available to configure server properties
type ServerConfiguration struct {
	ListenAddress string  `yaml:"listenAddress" validate:"nonzero"`
	DebugAddress  string  `yaml:"debugAddress"  validate:"nonzero"`
	CPUFactor     float64 `yaml:"cpuFactor"     validate:"min=0.5,max=3.0"`
}

// MetricsConfiguration represents the knobs available to configure metrics collection
type MetricsConfiguration struct {
	Prefix     string                `yaml:"prefix"`
	SampleRate float64               `yaml:"sampleRate" validate:"min=0.01,max=1.0"`
	M3         tallym3.Configuration `yaml:"metrics"    validate:"nonzero"`
}

// M3nschConfiguration represents the knobs available to configure m3nsch properties
type M3nschConfiguration struct {
	Concurrency       int `yaml:"concurrency"       validate:"min=500,max=5000"`
	NumPointsPerDatum int `yaml:"numPointsPerDatum" validate:"min=10,max=120"`
}

// New returns a Configuration read from the specified path
func New(filename string) (Configuration, error) {
	viper.SetConfigType("yaml")
	viper.SetConfigFile(filename)
	var conf Configuration

	if err := viper.ReadInConfig(); err != nil {
		return conf, err
	}

	if err := viper.Unmarshal(&conf); err != nil {
		return conf, err
	}

	if err := validator.Validate(conf); err != nil {
		return conf, err
	}

	return conf, nil
}
