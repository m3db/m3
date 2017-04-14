package config

import (
	"errors"
	"io/ioutil"

	validator "gopkg.in/validator.v2"
	yaml "gopkg.in/yaml.v2"
)

// errNoFilesToLoad is return when you attemp to call LoadFiles with no file paths
var errNoFilesToLoad = errors.New("attempt to load configuration with no files")

// LoadFile loads a config from a file
func LoadFile(config interface{}, fname string) error {
	return loadFiles(config, fname)
}

// loadFiles loads a config from list of files
// If value for a property is present in multiple files, the value from the last file will be applied
// Validation is done after merging all values
// TODO(cw) export this function if needed
func loadFiles(config interface{}, fnames ...string) error {
	if len(fnames) == 0 {
		return errNoFilesToLoad
	}
	for _, fname := range fnames {
		data, err := ioutil.ReadFile(fname)
		if err != nil {
			return err
		}

		if err := yaml.Unmarshal(data, config); err != nil {
			return err
		}
	}

	return validator.Validate(config)
}
