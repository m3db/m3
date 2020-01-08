package flagvar

import (
	"os"
	"strings"
)

// File is a `flag.Value` for file path arguments.
// By default, any errors from os.Stat are returned.
// Alternatively, the value of the `Validate` field is used as a validator when specified.
type File struct {
	Validate func(os.FileInfo, error) error

	Value string
}

// Set is flag.Value.Set
func (fv *File) Set(v string) error {
	info, err := os.Stat(v)
	fv.Value = v
	if fv.Validate != nil {
		return fv.Validate(info, err)
	}
	return err
}

func (fv *File) String() string {
	return fv.Value
}

// Files is a `flag.Value` for file path arguments.
// By default, any errors from os.Stat are returned.
// Alternatively, the value of the `Validate` field is used as a validator when specified.
type Files struct {
	Validate func(os.FileInfo, error) error

	Values []string
}

// Set is flag.Value.Set
func (fv *Files) Set(v string) error {
	info, err := os.Stat(v)
	fv.Values = append(fv.Values, v)
	if fv.Validate != nil {
		return fv.Validate(info, err)
	}
	return err
}

func (fv *Files) String() string {
	return strings.Join(fv.Values, ",")
}
