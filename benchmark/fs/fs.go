package fs

import (
	"errors"
	"os"
)

const (
	eofMarker = ^uint64(0)
	fileMode  = os.FileMode(0666)
)

var (
	errReadDataEntryNoSize = errors.New("unable to read data entry size")
)
