package fs

import (
	"code.uber.internal/infra/memtsdb"
	"code.uber.internal/infra/memtsdb/bootstrap/bootstrapper"
)

const (
	// FileSystemBootstrapperName is the name of th filesystem bootstrapper.
	FileSystemBootstrapperName = "filesystem"
)

type fileSystemBootstrapper struct {
	memtsdb.Bootstrapper
}

// NewFileSystemBootstrapper creates a new bootstrapper to bootstrap from on-disk files.
func NewFileSystemBootstrapper(
	prefix string,
	dbOpts memtsdb.DatabaseOptions,
	next memtsdb.Bootstrapper,
) memtsdb.Bootstrapper {
	fss := newFileSystemSource(prefix, dbOpts)
	return &fileSystemBootstrapper{
		Bootstrapper: bootstrapper.NewBaseBootstrapper(fss, dbOpts, next),
	}
}

func (fsb *fileSystemBootstrapper) String() string {
	return FileSystemBootstrapperName
}
