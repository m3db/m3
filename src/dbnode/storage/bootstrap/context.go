package bootstrap

import (
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/x/context"
)

func NewBootstrapContext(options ContextOptions) *BootstrapContext {

}

func (c *BootstrapContext) Context() context.Context {
	return c.ctx
}
func (c *BootstrapContext) ReadInfoFiles(invalidateCache bool, namespaces Namespaces) (InfoFilesByNamespace, error) {
	if c.InfoFilesByNamespace != nil && !invalidateCache {
		return c.InfoFilesByNamespace, nil
	}

	infoFilesByNamespace := make(InfoFilesByNamespace)

	for _, elem := range namespaces.Namespaces.Iter() {
		namespace := elem.Value()
		shardTimeRanges := namespace.DataRunOptions.ShardTimeRanges
		result := make(InfoFileResultsPerShard, shardTimeRanges.Len())
		for shard := range shardTimeRanges.Iter() {
			result[shard] = fs.ReadInfoFiles(c.fsOpts.FilePathPrefix(),
				namespace.Metadata.ID(), shard, c.fsOpts.InfoReaderBufferSize(), c.fsOpts.DecodingOptions(),
				persist.FileSetFlushType)
		}

		infoFilesByNamespace[namespace.Metadata] = result
	}

	return infoFilesByNamespace, nil
}

type contextOptions struct {
	fsOpts     fs.Options
	namespaces Namespaces
}

func NewContextOptions() ContextOptions {
	return contextOptions{}
}

func (c contextOptions) Validate() error {
	panic("implement me")
}

func (c contextOptions) SetFilesystemOptions(value fs.Options) ContextOptions {
	panic("implement me")
}

func (c contextOptions) FilesystemOptions() fs.Options {
	panic("implement me")
}

func (c contextOptions) SetNamespaces(value Namespaces) ContextOptions {
	panic("implement me")
}

func (c contextOptions) Namespaces() Namespaces {
	panic("implement me")
}
