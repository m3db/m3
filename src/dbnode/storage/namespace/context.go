package namespace

import "github.com/m3db/m3/src/x/ident"

type Context struct {
	Id ident.ID
	Schema SchemaDescr
}
