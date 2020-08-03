package iterators

import (
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/ts"
	xtime "github.com/m3db/m3/src/x/time"
)

type BaseIterator = encoding.Iterator

type iteratorSample struct {
	dp   ts.Datapoint
	unit xtime.Unit
	annotation ts.Annotation
}
