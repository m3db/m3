package namespace

import (
	"github.com/m3db/m3/src/metrics/metric/id"
)

// NewTestID creates a new ID for testing that always returns the namespace for TagValue.
func NewTestID(id, namespace string) id.ID {
	return &testID{
		id: []byte(id),
		ns: []byte(namespace),
	}
}

type testID struct {
	id []byte
	ns []byte
}

func (t testID) Bytes() []byte {
	return t.id
}

func (t testID) TagValue(_ []byte) ([]byte, bool) {
	return t.ns, true
}
