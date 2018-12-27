package debugdump

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHeapDumpProvider(t *testing.T) {
	prov := NewHeapDumpProvider()
	buff := bytes.NewBuffer([]byte{})
	prov.ProvideData(buff)
	require.NotZero(t, buff.Len())
}
