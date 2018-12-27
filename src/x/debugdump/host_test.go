package debugdump

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHostDataProvider(t *testing.T) {
	prov := NewHostDataProvider()
	buff := bytes.NewBuffer([]byte{})
	prov.ProvideData(buff)
	require.NotZero(t, buff.Len())
}
