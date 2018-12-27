package debugdump

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

type fakeProvider struct {
	called    bool
	shouldErr bool
}

func (f *fakeProvider) ProvideData(w io.Writer) error {
	f.called = true
	if f.shouldErr {
		return errors.New("bad provide")
	}
	w.Write([]byte("test"))
	return nil
}

func TestDumpData(t *testing.T) {
	dataDumper := NewDataDumper()
	fp := &fakeProvider{}
	dataDumper.RegisterProvider("test", fp)
	buff := bytes.NewBuffer([]byte{})
	err := dataDumper.DumpData(buff)
	require.NotZero(t, buff.Len())
	require.True(t, fp.called)
	require.NoError(t, err)
}

func TestDumpDataErr(t *testing.T) {
	dataDumper := NewDataDumper()
	fp := &fakeProvider{
		shouldErr: true,
	}
	dataDumper.RegisterProvider("test", fp)
	buff := bytes.NewBuffer([]byte{})
	err := dataDumper.DumpData(buff)
	require.Error(t, err)
	require.True(t, fp.called)
}

func TestRegisterProviderSameName(t *testing.T) {
	dataDumper := NewDataDumper()
	fp := &fakeProvider{}
	err := dataDumper.RegisterProvider("test", fp)
	require.NoError(t, err)
	err = dataDumper.RegisterProvider("test", fp)
	require.Error(t, err)
}
