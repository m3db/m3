package commitlog

import (
	"testing"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/dbnode/storage/testdata/prototest"
	"github.com/m3db/m3/src/dbnode/storage/namespace"

	"github.com/stretchr/testify/require"
)

var (
	testNamespace     = ident.StringID("commitlog_prototest_ns")
	testSchemaHistory = prototest.NewSchemaHistory("../../../testdata/prototest")
	testSchema        = prototest.NewMessageDescriptor(testSchemaHistory)
	testSchemaDesc    = namespace.GetTestSchemaDescr(testSchema)
	testProtoMessages = prototest.NewProtoTestMessages(testSchema)
	testProtoEqual    = func(expect, actual []byte) bool {
		return prototest.ProtoEqual(testSchema, expect, actual)
	}
)

func testProtoOptions() Options {
	resultOpts := testDefaultOpts.ResultOptions()
	bOpts := resultOpts.DatabaseBlockOptions().
		SetEncoderPool(prototest.ProtoPools.EncoderPool).
		SetReaderIteratorPool(prototest.ProtoPools.ReaderIterPool).
		SetMultiReaderIteratorPool(prototest.ProtoPools.MultiReaderIterPool)
	return testDefaultOpts.SetResultOptions(resultOpts.SetDatabaseBlockOptions(bOpts))
}

func testProtoNsMetadata(t *testing.T) namespace.Metadata {
	nOpts := namespace.NewOptions().SetSchemaHistory(testSchemaHistory)
	md, err := namespace.NewMetadata(testNamespace, nOpts)
	require.NoError(t, err)
	return md
}

func setProtoAnnotation(value []testValue) []testValue {
	protoIter := prototest.NewProtoMessageIterator(testProtoMessages)
	for _, v := range value {
		v.v = 0
		v.a = protoIter.Next()
	}
	return value
}

func TestProtoReadOrderedValues(t *testing.T) {
	opts := testProtoOptions()
	md := testProtoNsMetadata(t)
	testReadOrderedValues(t, opts, md, setProtoAnnotation, testProtoEqual)
}

func TestProtoReadUnorderedValues(t *testing.T) {
	opts := testProtoOptions()
	md := testProtoNsMetadata(t)
	testReadUnorderedValues(t, opts, md, setProtoAnnotation, testProtoEqual)
}

func TestProtoItMergesSnapshotsAndCommitLogs(t *testing.T) {
	opts := testProtoOptions()
	md := testProtoNsMetadata(t)
	testItMergesSnapshotsAndCommitLogs(t, opts, md, setProtoAnnotation, testProtoEqual)
}

