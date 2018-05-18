// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package namespace

import (
	"fmt"
	"testing"

	"github.com/m3db/m3db/src/dbnode/retention"
	"github.com/m3db/m3x/ident"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestMetadataEqualsTrue(t *testing.T) {

	testID := ident.StringID("some-string")
	testOpts := NewOptions()
	md1, err := NewMetadata(testID, testOpts)
	require.NoError(t, err)
	md2, err := NewMetadata(testID, testOpts)
	require.NoError(t, err)

	require.True(t, md1.Equal(md1))
	require.True(t, md1.Equal(md2))
	require.True(t, md2.Equal(md1))
}

func TestMetadataEqualsIDsDiffer(t *testing.T) {
	testID1 := ident.StringID("some-string-1")
	testID2 := ident.StringID("some-string-2")
	testOpts := NewOptions()
	md1, err := NewMetadata(testID1, testOpts)
	require.NoError(t, err)
	md2, err := NewMetadata(testID2, testOpts)
	require.NoError(t, err)
	require.False(t, md1.Equal(md2))
	require.False(t, md2.Equal(md1))
}

func TestMetadataEqualsOptsDiffer(t *testing.T) {
	testID := ident.StringID("some-string")
	testOpts1 := NewOptions()
	testOpts2 := testOpts1.SetBootstrapEnabled(!testOpts1.BootstrapEnabled())
	md1, err := NewMetadata(testID, testOpts1)
	require.NoError(t, err)
	md2, err := NewMetadata(testID, testOpts2)
	require.NoError(t, err)
	require.False(t, md1.Equal(md2))
	require.False(t, md2.Equal(md1))
}

func TestMetadataEqualsRetentionOptsDiffer(t *testing.T) {
	testID := ident.StringID("some-string")
	testOpts1 := NewOptions()
	ropts := testOpts1.RetentionOptions()
	testOpts2 := testOpts1.SetRetentionOptions(ropts.SetBlockSize(ropts.BlockSize() * 2))
	md1, err := NewMetadata(testID, testOpts1)
	require.NoError(t, err)
	md2, err := NewMetadata(testID, testOpts2)
	require.NoError(t, err)
	require.False(t, md1.Equal(md2))
	require.False(t, md2.Equal(md1))
}

func TestMetadataValidateEmptyID(t *testing.T) {
	testID := ident.StringID("")
	testOpts1 := NewOptions()
	_, err := NewMetadata(testID, testOpts1)
	require.Error(t, err)
}

func TestMetadataValidateRetentionErr(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockROpts := retention.NewMockOptions(ctrl)
	testID := ident.StringID("some-string")
	testOpts1 := NewOptions().SetRetentionOptions(mockROpts)

	mockROpts.EXPECT().Validate().Return(nil)
	_, err := NewMetadata(testID, testOpts1)
	require.NoError(t, err)

	mockROpts.EXPECT().Validate().Return(fmt.Errorf("some-error"))
	_, err = NewMetadata(testID, testOpts1)
	require.Error(t, err)
}
