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
	"time"

	"github.com/m3db/m3/src/dbnode/retention"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestOptionsEquals(t *testing.T) {
	o1 := NewOptions()
	require.True(t, o1.Equal(o1))

	o2 := NewOptions()
	require.True(t, o1.Equal(o2))
	require.True(t, o2.Equal(o1))
}

func TestOptionsEqualsIndexOpts(t *testing.T) {
	o1 := NewOptions()
	o2 := o1.SetIndexOptions(
		o1.IndexOptions().SetBlockSize(
			o1.IndexOptions().BlockSize() * 2))
	require.True(t, o1.Equal(o1))
	require.True(t, o2.Equal(o2))
	require.False(t, o1.Equal(o2))
	require.False(t, o2.Equal(o1))
}

func TestOptionsEqualsSchema(t *testing.T) {
	o1 := NewOptions()
	s1, err := LoadSchemaHistory(testSchemaOptions)
	require.NoError(t, err)
	require.NotNil(t, s1)
	o2 := o1.SetSchemaHistory(s1)
	require.True(t, o1.Equal(o1))
	require.True(t, o2.Equal(o2))
	require.False(t, o1.Equal(o2))
	require.False(t, o2.Equal(o1))
}

func TestOptionsEqualsRetention(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	r1 := retention.NewMockOptions(ctrl)
	o1 := NewOptions().SetRetentionOptions(r1)

	r1.EXPECT().Equal(r1).Return(true)
	require.True(t, o1.Equal(o1))

	r2 := retention.NewMockOptions(ctrl)
	o2 := NewOptions().SetRetentionOptions(r2)

	r1.EXPECT().Equal(r2).Return(true)
	require.True(t, o1.Equal(o2))

	r1.EXPECT().Equal(r2).Return(false)
	require.False(t, o1.Equal(o2))

	r2.EXPECT().Equal(r1).Return(false)
	require.False(t, o2.Equal(o1))

	r2.EXPECT().Equal(r1).Return(true)
	require.True(t, o2.Equal(o1))
}

func TestOptionsValidate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rOpts := retention.NewMockOptions(ctrl)
	iOpts := NewMockIndexOptions(ctrl)
	o1 := NewOptions().
		SetRetentionOptions(rOpts).
		SetIndexOptions(iOpts)

	iOpts.EXPECT().Enabled().Return(true).AnyTimes()

	rOpts.EXPECT().Validate().Return(nil)
	rOpts.EXPECT().RetentionPeriod().Return(time.Hour)
	rOpts.EXPECT().FutureRetentionPeriod().Return(time.Duration(0))
	rOpts.EXPECT().BlockSize().Return(time.Hour)
	iOpts.EXPECT().BlockSize().Return(time.Hour)
	require.NoError(t, o1.Validate())

	rOpts.EXPECT().Validate().Return(nil)
	rOpts.EXPECT().RetentionPeriod().Return(time.Hour)
	rOpts.EXPECT().FutureRetentionPeriod().Return(time.Duration(0))
	rOpts.EXPECT().BlockSize().Return(time.Hour)
	iOpts.EXPECT().BlockSize().Return(2 * time.Hour)
	require.Error(t, o1.Validate())

	rOpts.EXPECT().Validate().Return(fmt.Errorf("test error"))
	require.Error(t, o1.Validate())
}

func TestOptionsValidateWithExtendedOptions(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	extendedOpts := NewMockExtendedOptions(ctrl)
	opts := NewOptions().SetExtendedOptions(extendedOpts)

	extendedOpts.EXPECT().Validate().Return(nil)
	require.NoError(t, opts.Validate())

	extendedOpts.EXPECT().Validate().Return(fmt.Errorf("test error"))
	require.Error(t, opts.Validate())
}

func TestOptionsValidateBlockSizeMustBeMultiple(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rOpts := retention.NewMockOptions(ctrl)
	iOpts := NewMockIndexOptions(ctrl)
	o1 := NewOptions().
		SetRetentionOptions(rOpts).
		SetIndexOptions(iOpts)

	iOpts.EXPECT().Enabled().Return(true).AnyTimes()

	rOpts.EXPECT().Validate().Return(nil)
	rOpts.EXPECT().RetentionPeriod().Return(4 * time.Hour).AnyTimes()
	rOpts.EXPECT().FutureRetentionPeriod().Return(time.Duration(0)).AnyTimes()
	rOpts.EXPECT().BlockSize().Return(2 * time.Hour).AnyTimes()
	iOpts.EXPECT().BlockSize().Return(3 * time.Hour).AnyTimes()
	require.Error(t, o1.Validate())
}

func TestOptionsValidateBlockSizePositive(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rOpts := retention.NewMockOptions(ctrl)
	iOpts := NewMockIndexOptions(ctrl)
	o1 := NewOptions().
		SetRetentionOptions(rOpts).
		SetIndexOptions(iOpts)

	iOpts.EXPECT().Enabled().Return(true).AnyTimes()

	rOpts.EXPECT().Validate().Return(nil)
	rOpts.EXPECT().RetentionPeriod().Return(4 * time.Hour).AnyTimes()
	rOpts.EXPECT().FutureRetentionPeriod().Return(time.Duration(0)).AnyTimes()
	rOpts.EXPECT().BlockSize().Return(2 * time.Hour).AnyTimes()
	iOpts.EXPECT().BlockSize().Return(0 * time.Hour).AnyTimes()
	require.Error(t, o1.Validate())

	rOpts.EXPECT().Validate().Return(nil)
	rOpts.EXPECT().RetentionPeriod().Return(4 * time.Hour).AnyTimes()
	rOpts.EXPECT().BlockSize().Return(2 * time.Hour).AnyTimes()
	iOpts.EXPECT().BlockSize().Return(-2 * time.Hour).AnyTimes()
	require.Error(t, o1.Validate())
}

func TestOptionsValidateNoIndexing(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rOpts := retention.NewMockOptions(ctrl)
	iOpts := NewMockIndexOptions(ctrl)
	o1 := NewOptions().
		SetRetentionOptions(rOpts).
		SetIndexOptions(iOpts)

	iOpts.EXPECT().Enabled().Return(false).AnyTimes()

	rOpts.EXPECT().Validate().Return(nil)
	require.NoError(t, o1.Validate())
}

func TestOptionsValidateStagingStatus(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rOpts := retention.NewMockOptions(ctrl)
	iOpts := NewMockIndexOptions(ctrl)
	o1 := NewOptions().
		SetRetentionOptions(rOpts).
		SetIndexOptions(iOpts)

	iOpts.EXPECT().Enabled().Return(true).AnyTimes()

	rOpts.EXPECT().Validate().Return(nil).AnyTimes()
	rOpts.EXPECT().RetentionPeriod().Return(time.Hour).AnyTimes()
	rOpts.EXPECT().FutureRetentionPeriod().Return(time.Duration(0)).AnyTimes()
	rOpts.EXPECT().BlockSize().Return(time.Hour).AnyTimes()
	iOpts.EXPECT().BlockSize().Return(time.Hour).AnyTimes()
	require.NoError(t, o1.Validate())

	o1 = o1.SetStagingState(StagingState{status: StagingStatus(12)})
	require.Error(t, o1.Validate())
}
