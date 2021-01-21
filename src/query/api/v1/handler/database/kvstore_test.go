// Copyright (c) 2021 Uber Technologies, Inc.
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

package database

import (
	"encoding/json"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/m3db/m3/src/cluster/generated/proto/kvpb"
	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/dbnode/kvconfig"
)

func TestUpdateQueryLimits(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tests := []struct {
		name          string
		limits        *kvpb.QueryLimits
		commit        bool
		expectedJSON  string
		expectedError string
	}{
		{
			name:         `nil`,
			limits:       nil,
			commit:       true,
			expectedJSON: "",
		},
		{
			name:         `empty`,
			limits:       &kvpb.QueryLimits{},
			commit:       true,
			expectedJSON: "",
		},
		{
			name: `only block - commit`,
			limits: &kvpb.QueryLimits{
				MaxRecentlyQueriedSeriesBlocks: &kvpb.QueryLimit{
					Limit:           1,
					LookbackSeconds: 15,
					ForceExceeded:   true,
				},
			},
			commit:       true,
			expectedJSON: `maxRecentlyQueriedSeriesBlocks:<limit:1 lookbackSeconds:15 forceExceeded:true > `,
		},
		{
			name: `only block - no commit`,
			limits: &kvpb.QueryLimits{
				MaxRecentlyQueriedSeriesBlocks: &kvpb.QueryLimit{
					Limit:           1,
					LookbackSeconds: 15,
					ForceExceeded:   true,
				},
			},
			commit:       false,
			expectedJSON: `maxRecentlyQueriedSeriesBlocks:<limit:1 lookbackSeconds:15 forceExceeded:true > `,
		},
		{
			name: `all - commit`,
			limits: &kvpb.QueryLimits{
				MaxRecentlyQueriedSeriesBlocks: &kvpb.QueryLimit{
					Limit:           1,
					LookbackSeconds: 15,
					ForceExceeded:   true,
				},
				MaxRecentlyQueriedSeriesDiskBytesRead: &kvpb.QueryLimit{
					Limit:           1,
					LookbackSeconds: 15,
					ForceExceeded:   true,
				},
				MaxRecentlyQueriedSeriesDiskRead: &kvpb.QueryLimit{
					Limit:           1,
					LookbackSeconds: 15,
					ForceExceeded:   true,
				},
			},
			commit: true,
			// nolint: lll
			expectedJSON: `maxRecentlyQueriedSeriesBlocks:<limit:1 lookbackSeconds:15 forceExceeded:true > maxRecentlyQueriedSeriesDiskBytesRead:<limit:1 lookbackSeconds:15 forceExceeded:true > maxRecentlyQueriedSeriesDiskRead:<limit:1 lookbackSeconds:15 forceExceeded:true > `,
		},
		{
			name: `all - no commit`,
			limits: &kvpb.QueryLimits{
				MaxRecentlyQueriedSeriesBlocks: &kvpb.QueryLimit{
					Limit:           1,
					LookbackSeconds: 15,
					ForceExceeded:   true,
				},
				MaxRecentlyQueriedSeriesDiskBytesRead: &kvpb.QueryLimit{
					Limit:           1,
					LookbackSeconds: 15,
					ForceExceeded:   true,
				},
				MaxRecentlyQueriedSeriesDiskRead: &kvpb.QueryLimit{
					Limit:           1,
					LookbackSeconds: 15,
					ForceExceeded:   true,
				},
			},
			commit: false,
			// nolint: lll
			expectedJSON: `maxRecentlyQueriedSeriesBlocks:<limit:1 lookbackSeconds:15 forceExceeded:true > maxRecentlyQueriedSeriesDiskBytesRead:<limit:1 lookbackSeconds:15 forceExceeded:true > maxRecentlyQueriedSeriesDiskRead:<limit:1 lookbackSeconds:15 forceExceeded:true > `,
		},
	}

	for _, test := range tests {
		limitJSON, err := json.Marshal(test.limits)
		require.NoError(t, err)

		update := &KeyValueUpdate{
			Key:    kvconfig.QueryLimits,
			Value:  json.RawMessage(limitJSON),
			Commit: test.commit,
		}

		storeMock := kv.NewMockStore(ctrl)

		// (A) test no old value.
		storeMock.EXPECT().Get(kvconfig.QueryLimits).Return(nil, kv.ErrNotFound)
		if test.commit {
			storeMock.EXPECT().Set(kvconfig.QueryLimits, gomock.Any()).Return(0, nil)
		}

		handler := &KeyValueStoreHandler{}
		r, err := handler.update(zap.NewNop(), storeMock, update)
		require.NoError(t, err)
		require.Equal(t, kvconfig.QueryLimits, r.Key)
		require.Equal(t, "", r.Old)
		require.Equal(t, test.expectedJSON, r.New)
		require.Equal(t, 0, r.Version)

		// (B) test old value.
		mockVal := kv.NewMockValue(ctrl)
		storeMock.EXPECT().Get(kvconfig.QueryLimits).Return(mockVal, nil)
		mockVal.EXPECT().Unmarshal(gomock.Any()).DoAndReturn(func(v *kvpb.QueryLimits) error {
			v.MaxRecentlyQueriedSeriesBlocks = &kvpb.QueryLimit{
				Limit:           10,
				LookbackSeconds: 30,
				ForceExceeded:   false,
			}
			v.MaxRecentlyQueriedSeriesDiskBytesRead = &kvpb.QueryLimit{
				Limit:           100,
				LookbackSeconds: 300,
				ForceExceeded:   false,
			}
			return nil
		})
		if test.commit {
			storeMock.EXPECT().Set(kvconfig.QueryLimits, gomock.Any()).Return(0, nil)
		}

		handler = &KeyValueStoreHandler{}
		r, err = handler.update(zap.NewNop(), storeMock, update)
		require.NoError(t, err)
		require.Equal(t, kvconfig.QueryLimits, r.Key)
		// nolint: lll
		require.Equal(t, `maxRecentlyQueriedSeriesBlocks:<limit:10 lookbackSeconds:30 > maxRecentlyQueriedSeriesDiskBytesRead:<limit:100 lookbackSeconds:300 > `, r.Old)
		require.Equal(t, test.expectedJSON, r.New)
		require.Equal(t, 0, r.Version)
	}
}
