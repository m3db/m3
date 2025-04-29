package namespace

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/m3db/m3/src/cluster/client"
)

func TestDynamicOptions_Validate(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := client.NewMockClient(ctrl)
	tests := []struct {
		name              string
		optsFn            func() DynamicOptions
		expectedErr       error
		expectedKey       string
		expectedForceCold bool
	}{
		{
			name: "valid options with defaults",
			optsFn: func() DynamicOptions {
				return NewDynamicOptions().SetConfigServiceClient(mockClient)
			},
			expectedErr:       nil,
			expectedKey:       defaultNsRegistryKey,
			expectedForceCold: false,
		},
		{
			name: "invalid init timeout",
			optsFn: func() DynamicOptions {
				opts := NewDynamicOptions().SetConfigServiceClient(mockClient).(*dynamicOpts)
				opts.initTimeout = 0
				return opts
			},
			expectedErr:       errInitTimeoutPositive,
			expectedKey:       defaultNsRegistryKey,
			expectedForceCold: false,
		},
		{
			name: "empty namespace registry key",
			optsFn: func() DynamicOptions {
				opts := NewDynamicOptions().SetConfigServiceClient(mockClient).(*dynamicOpts)
				opts.nsRegistryKey = ""
				return opts
			},
			expectedErr:       errNsRegistryKeyEmpty,
			expectedKey:       "",
			expectedForceCold: false,
		},
		{
			name:              "nil config service client",
			optsFn:            NewDynamicOptions,
			expectedErr:       errCsClientNotSet,
			expectedKey:       defaultNsRegistryKey,
			expectedForceCold: false,
		},
		{
			name: "custom key and force cold writes enabled",
			optsFn: func() DynamicOptions {
				return NewDynamicOptions().
					SetNamespaceRegistryKey("custom.key").
					SetConfigServiceClient(mockClient).
					SetForceColdWritesEnabled(true)
			},
			expectedErr:       nil,
			expectedKey:       "custom.key",
			expectedForceCold: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := tt.optsFn()
			err := opts.Validate()
			assert.Equal(t, tt.expectedErr, err)

			assert.Equal(t, tt.expectedKey, opts.NamespaceRegistryKey(), "unexpected registry key")
			assert.Equal(t, tt.expectedForceCold, opts.ForceColdWritesEnabled(), "unexpected force cold writes flag")
		})
	}
}
