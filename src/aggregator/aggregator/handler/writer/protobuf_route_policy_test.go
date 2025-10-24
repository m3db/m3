package writer

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/m3db/m3/src/metrics/encoding/protobuf"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/msg/producer"
	"github.com/m3db/m3/src/msg/routing"
)

func TestRoutePolicyFilter_Filter_ZeroTrafficTypes(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rph := routing.NewMockPolicyHandler(ctrl)

	tests := []struct {
		name      string
		isDefault bool
		expected  bool
	}{
		{
			name:      "default true - should pass",
			isDefault: true,
			expected:  true,
		},
		{
			name:      "default false - should reject",
			isDefault: false,
			expected:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := RoutingPolicyFilterParams{
				RoutingPolicyHandler: rph,
				IsDefault:            tt.isDefault,
				AllowedTrafficTypes:  []string{"type1"},
			}

			filter := NewRoutingPolicyFilter(params, producer.StaticConfig)

			// Create message with zero traffic types
			msg := createTestMessage(0)
			result := filter.Function(msg)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestRoutePolicyFilter_Filter_WithTrafficTypes(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tests := []struct {
		name                string
		trafficTypes        map[string]uint64
		allowedTypes        []string
		messageTrafficTypes uint64
		expected            bool
		description         string
	}{
		{
			name: "single matching traffic type",
			trafficTypes: map[string]uint64{
				"type1": 0, // bit position 0
				"type2": 1, // bit position 1
			},
			allowedTypes:        []string{"type1"},
			messageTrafficTypes: 1, // bit 0 set (type1)
			expected:            true,
			description:         "should pass when message has allowed type1",
		},
		{
			name: "multiple allowed types - one matches",
			trafficTypes: map[string]uint64{
				"type1": 0, // bit position 0
				"type2": 1, // bit position 1
				"type3": 2, // bit position 2
			},
			allowedTypes:        []string{"type1", "type3"},
			messageTrafficTypes: 2, // bit 1 set (type2)
			expected:            false,
			description:         "should reject when message has non-allowed type2",
		},
		{
			name: "multiple allowed types - multiple match",
			trafficTypes: map[string]uint64{
				"type1": 0, // bit position 0
				"type2": 1, // bit position 1
				"type3": 2, // bit position 2
			},
			allowedTypes:        []string{"type1", "type2"},
			messageTrafficTypes: 3, // bits 0 and 1 set (type1 and type2)
			expected:            true,
			description:         "should pass when message has at least one allowed type",
		},
		{
			name: "high bit positions",
			trafficTypes: map[string]uint64{
				"type1": 31, // bit position 31
				"type2": 63, // bit position 63
			},
			allowedTypes:        []string{"type2"},
			messageTrafficTypes: 1 << 63, // bit 63 set (type2)
			expected:            true,
			description:         "should handle high bit positions correctly",
		},
		{
			name: "no matching traffic types",
			trafficTypes: map[string]uint64{
				"type1": 0, // bit position 0
				"type2": 1, // bit position 1
			},
			allowedTypes:        []string{"type3"}, // type3 not in traffic types
			messageTrafficTypes: 1,                 // bit 0 set (type1)
			expected:            false,
			description:         "should reject when allowed type doesn't exist in traffic types",
		},
		{
			name: "complex bit pattern",
			trafficTypes: map[string]uint64{
				"read":    0, // bit 0
				"write":   1, // bit 1
				"admin":   2, // bit 2
				"metrics": 3, // bit 3
				"debug":   4, // bit 4
			},
			allowedTypes:        []string{"read", "write"},
			messageTrafficTypes: 0b10110, // bits 1, 2, 4 set (write, admin, debug)
			expected:            true,
			description:         "should pass when message has write permission (bit 1)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rph := routing.NewMockPolicyHandler(ctrl)
			rph.EXPECT().GetTrafficTypes().Return(tt.trafficTypes).AnyTimes()

			params := RoutingPolicyFilterParams{
				RoutingPolicyHandler: rph,
				IsDefault:            false,
				AllowedTrafficTypes:  tt.allowedTypes,
			}

			filter := NewRoutingPolicyFilter(params, producer.StaticConfig)

			msg := createTestMessage(tt.messageTrafficTypes)
			result := filter.Function(msg)
			assert.Equal(t, tt.expected, result, tt.description)
		})
	}
}

func TestRoutePolicyFilter_resolveTrafficTypeToBitPosition(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rph := routing.NewMockPolicyHandler(ctrl)
	trafficTypes := map[string]uint64{
		"type1": 0,
		"type2": 5,
		"type3": 31,
		"type4": 63,
	}
	rph.EXPECT().GetTrafficTypes().Return(trafficTypes).AnyTimes()

	filter := routingPolicyFilter{
		rph:                 rph,
		isDefault:           false,
		allowedTrafficTypes: []string{"type1", "type2", "type3", "type4", "nonexistent"},
	}

	tests := []struct {
		trafficType string
		expected    int
	}{
		{"type1", 0},
		{"type2", 5},
		{"type3", 31},
		{"type4", 63},
		{"nonexistent", -1},
		{"", -1},
	}

	for _, tt := range tests {
		t.Run(tt.trafficType, func(t *testing.T) {
			result := filter.resolveTrafficTypeToBitPosition(tt.trafficType)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestRoutePolicyFilter_EdgeCases(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tests := []struct {
		name         string
		setupFilter  func() producer.FilterFunc
		setupMessage func() producer.Message
		expected     bool
		description  string
	}{
		{
			name: "empty allowed traffic types",
			setupFilter: func() producer.FilterFunc {
				rph := routing.NewMockPolicyHandler(ctrl)
				rph.EXPECT().GetTrafficTypes().Return(map[string]uint64{"type1": 0}).AnyTimes()

				params := RoutingPolicyFilterParams{
					RoutingPolicyHandler: rph,
					IsDefault:            false,
					AllowedTrafficTypes:  []string{}, // empty
				}
				return NewRoutingPolicyFilter(params, producer.StaticConfig)
			},
			setupMessage: func() producer.Message {
				return createTestMessage(1) // has traffic types
			},
			expected:    false,
			description: "should reject when no traffic types are allowed",
		},
		{
			name: "nil traffic types from handler",
			setupFilter: func() producer.FilterFunc {
				rph := routing.NewMockPolicyHandler(ctrl)
				rph.EXPECT().GetTrafficTypes().Return(nil).AnyTimes()

				params := RoutingPolicyFilterParams{
					RoutingPolicyHandler: rph,
					IsDefault:            true,
					AllowedTrafficTypes:  []string{"type1"},
				}
				return NewRoutingPolicyFilter(params, producer.StaticConfig)
			},
			setupMessage: func() producer.Message {
				return createTestMessage(1)
			},
			expected:    false,
			description: "should reject when traffic types map is nil",
		},
		{
			name: "max uint64 traffic types",
			setupFilter: func() producer.FilterFunc {
				rph := routing.NewMockPolicyHandler(ctrl)
				rph.EXPECT().GetTrafficTypes().Return(map[string]uint64{
					"all": 0,
				}).AnyTimes()

				params := RoutingPolicyFilterParams{
					RoutingPolicyHandler: rph,
					IsDefault:            false,
					AllowedTrafficTypes:  []string{"all"},
				}
				return NewRoutingPolicyFilter(params, producer.StaticConfig)
			},
			setupMessage: func() producer.Message {
				return createTestMessage(^uint64(0)) // all bits set
			},
			expected:    true,
			description: "should handle max uint64 value correctly",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter := tt.setupFilter()
			msg := tt.setupMessage()
			result := filter.Function(msg)
			assert.Equal(t, tt.expected, result, tt.description)
		})
	}
}

// Helper function to create test messages
func createTestMessage(trafficTypes uint64) message {
	return message{
		shard: 1,
		sp:    policy.NewStoragePolicy(0, 0, 0),
		rp:    policy.NewRoutingPolicy(trafficTypes),
		data:  protobuf.Buffer{},
	}
}

func TestRoutePolicyFilter_BitMaskingEdgeCases(t *testing.T) {
	tests := []struct {
		name         string
		trafficTypes map[string]uint64
		allowedTypes []string
		messageTypes uint64
		expected     bool
	}{
		{
			name: "bit position 0 - edge case",
			trafficTypes: map[string]uint64{
				"zero": 0,
			},
			allowedTypes: []string{"zero"},
			messageTypes: 1, // 2^0 = 1
			expected:     true,
		},
		{
			name: "bit position 63 - max position",
			trafficTypes: map[string]uint64{
				"max": 63,
			},
			allowedTypes: []string{"max"},
			messageTypes: 1 << 63, // 2^63
			expected:     true,
		},
		{
			name: "multiple bits with gaps",
			trafficTypes: map[string]uint64{
				"bit0":  0,
				"bit5":  5,
				"bit10": 10,
				"bit20": 20,
			},
			allowedTypes: []string{"bit5", "bit20"},
			messageTypes: (1 << 5) | (1 << 10) | (1 << 20), // bits 5, 10, 20 set
			expected:     true,                             // matches bit5 and bit20
		},
		{
			name: "no bits match despite having traffic types",
			trafficTypes: map[string]uint64{
				"bit1": 1,
				"bit3": 3,
			},
			allowedTypes: []string{"bit1"},
			messageTypes: (1 << 2) | (1 << 4), // bits 2, 4 set (not bit 1)
			expected:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			rph := routing.NewMockPolicyHandler(ctrl)
			rph.EXPECT().GetTrafficTypes().Return(tt.trafficTypes).AnyTimes()

			params := RoutingPolicyFilterParams{
				RoutingPolicyHandler: rph,
				IsDefault:            false,
				AllowedTrafficTypes:  tt.allowedTypes,
			}

			filter := NewRoutingPolicyFilter(params, producer.StaticConfig)
			msg := createTestMessage(tt.messageTypes)
			result := filter.Function(msg)
			assert.Equal(t, tt.expected, result)
		})
	}
}
