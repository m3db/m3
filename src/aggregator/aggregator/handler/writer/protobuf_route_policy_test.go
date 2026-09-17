package writer

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
	"go.uber.org/zap"

	"github.com/m3db/m3/src/metrics/encoding/protobuf"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/msg/producer"
	"github.com/m3db/m3/src/msg/routing"
)

func TestRoutePolicyFilter_Filter_ZeroTrafficTypes(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

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
			rph := routing.NewMockPolicyHandler(ctrl)
			rph.EXPECT().Subscribe(gomock.Any()).Times(1)

			params := RoutingPolicyFilterParams{
				Logger:               zap.NewNop(),
				Scope:                tally.NoopScope,
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
			name: "partial matching - some allowed types missing from config",
			trafficTypes: map[string]uint64{
				"type1": 0, // bit position 0
				"type2": 1, // bit position 1
				// type3 is missing from config
			},
			allowedTypes:        []string{"type1", "type2", "type3"}, // type3 not in traffic types
			messageTrafficTypes: 1,                                   // bit 0 set (type1)
			expected:            true,
			description:         "should pass when message has type1, even though type3 is missing from config",
		},
		{
			name: "partial matching - message with only missing type should fail",
			trafficTypes: map[string]uint64{
				"type1": 0, // bit position 0
				"type2": 1, // bit position 1
				// type3 and type4 are missing
			},
			allowedTypes:        []string{"type1", "type2", "type3"},
			messageTrafficTypes: 4, // bit 2 set (would be type3, but not in config)
			expected:            false,
			description:         "should reject when message has traffic type not in allowed mask",
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

			var subscriberCallback routing.PolicyUpdateListener
			rph.EXPECT().Subscribe(gomock.Any()).DoAndReturn(func(listener routing.PolicyUpdateListener) int {
				subscriberCallback = listener
				return 0
			}).Times(1)

			params := RoutingPolicyFilterParams{
				Logger:               zap.NewNop(),
				Scope:                tally.NoopScope,
				RoutingPolicyHandler: rph,
				IsDefault:            false,
				AllowedTrafficTypes:  tt.allowedTypes,
			}

			filter := NewRoutingPolicyFilter(params, producer.StaticConfig)

			// Invoke the subscription callback with the policy config
			if subscriberCallback != nil {
				policyConfig := routing.NewMockPolicyConfig(ctrl)
				policyConfig.EXPECT().TrafficTypes().Return(tt.trafficTypes).Times(1)
				subscriberCallback(policyConfig)
			}

			msg := createTestMessage(tt.messageTrafficTypes)
			result := filter.Function(msg)
			assert.Equal(t, tt.expected, result, tt.description)
		})
	}
}

func TestRoutePolicyFilter_ZeroMaskBehavior(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tests := []struct {
		name                string
		allowedTypes        []string
		trafficTypes        map[string]uint64
		messageTrafficTypes uint64
		isDefault           bool
		expected            bool
		description         string
	}{
		{
			name:                "zero mask with isDefault true",
			allowedTypes:        []string{"nonexistent"},
			trafficTypes:        map[string]uint64{"type1": 0},
			messageTrafficTypes: 1,
			isDefault:           true,
			expected:            false,
			description:         "should return false when mask is 0 but message has traffic types",
		},
		{
			name:                "zero mask with isDefault false",
			allowedTypes:        []string{"nonexistent"},
			trafficTypes:        map[string]uint64{"type1": 0},
			messageTrafficTypes: 1,
			isDefault:           false,
			expected:            false,
			description:         "should return false when mask is 0 but message has traffic types",
		},
		{
			name:                "zero mask with empty allowed types and isDefault true",
			allowedTypes:        []string{},
			trafficTypes:        map[string]uint64{"type1": 0},
			messageTrafficTypes: 1,
			isDefault:           true,
			expected:            false,
			description:         "should return false when no types allowed but message has traffic types",
		},
		{
			name:                "zero mask with empty allowed types and isDefault false",
			allowedTypes:        []string{},
			trafficTypes:        map[string]uint64{"type1": 0},
			messageTrafficTypes: 1,
			isDefault:           false,
			expected:            false,
			description:         "should return false when no types allowed but message has traffic types",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rph := routing.NewMockPolicyHandler(ctrl)

			var subscriberCallback routing.PolicyUpdateListener
			rph.EXPECT().Subscribe(gomock.Any()).DoAndReturn(func(listener routing.PolicyUpdateListener) int {
				subscriberCallback = listener
				return 0
			}).Times(1)

			params := RoutingPolicyFilterParams{
				Logger:               zap.NewNop(),
				Scope:                tally.NoopScope,
				RoutingPolicyHandler: rph,
				IsDefault:            tt.isDefault,
				AllowedTrafficTypes:  tt.allowedTypes,
			}

			filter := NewRoutingPolicyFilter(params, producer.StaticConfig)

			// Trigger the update to set the mask
			if subscriberCallback != nil {
				policyConfig := routing.NewMockPolicyConfig(ctrl)
				policyConfig.EXPECT().TrafficTypes().Return(tt.trafficTypes).Times(1)
				subscriberCallback(policyConfig)
			}

			msg := createTestMessage(tt.messageTrafficTypes)
			result := filter.Function(msg)
			assert.Equal(t, tt.expected, result, tt.description)
		})
	}
}

func TestRoutePolicyFilter_EdgeCases(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tests := []struct {
		name         string
		description  string
		trafficTypes map[string]uint64
		allowedTypes []string
		messageTypes uint64
		isDefault    bool
		expected     bool
	}{
		{
			name:         "empty allowed traffic types",
			trafficTypes: map[string]uint64{"type1": 0},
			allowedTypes: []string{}, // empty
			isDefault:    false,
			messageTypes: 1, // has traffic types
			expected:     false,
			description:  "should reject when no traffic types are allowed",
		},
		{
			name:         "nil traffic types from handler",
			trafficTypes: nil,
			allowedTypes: []string{"type1"},
			isDefault:    true,
			messageTypes: 1,
			expected:     false,
			description:  "should return false when mask is 0 (due to nil traffic types) but message has traffic types",
		},
		{
			name: "max uint64 traffic types",
			trafficTypes: map[string]uint64{
				"all": 0,
			},
			allowedTypes: []string{"all"},
			isDefault:    false,
			messageTypes: ^uint64(0), // all bits set
			expected:     true,
			description:  "should handle max uint64 value correctly",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rph := routing.NewMockPolicyHandler(ctrl)

			var subscriberCallback routing.PolicyUpdateListener
			rph.EXPECT().Subscribe(gomock.Any()).DoAndReturn(func(listener routing.PolicyUpdateListener) int {
				subscriberCallback = listener
				return 0
			}).Times(1)

			params := RoutingPolicyFilterParams{
				Logger:               zap.NewNop(),
				Scope:                tally.NoopScope,
				RoutingPolicyHandler: rph,
				IsDefault:            tt.isDefault,
				AllowedTrafficTypes:  tt.allowedTypes,
			}

			filter := NewRoutingPolicyFilter(params, producer.StaticConfig)

			// Trigger the update to set the mask
			if subscriberCallback != nil && tt.trafficTypes != nil {
				policyConfig := routing.NewMockPolicyConfig(ctrl)
				policyConfig.EXPECT().TrafficTypes().Return(tt.trafficTypes).Times(1)
				subscriberCallback(policyConfig)
			}

			msg := createTestMessage(tt.messageTypes)

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

func TestRoutePolicyFilter_DynamicPolicyUpdate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rph := routing.NewMockPolicyHandler(ctrl)

	var subscriberCallback routing.PolicyUpdateListener
	rph.EXPECT().Subscribe(gomock.Any()).DoAndReturn(func(listener routing.PolicyUpdateListener) int {
		subscriberCallback = listener
		return 0
	}).Times(1)

	params := RoutingPolicyFilterParams{
		Logger:               zap.NewNop(),
		Scope:                tally.NoopScope,
		RoutingPolicyHandler: rph,
		IsDefault:            false,
		AllowedTrafficTypes:  []string{"type1", "type2"},
	}

	filter := NewRoutingPolicyFilter(params, producer.StaticConfig)

	// Initially, with no policy update, mask should be 0, so isDefault is used
	msg := createTestMessage(1)
	result := filter.Function(msg)
	assert.False(t, result, "should reject when mask is 0 and isDefault is false")

	// Now trigger a policy update with initial traffic types
	policyConfig1 := routing.NewMockPolicyConfig(ctrl)
	policyConfig1.EXPECT().TrafficTypes().Return(map[string]uint64{
		"type1": 0,
		"type2": 1,
	}).Times(1)
	subscriberCallback(policyConfig1)

	// Message with type1 should now pass
	msg = createTestMessage(1) // bit 0 set (type1)
	result = filter.Function(msg)
	assert.True(t, result, "should pass when message has type1")

	// Message with type3 should fail
	msg = createTestMessage(4) // bit 2 set (type3, not allowed)
	result = filter.Function(msg)
	assert.False(t, result, "should reject when message has non-allowed type")

	// Now trigger another policy update with different traffic types
	policyConfig2 := routing.NewMockPolicyConfig(ctrl)
	policyConfig2.EXPECT().TrafficTypes().Return(map[string]uint64{
		"type1": 2, // type1 now at bit position 2
		"type2": 3, // type2 now at bit position 3
	}).Times(1)
	subscriberCallback(policyConfig2)

	// The old message (bit 0) should now fail
	msg = createTestMessage(1) // bit 0 set
	result = filter.Function(msg)
	assert.False(t, result, "should reject after policy update changes bit positions")

	// New message with updated bit positions should pass
	msg = createTestMessage(4) // bit 2 set (type1 at new position)
	result = filter.Function(msg)
	assert.True(t, result, "should pass when message matches new bit position for type1")

	// Trigger a policy update where allowed types don't exist
	policyConfig3 := routing.NewMockPolicyConfig(ctrl)
	policyConfig3.EXPECT().TrafficTypes().Return(map[string]uint64{
		"type3": 0, // Only type3, but we allow type1 and type2
	}).Times(1)
	subscriberCallback(policyConfig3)

	// Mask should now be 0, so isDefault is used
	msg = createTestMessage(1)
	result = filter.Function(msg)
	assert.False(t, result, "should use isDefault when allowed types not found in policy")
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

			var subscriberCallback routing.PolicyUpdateListener
			rph.EXPECT().Subscribe(gomock.Any()).DoAndReturn(func(listener routing.PolicyUpdateListener) int {
				subscriberCallback = listener
				return 0
			}).Times(1)

			params := RoutingPolicyFilterParams{
				Logger:               zap.NewNop(),
				Scope:                tally.NoopScope,
				RoutingPolicyHandler: rph,
				IsDefault:            false,
				AllowedTrafficTypes:  tt.allowedTypes,
			}

			filter := NewRoutingPolicyFilter(params, producer.StaticConfig)

			// Trigger the update to set the mask
			if subscriberCallback != nil {
				policyConfig := routing.NewMockPolicyConfig(ctrl)
				policyConfig.EXPECT().TrafficTypes().Return(tt.trafficTypes).Times(1)
				subscriberCallback(policyConfig)
			}

			msg := createTestMessage(tt.messageTypes)
			result := filter.Function(msg)
			assert.Equal(t, tt.expected, result)
		})
	}
}
