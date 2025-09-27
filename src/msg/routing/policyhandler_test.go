package routing

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/kv/mem"
	routingpolicypb "github.com/m3db/m3/src/msg/generated/proto/routingpolicypb"
)

// Test helper to setup a policy handler with common test configuration
func setupTestHandler(ctrl *gomock.Controller, staticTypes map[string]uint64) (PolicyHandler, kv.Store) {
	return setupTestHandlerWithKey(ctrl, "default-key", staticTypes)
}

// Test helper to setup a policy handler with a specific key for watch tests
func setupTestHandlerWithKey(ctrl *gomock.Controller, key string, staticTypes map[string]uint64) (PolicyHandler, kv.Store) {
	kvClient := client.NewMockClient(ctrl)
	store := mem.NewStore()
	kvOpts := kv.NewOverrideOptions().SetZone("test-zone").SetEnvironment("test-env").SetNamespace("test-ns")
	kvClient.EXPECT().Store(kvOpts).Return(store, nil)

    // Pre-populate the store so the watch can initialize successfully during handler construction
    initialPolicy := &routingpolicypb.RoutingPolicyConfig{
        TrafficTypes: staticTypes,
    }
    _, err := store.Set(key, initialPolicy)
    if err != nil {
        panic(err)
    }

	opts := NewPolicyHandlerOptions().
		WithKVClient(kvClient).
		WithKVOverrideOptions(kvOpts).
        WithKVKey(key).
		WithPolicyConfig(NewPolicyConfig(staticTypes))

	handler, err := NewRoutingPolicyHandler(opts)
	if err != nil {
		panic(err) // This should not happen in tests with valid setup
	}

	return handler, store
}

func TestRoutingPolicyHandler_NewRoutingPolicyHandler(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tests := []struct {
		name        string
		setupOpts   func() PolicyHandlerOptions
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid options",
			setupOpts: func() PolicyHandlerOptions {
				kvClient := client.NewMockClient(ctrl)
				store := mem.NewStore()
				kvOpts := kv.NewOverrideOptions().SetZone("test-zone").SetEnvironment("test-env").SetNamespace("test-ns")
				kvClient.EXPECT().Store(kvOpts).Return(store, nil)

                // Seed the store so constructor watch can read the initial value
                _, _ = store.Set("test-key", &routingpolicypb.RoutingPolicyConfig{TrafficTypes: map[string]uint64{"static": 1}})

				return NewPolicyHandlerOptions().
					WithKVClient(kvClient).
					WithKVOverrideOptions(kvOpts).
                    WithKVKey("test-key").
					WithPolicyConfig(NewPolicyConfig(map[string]uint64{"static": 1}))
			},
			expectError: false,
		},
		{
			name: "missing kv client",
			setupOpts: func() PolicyHandlerOptions {
				return NewPolicyHandlerOptions().
					WithKVOverrideOptions(kv.NewOverrideOptions()).
                    WithKVKey("test-key").
					WithPolicyConfig(NewPolicyConfig(map[string]uint64{"static": 1}))
			},
			expectError: true,
            errorMsg:    "kvClient is required if kvKey is set",
		},
        {
            name: "no kv key provided (no watch)",
            setupOpts: func() PolicyHandlerOptions {
                return NewPolicyHandlerOptions().
                    WithPolicyConfig(NewPolicyConfig(map[string]uint64{"static": 1}))
            },
            expectError: false,
        },
		{
			name: "missing static traffic types",
			setupOpts: func() PolicyHandlerOptions {
				kvClient := client.NewMockClient(ctrl)
				return NewPolicyHandlerOptions().
					WithKVClient(kvClient).
					WithKVOverrideOptions(kv.NewOverrideOptions().SetZone("test-zone").SetEnvironment("test-env").SetNamespace("test-ns")).
                    WithKVKey("test-key")
			},
			expectError: true,
			errorMsg:    "policyConfig is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := tt.setupOpts()
			handler, err := NewRoutingPolicyHandler(opts)

			if tt.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
				assert.Nil(t, handler)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, handler)

				// Verify static traffic types are set
				trafficTypes := handler.GetTrafficTypes()
				expected := map[string]uint64{"static": 1}
				assert.Equal(t, expected, trafficTypes)
			}
		})
	}
}

func TestRoutingPolicyHandler_GetTrafficTypes(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	staticTrafficTypes := map[string]uint64{
		"static1": 1,
		"static2": 2,
	}

	handler, _ := setupTestHandler(ctrl, staticTrafficTypes)

	// Should return static traffic types initially
	trafficTypes := handler.GetTrafficTypes()
	assert.Equal(t, staticTrafficTypes, trafficTypes)
}

func TestRoutingPolicyHandler_WatchAndUpdate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	key := "routing-policy-test"
	staticTrafficTypes := map[string]uint64{
		"static1": 1,
		"static2": 2,
	}

	handler, store := setupTestHandlerWithKey(ctrl, key, staticTrafficTypes)

    // Pre-populate again to trigger the initial update (helper already seeded before construction)
    initialPolicy := &routingpolicypb.RoutingPolicyConfig{
        TrafficTypes: staticTrafficTypes,
    }
    _, err := store.Set(key, initialPolicy)
    require.NoError(t, err)
	defer handler.Close()

	// Verify initial static traffic types
	trafficTypes := handler.GetTrafficTypes()
	assert.Equal(t, staticTrafficTypes, trafficTypes)

	// Create dynamic traffic types policy
	dynamicTrafficTypes := map[string]uint64{
		"dynamic1": 10,
		"dynamic2": 20,
		"dynamic3": 30,
	}

	policy := &routingpolicypb.RoutingPolicyConfig{
		TrafficTypes: dynamicTrafficTypes,
	}

	// Set the policy in the store to trigger watch update
	_, err = store.Set(key, policy)
	require.NoError(t, err)

	// Wait for the watch to process the update
	assert.Eventually(t, func() bool {
		trafficTypes := handler.GetTrafficTypes()
		return len(trafficTypes) == len(dynamicTrafficTypes) &&
			trafficTypes["dynamic1"] == 10 &&
			trafficTypes["dynamic2"] == 20 &&
			trafficTypes["dynamic3"] == 30
	}, 2*time.Second, 50*time.Millisecond, "traffic types should be updated from dynamic config")

	// Update the policy again
	updatedTrafficTypes := map[string]uint64{
		"updated1": 100,
		"updated2": 200,
	}

	updatedPolicy := &routingpolicypb.RoutingPolicyConfig{
		TrafficTypes: updatedTrafficTypes,
	}

	_, err = store.Set(key, updatedPolicy)
	require.NoError(t, err)

	// Wait for the second update
	assert.Eventually(t, func() bool {
		trafficTypes := handler.GetTrafficTypes()
		return len(trafficTypes) == len(updatedTrafficTypes) &&
			trafficTypes["updated1"] == 100 &&
			trafficTypes["updated2"] == 200
	}, 2*time.Second, 50*time.Millisecond, "traffic types should be updated again")
}

func TestRoutingPolicyHandler_Close(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	key := "routing-policy-close-test"
	staticTypes := map[string]uint64{"static": 1}
	handler, store := setupTestHandlerWithKey(ctrl, key, staticTypes)

	// Pre-populate the store to avoid watch timeout
	initialPolicy := &routingpolicypb.RoutingPolicyConfig{
		TrafficTypes: staticTypes,
	}
	_, err := store.Set(key, initialPolicy)
	require.NoError(t, err)

	handlerImpl := handler.(*routingPolicyHandler)

	// Close should not panic and should be idempotent
	handler.Close()
	handler.Close() // Second close should be safe

	assert.False(t, handlerImpl.isWatchingValue)
}

func TestRoutingPolicyHandler_ConcurrentAccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	key := "routing-policy-concurrent-test"
	staticTrafficTypes := map[string]uint64{
		"static1": 1,
	}

	handler, store := setupTestHandlerWithKey(ctrl, key, staticTrafficTypes)

	// Pre-populate the store to avoid watch timeout
	initialPolicy := &routingpolicypb.RoutingPolicyConfig{
		TrafficTypes: staticTrafficTypes,
	}
	_, err := store.Set(key, initialPolicy)
	require.NoError(t, err)

	defer handler.Close()

	// Test concurrent reads and writes
	done := make(chan bool)

	// Start multiple goroutines reading traffic types
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				trafficTypes := handler.GetTrafficTypes()
				assert.NotNil(t, trafficTypes)
			}
			done <- true
		}()
	}

	// Update the policy multiple times while reads are happening
	for i := 0; i < 5; i++ {
		policy := &routingpolicypb.RoutingPolicyConfig{
			TrafficTypes: map[string]uint64{
				"concurrent": uint64(i + 10),
			},
		}
		_, err = store.Set(key, policy)
		require.NoError(t, err)
		time.Sleep(10 * time.Millisecond)
	}

	// Wait for all readers to complete
	for i := 0; i < 10; i++ {
		<-done
	}
}

func TestNewPolicyFromValue(t *testing.T) {
	tests := []struct {
		name           string
		setupValue     func() kv.Value
		expectedPolicy map[string]uint64
		expectError    bool
	}{
		{
			name: "valid policy value",
			setupValue: func() kv.Value {
				policy := &routingpolicypb.RoutingPolicyConfig{
					TrafficTypes: map[string]uint64{
						"type1": 1,
						"type2": 2,
					},
				}
				value := mem.NewValue(1, policy)
				return value
			},
			expectedPolicy: map[string]uint64{
				"type1": 1,
				"type2": 2,
			},
			expectError: false,
		},
		{
			name: "empty traffic types",
			setupValue: func() kv.Value {
				policy := &routingpolicypb.RoutingPolicyConfig{
					TrafficTypes: map[string]uint64{},
				}
				value := mem.NewValue(1, policy)
				return value
			},
			expectedPolicy: map[string]uint64{},
			expectError:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value := tt.setupValue()
			policy, err := NewPolicyConfigFromValue(value)

			if tt.expectError {
				require.Error(t, err)
				assert.Nil(t, policy)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, policy)
				assert.Equal(t, tt.expectedPolicy, policy.TrafficTypes())
			}
		})
	}
}

func TestPolicyHandlerOptions(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	kvClient := client.NewMockClient(ctrl)
	kvOpts := kv.NewOverrideOptions().SetZone("test-zone").SetEnvironment("test-env").SetNamespace("test-ns")
	staticTypes := map[string]uint64{"test": 1}
	key := "test-key"

    opts := NewPolicyHandlerOptions().
        WithKVClient(kvClient).
        WithKVOverrideOptions(kvOpts).
        WithPolicyConfig(NewPolicyConfig(staticTypes)).
        WithKVKey(key)

	assert.Equal(t, kvClient, opts.KVClient())
	assert.Equal(t, kvOpts, opts.KVOverrideOptions())
	assert.Equal(t, staticTypes, opts.PolicyConfig().TrafficTypes())
    assert.Equal(t, key, opts.KVKey())

	// Test validation
	err := opts.Validate()
	assert.NoError(t, err)
}
