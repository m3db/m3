package middleware

import (
	"encoding/base64"
	"fmt"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/m3db/m3/src/cluster/generated/proto/commonpb"
	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/dbnode/client/circuitbreaker"
	"go.uber.org/zap"
)

// Config represents the configuration for the circuit breaker middleware.
type Config struct {
	Enabled              bool                  `yaml:"enabled"`
	ShadowMode           bool                  `yaml:"shadowMode"`
	CircuitBreakerConfig circuitbreaker.Config `yaml:"circuitBreakerConfig"`
}

// CircuitBreakerConfigProto is a protobuf message for the complete circuit breaker config.
type CircuitBreakerConfigProto struct {
	Enabled                bool                         `protobuf:"varint,1,opt,name=enabled,proto3" json:"enabled,omitempty"`
	ShadowMode             bool                         `protobuf:"varint,2,opt,name=shadow_mode,json=shadowMode,proto3" json:"shadow_mode,omitempty"`
	CircuitBreakerSettings *CircuitBreakerSettingsProto `protobuf:"bytes,3,opt,name=circuit_breaker_settings,json=circuitBreakerSettings,proto3" json:"circuit_breaker_settings,omitempty"`
}

type CircuitBreakerSettingsProto struct {
	WindowSize           int32     `protobuf:"varint,1,opt,name=window_size,json=windowSize,proto3" json:"window_size,omitempty"`
	BucketDuration       int64     `protobuf:"varint,2,opt,name=bucket_duration,json=bucketDuration,proto3" json:"bucket_duration,omitempty"`
	RecoveryTime         int64     `protobuf:"varint,3,opt,name=recovery_time,json=recoveryTime,proto3" json:"recovery_time,omitempty"`
	Jitter               int64     `protobuf:"varint,4,opt,name=jitter,proto3" json:"jitter,omitempty"`
	MaxProbeTime         int64     `protobuf:"varint,5,opt,name=max_probe_time,json=maxProbeTime,proto3" json:"max_probe_time,omitempty"`
	MinimumRequests      int64     `protobuf:"varint,6,opt,name=minimum_requests,json=minimumRequests,proto3" json:"minimum_requests,omitempty"`
	FailureRatio         float64   `protobuf:"fixed64,7,opt,name=failure_ratio,json=failureRatio,proto3" json:"failure_ratio,omitempty"`
	ProbeRatios          []float64 `protobuf:"fixed64,8,rep,packed,name=probe_ratios,json=probeRatios,proto3" json:"probe_ratios,omitempty"`
	MinimumProbeRequests int64     `protobuf:"varint,9,opt,name=minimum_probe_requests,json=minimumProbeRequests,proto3" json:"minimum_probe_requests,omitempty"`
}

func (m *CircuitBreakerConfigProto) Reset()         { *m = CircuitBreakerConfigProto{} }
func (m *CircuitBreakerConfigProto) String() string { return proto.CompactTextString(m) }
func (*CircuitBreakerConfigProto) ProtoMessage()    {}

func (m *CircuitBreakerSettingsProto) Reset()         { *m = CircuitBreakerSettingsProto{} }
func (m *CircuitBreakerSettingsProto) String() string { return proto.CompactTextString(m) }
func (*CircuitBreakerSettingsProto) ProtoMessage()    {}

// WatchConfig watches for changes to the circuit breaker middleware configuration in etcd.
// It takes a kv store, logger, and a callback function that will be called when the config changes.
// The callback function should handle updating the middleware with the new configuration.
func WatchConfig(
	store kv.Store,
	logger *zap.Logger,
	onConfigChange func(Config) error,
) error {
	// Watch for changes to the circuit breaker middleware configuration

	logger.Info("watching circuit breaker middleware configuration")

	watch, err := store.Watch("circuitbreaker/middleware/config")
	if err != nil {
		return fmt.Errorf("failed to watch circuit breaker middleware configuration: %v", err)
	}

	logger.Info("watch created for circuit breaker middleware configuration")

	go func() {
		for {
			select {
			case <-watch.C():
				// Get the current value
				value, err := store.Get("circuitbreaker/middleware/config")
				if err != nil {
					logger.Error("failed to get circuit breaker middleware configuration", zap.Error(err))
					continue
				}
				logger.Info("circuit breaker middleware configuration changed", zap.Any("value", value))

				// Get raw bytes by unmarshaling into a StringProto
				var stringProto commonpb.StringProto
				if err := value.Unmarshal(&stringProto); err != nil {
					logger.Error("failed to unmarshal value into StringProto", zap.Error(err))
					continue
				}

				// Decode base64 value
				decodedValue, err := base64.StdEncoding.DecodeString(stringProto.Value)
				if err != nil {
					logger.Error("failed to decode base64 value", zap.Error(err))
					continue
				}

				// Unmarshal protobuf message from decoded value
				var configProto CircuitBreakerConfigProto
				if err := proto.Unmarshal(decodedValue, &configProto); err != nil {
					logger.Error("failed to unmarshal circuit breaker middleware configuration", zap.Error(err))
					continue
				}

				// Convert protobuf config to our Config type
				config := Config{
					Enabled:    configProto.Enabled,
					ShadowMode: configProto.ShadowMode,
					CircuitBreakerConfig: circuitbreaker.Config{
						WindowSize:           int(configProto.CircuitBreakerSettings.WindowSize),
						BucketDuration:       time.Duration(configProto.CircuitBreakerSettings.BucketDuration),
						RecoveryTime:         time.Duration(configProto.CircuitBreakerSettings.RecoveryTime),
						Jitter:               time.Duration(configProto.CircuitBreakerSettings.Jitter),
						MaxProbeTime:         time.Duration(configProto.CircuitBreakerSettings.MaxProbeTime),
						MinimumRequests:      configProto.CircuitBreakerSettings.MinimumRequests,
						FailureRatio:         configProto.CircuitBreakerSettings.FailureRatio,
						ProbeRatios:          configProto.CircuitBreakerSettings.ProbeRatios,
						MinimumProbeRequests: configProto.CircuitBreakerSettings.MinimumProbeRequests,
					},
				}

				// Call the callback with the updated config
				if err := onConfigChange(config); err != nil {
					logger.Error("failed to update circuit breaker middleware configuration", zap.Error(err))
				}
			}
		}
	}()
	logger.Info("watching circuit breaker middleware configuration complete")
	return nil
}
