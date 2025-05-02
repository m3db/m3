package middleware

import (
	"encoding/base64"
	"fmt"

	"github.com/gogo/protobuf/proto"
	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/dbnode/client/circuitbreaker"
	"go.uber.org/zap"
	yaml "gopkg.in/yaml.v2"
)

// Config represents the configuration for the circuit breaker middleware.
type Config struct {
	Enabled              bool                  `yaml:"enabled"`
	ShadowMode           bool                  `yaml:"shadowMode"`
	CircuitBreakerConfig circuitbreaker.Config `yaml:"circuitBreakerConfig"`
}

// EtcdConfig represents the configuration stored in etcd.
type EtcdConfig struct {
	Enabled    bool `yaml:"enabled"`
	ShadowMode bool `yaml:"shadowMode"`
}

// EtcdConfigProto is a protobuf message for the etcd config.
type EtcdConfigProto struct {
	Enabled    bool   `protobuf:"varint,1,opt,name=enabled,proto3" json:"enabled,omitempty"`
	ShadowMode bool   `protobuf:"varint,2,opt,name=shadow_mode,json=shadowMode,proto3" json:"shadow_mode,omitempty"`
	Value      string `protobuf:"bytes,3,opt,name=value,proto3" json:"value,omitempty"`
}

func (m *EtcdConfigProto) Reset()         { *m = EtcdConfigProto{} }
func (m *EtcdConfigProto) String() string { return proto.CompactTextString(m) }
func (*EtcdConfigProto) ProtoMessage()    {}

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

				// Unmarshal into EtcdConfigProto
				var configProto EtcdConfigProto
				if err := value.Unmarshal(&configProto); err != nil {
					logger.Error("failed to unmarshal circuit breaker middleware configuration", zap.Error(err))
					continue
				}

				// Decode base64 string
				decoded, err := base64.StdEncoding.DecodeString(configProto.Value)
				if err != nil {
					logger.Error("failed to decode base64 config", zap.Error(err))
					continue
				}

				// Parse YAML into EtcdConfig
				var etcdConfig EtcdConfig
				if err := yaml.Unmarshal(decoded, &etcdConfig); err != nil {
					logger.Error("failed to parse YAML config", zap.Error(err))
					continue
				}

				// Create a new config with the boolean flags from etcd
				config := Config{
					Enabled:    etcdConfig.Enabled,
					ShadowMode: etcdConfig.ShadowMode,
				}

				// Call the callback with the new config
				if err := onConfigChange(config); err != nil {
					logger.Error("failed to update circuit breaker middleware configuration", zap.Error(err))
				}
			}
		}
	}()
	logger.Info("watching circuit breaker middleware configuration complete")
	return nil
}
