package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"

	m3msgserver "github.com/m3db/m3/src/cmd/services/m3coordinator/server/m3msg"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/msg/consumer"
	"github.com/m3db/m3/src/x/instrument"
	xio "github.com/m3db/m3/src/x/io"
	"github.com/m3db/m3/src/x/pool"
	"github.com/m3db/m3/src/x/server"
)

func main() {
	log.Println("Starting M3 Aggregator Consumer Service...")

	// Listen address - this is where m3aggregator will send aggregated metrics
	listenAddr := "localhost:9000"
	if addr := os.Getenv("LISTEN_ADDR"); addr != "" {
		listenAddr = addr
	}

	// Create instrument options
	instrumentOpts := instrument.NewOptions()
	logger := instrumentOpts.Logger()

	// Create IO options
	ioOpts := xio.NewOptions()

	// Define our write function that will handle incoming metrics
	writeFn := func(
		ctx context.Context,
		id []byte,
		metricNanos, encodeNanos int64,
		value float64,
		annotation []byte,
		sp policy.StoragePolicy,
		callback m3msgserver.Callbackable,
	) {
		timestamp := time.Unix(0, metricNanos)

		logger.Info("received metric",
			zap.ByteString("id", id),
			zap.Float64("value", value),
			zap.Time("timestamp", timestamp),
			zap.String("storage_policy", sp.String()),
			zap.ByteString("annotation", annotation))

		// Here you could forward to other systems, store to DB, etc.
		// For now, we just log the metrics

		// Always call the callback to acknowledge processing
		if callback != nil {
			callback.Callback(m3msgserver.OnSuccess)
		}
	}

	// Create m3msg server configuration using reflection of the internal handlerConfiguration struct
	config := m3msgserver.Configuration{
		Server: server.Configuration{
			ListenAddress: listenAddr,
		},
		Handler: struct {
			ProtobufDecoderPool pool.ObjectPoolConfiguration `yaml:"protobufDecoderPool"`
			BlackholePolicies   []policy.StoragePolicy       `yaml:"blackholePolicies"`
		}{
			ProtobufDecoderPool: pool.ObjectPoolConfiguration{},
			BlackholePolicies:   []policy.StoragePolicy{},
		},
		Consumer: consumer.Configuration{},
	}

	// Create the server
	srv, err := config.NewServer(writeFn, ioOpts, instrumentOpts)
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	log.Printf("Consumer service starting on %s", listenAddr)

	// Start the server
	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}

	// Set up graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Wait for shutdown signal
	<-sigChan
	log.Println("Shutting down consumer service...")

	// Stop the server
	srv.Close()

	log.Println("Consumer service stopped")
}