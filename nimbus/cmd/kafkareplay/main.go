package main

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/learningfun-dev/NimbusGRPC/nimbus/config"
	"github.com/learningfun-dev/NimbusGRPC/nimbus/kafkaadmin"
	"github.com/learningfun-dev/NimbusGRPC/nimbus/kafkaconsumer"
	"github.com/learningfun-dev/NimbusGRPC/nimbus/kafkaproducer"
	"github.com/learningfun-dev/NimbusGRPC/nimbus/logger"
	"github.com/learningfun-dev/NimbusGRPC/nimbus/redisclient"
	"github.com/rs/zerolog/log"
)

func main() {
	logger.Init()

	log.Info().Msg("Kafka Replay Service starting...")

	// Load application configuration.
	cfg, err := config.Load()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to load configuration")
	}

	// Initialize Redis Client (needed by ReplayConsumer and StatusManager).
	if err := redisclient.InitClient(cfg); err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize Redis client")
	}
	defer func() {
		log.Info().Msg("Closing Redis client...")
		if err := redisclient.CloseClient(); err != nil {
			log.Error().Err(err).Msg("Error closing Redis client")
		} else {
			log.Info().Msg("Redis client closed.")
		}
	}()

	// Initialize Kafka Admin Client (needed by StatusManager).
	if err := kafkaadmin.InitAdminClient(cfg); err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize Kafka admin client")
	}
	defer func() {
		log.Info().Msg("Closing Kafka admin client...")
		kafkaadmin.CloseAdminClient()
		log.Info().Msg("Kafka admin client closed.")
	}()

	// Initialize Kafka Producer (needed by StatusManager for offset queries).
	if err := kafkaproducer.InitProducer(cfg); err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize Kafka producer")
	}
	defer func() {
		log.Info().Msg("Closing Kafka producer...")
		kafkaproducer.CloseProducer(cfg.ShutdownTimeout)
		log.Info().Msg("Kafka producer closed.")
	}()

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())

	// Create and start ReplayConsumer.
	wg.Add(1)
	replayConsumer, err := kafkaconsumer.NewReplayConsumer(cfg, &wg)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create ReplayConsumer")
	}
	go replayConsumer.Start(ctx)
	defer replayConsumer.Shutdown()

	// Create and start StatusManager.
	wg.Add(1)
	statusManager, err := kafkaconsumer.NewStatusManager(cfg, &wg)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create StatusManager")
	}
	go statusManager.Start(ctx)
	defer statusManager.Shutdown()

	log.Info().Msg("ReplayConsumer and StatusManager started. Waiting for signals...")

	// Graceful shutdown handling.
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Wait for shutdown signal.
	receivedSignal := <-sigChan
	log.Info().Str("signal", receivedSignal.String()).Msg("Received shutdown signal. Initiating graceful shutdown")

	// Signal consumers to stop by canceling the context.
	cancel()

	// Wait for all goroutines to finish.
	shutdownComplete := make(chan struct{})
	go func() {
		wg.Wait()
		close(shutdownComplete)
	}()

	select {
	case <-shutdownComplete:
		log.Info().Msg("All replay services finished.")
	case <-time.After(cfg.ShutdownTimeout + (5 * time.Second)):
		log.Warn().Msg("Timeout waiting for replay services to finish. Some tasks might not have completed.")
	}

	log.Info().Msg("Kafka Replay Service shut down gracefully.")
}
