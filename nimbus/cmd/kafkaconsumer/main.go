package main

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/learningfun-dev/NimbusGRPC/nimbus/config"
	"github.com/learningfun-dev/NimbusGRPC/nimbus/kafkaconsumer"
	"github.com/learningfun-dev/NimbusGRPC/nimbus/kafkaproducer"
	"github.com/learningfun-dev/NimbusGRPC/nimbus/logger"
	"github.com/learningfun-dev/NimbusGRPC/nimbus/redisclient"
	"github.com/rs/zerolog/log"
)

func main() {
	logger.Init()

	log.Info().Msg("Kafka Consumer Service starting...")

	// Load application configuration
	cfg, err := config.Load()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to load configuration")
	}

	// Initialize Kafka Producer (needed by EventConsumer)
	if err := kafkaproducer.InitProducer(cfg); err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize Kafka producer")
	}
	defer func() {
		log.Info().Msg("Closing Kafka producer...")
		kafkaproducer.CloseProducer(cfg.ShutdownTimeout)
		log.Info().Msg("Kafka producer closed.")
	}()

	// Initialize Redis Client (needed by ResultConsumer)
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

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())

	// Create and start EventConsumer
	wg.Add(1)
	eventConsumer, err := kafkaconsumer.NewEventConsumer(cfg, &wg)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create EventConsumer")
	}
	go eventConsumer.Start(ctx)
	defer eventConsumer.Shutdown() // Ensure consumer is shutdown

	// Create and start ResultConsumer
	wg.Add(1)
	resultConsumer, err := kafkaconsumer.NewResultConsumer(cfg, &wg)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create ResultConsumer")
	}
	go resultConsumer.Start(ctx)
	defer resultConsumer.Shutdown() // Ensure consumer is shutdown

	log.Info().Msg("EventConsumer and ResultConsumer started. Waiting for signals...")

	// Graceful shutdown handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Wait for shutdown signal
	receivedSignal := <-sigChan
	log.Info().Str("signal", receivedSignal.String()).Msg("Received shutdown signal. Initiating graceful shutdown")

	// Signal consumers to stop by canceling the context
	cancel()

	// Wait for all goroutines (consumers) to finish
	shutdownComplete := make(chan struct{})
	go func() {
		wg.Wait()
		close(shutdownComplete)
	}()

	select {
	case <-shutdownComplete:
		log.Info().Msg("All consumer goroutines finished.")
	case <-time.After(cfg.ShutdownTimeout + (5 * time.Second)): // Add a bit more buffer
		log.Warn().Msg("Timeout waiting for consumer goroutines to finish. Some tasks might not have completed.")
	}

	log.Info().Msg("Kafka Consumer Service shut down gracefully.")
}
