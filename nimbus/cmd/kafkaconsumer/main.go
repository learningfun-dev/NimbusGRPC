package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/learningfun-dev/NimbusGRPC/nimbus/config"        // Your config package
	"github.com/learningfun-dev/NimbusGRPC/nimbus/kafkaconsumer" // Your new Kafka consumer package
	"github.com/learningfun-dev/NimbusGRPC/nimbus/kafkaproducer" // Your Kafka producer package
	"github.com/learningfun-dev/NimbusGRPC/nimbus/redisclient"   // Your Redis client package
)

func main() {
	err := godotenv.Load() // Tries to load .env from current directory
	if err != nil {
		log.Println("[INFO] No .env file found or error loading .env, using system environment variables or defaults.")
	}
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.Println("[INFO] Kafka Consumer Service starting...")

	// Load application configuration
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("[FATAL] Failed to load configuration: %v", err)
	}

	// Initialize Kafka Producer (needed by EventConsumer)
	// The InitProducer in kafkaproducer.go now takes *config.Config
	if err := kafkaproducer.InitProducer(cfg); err != nil {
		log.Fatalf("[FATAL] Failed to initialize Kafka producer: %v", err)
	}
	defer func() {
		log.Println("[INFO] Main: Closing Kafka producer...")
		kafkaproducer.CloseProducer(cfg.ShutdownTimeout) // Use timeout from config
		log.Println("[INFO] Main: Kafka producer closed.")
	}()

	// Initialize Redis Client (needed by ResultConsumer)
	if err := redisclient.InitClient(cfg); err != nil {
		log.Fatalf("[FATAL] Failed to initialize Redis client: %v", err)
	}
	defer func() {
		log.Println("[INFO] Main: Closing Redis client...")
		if err := redisclient.CloseClient(); err != nil {
			log.Printf("[ERROR] Main: Error closing Redis client: %v", err)
		} else {
			log.Println("[INFO] Main: Redis client closed.")
		}
	}()

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())

	// Create and start EventConsumer
	wg.Add(1)
	eventConsumer, err := kafkaconsumer.NewEventConsumer(cfg, &wg)
	if err != nil {
		log.Fatalf("[FATAL] Failed to create EventConsumer: %v", err)
	}
	go eventConsumer.Start(ctx)
	defer eventConsumer.Shutdown() // Ensure consumer is shutdown

	// Create and start ResultConsumer
	wg.Add(1)
	resultConsumer, err := kafkaconsumer.NewResultConsumer(cfg, &wg)
	if err != nil {
		log.Fatalf("[FATAL] Failed to create ResultConsumer: %v", err)
	}
	go resultConsumer.Start(ctx)
	defer resultConsumer.Shutdown() // Ensure consumer is shutdown

	log.Println("[INFO] Main: EventConsumer and ResultConsumer started. Waiting for signals...")

	// Graceful shutdown handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Wait for shutdown signal
	receivedSignal := <-sigChan
	log.Printf("[INFO] Main: Received shutdown signal: %v. Initiating graceful shutdown...", receivedSignal)

	// Signal consumers to stop by canceling the context
	cancel()

	// Wait for all goroutines (consumers) to finish
	// Add a timeout for waiting to prevent hanging indefinitely
	shutdownComplete := make(chan struct{})
	go func() {
		wg.Wait()
		close(shutdownComplete)
	}()

	select {
	case <-shutdownComplete:
		log.Println("[INFO] Main: All consumer goroutines finished.")
	case <-time.After(cfg.ShutdownTimeout + (5 * time.Second)): // Add a bit more buffer
		log.Println("[WARN] Main: Timeout waiting for consumer goroutines to finish. Some tasks might not have completed.")
	}

	log.Println("[INFO] Main: Kafka Consumer Service shut down gracefully.")
}
