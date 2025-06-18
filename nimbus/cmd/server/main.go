package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/learningfun-dev/NimbusGRPC/nimbus/config"
	"github.com/learningfun-dev/NimbusGRPC/nimbus/kafkaadmin"
	"github.com/learningfun-dev/NimbusGRPC/nimbus/kafkaproducer"
	"github.com/learningfun-dev/NimbusGRPC/nimbus/logger"
	pb "github.com/learningfun-dev/NimbusGRPC/nimbus/proto"
	"github.com/learningfun-dev/NimbusGRPC/nimbus/redisclient"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

// Server struct and its methods are likely defined across main.go and events.go
// within the same 'main' package.
type Server struct {
	pb.NimbusServiceServer
	clientStreamManager *ClientStreamManager
	redisSub            *redisclient.RedisSubscriber
	appConfig           *config.Config
}

func main() {
	logger.Init()

	log.Info().Msg("NimbusGRPC server starting...")

	cfg, err := config.Load()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to load configuration")
	}

	// Initialize Kafka Producer
	if err := kafkaproducer.InitProducer(cfg); err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize Kafka producer")
	}
	defer func() {
		log.Info().Msg("Closing Kafka producer...")
		kafkaproducer.CloseProducer(cfg.ShutdownTimeout)
		log.Info().Msg("Kafka producer closed.")
	}()

	// Initialize Redis Client
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

	// Initialize Kafka Admin Client
	if err := kafkaadmin.InitAdminClient(cfg); err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize Kafka admin client")
	}
	defer func() {
		log.Info().Msg("Closing Kafka admin client...")
		kafkaadmin.CloseAdminClient()
		log.Info().Msg("Kafka admin client closed.")
	}()

	// Create Kafka topics if they don't exist.
	topics := []string{
		cfg.KafkaEventsTopic,
		cfg.KafkaResultsTopic,
		cfg.KafkaDLQTopic,
	}
	if err := kafkaadmin.CreateTopics(topics); err != nil {
		log.Fatal().Err(err).Msg("Failed to create Kafka Topics")
	}

	addr := fmt.Sprintf("0.0.0.0:%d", cfg.Port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal().Err(err).Str("address", addr).Msg("Failed to listen on address")
	}
	log.Info().Str("address", addr).Msg("gRPC server listening")

	grpcServer := grpc.NewServer()

	// Initialize ClientStreamManager
	csm := NewClientStreamManager()

	// Get the initialized Redis client
	rdb := redisclient.GetClient()

	// The ClientStreamManager (csm) implements the redisclient.StreamSender interface.
	redisSub := redisclient.NewRedisSubscriber(rdb, cfg.RedisResultsChannel, csm)

	nimbusServer := &Server{
		clientStreamManager: csm,
		redisSub:            redisSub,
		appConfig:           cfg,
	}
	pb.RegisterNimbusServiceServer(grpcServer, nimbusServer)

	// Register health check service
	healthService := health.NewServer()
	grpc_health_v1.RegisterHealthServer(grpcServer, healthService)
	healthService.SetServingStatus("nimbus.NimbusService", grpc_health_v1.HealthCheckResponse_SERVING)
	healthService.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING) // Overall server health

	var wg sync.WaitGroup

	// Start gRPC server
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Info().Msg("Starting gRPC server...")
		if errSrv := grpcServer.Serve(lis); errSrv != nil {
			log.Error().Err(errSrv).Msg("gRPC server failed to serve")
		}
		log.Info().Msg("gRPC server stopped.")
	}()

	// Start Redis subscriber
	subscriberCtx, subscriberCancel := context.WithCancel(context.Background())
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Info().Msg("Starting Redis subscriber...")
		// Start method is now on nimbusServer.redisSub which is *redisclient.RedisSubscriber
		nimbusServer.redisSub.Start(subscriberCtx)
		log.Info().Msg("Redis subscriber stopped.")
	}()

	// Wait for shutdown signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	receivedSignal := <-sigChan
	log.Info().Str("signal", receivedSignal.String()).Msg("Shutdown signal received, initiating graceful shutdown")

	healthService.SetServingStatus("nimbus.NimbusService", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	healthService.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)

	subscriberCancel()

	// Handle graceful shutdown
	stopped := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(stopped)
	}()

	select {
	case <-stopped:
		log.Info().Msg("gRPC server gracefully stopped.")
	case <-time.After(cfg.ShutdownTimeout):
		log.Warn().Msg("gRPC server shutdown timed out. Forcing stop...")
		grpcServer.Stop()
	}

	log.Info().Msg("Waiting for background goroutines to finish...")
	wg.Wait()
	log.Info().Msg("NimbusGRPC server shut down gracefully.")
}
