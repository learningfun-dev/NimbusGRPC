package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/learningfun-dev/NimbusGRPC/nimbus/config"
	"github.com/learningfun-dev/NimbusGRPC/nimbus/kafkaproducer"
	pb "github.com/learningfun-dev/NimbusGRPC/nimbus/proto"
	"github.com/learningfun-dev/NimbusGRPC/nimbus/redisclient"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

type Server struct {
	pb.NimbusServiceServer
	clientStreamManager *ClientStreamManager
	redisSub            *redisclient.RedisSubscriber
	appConfig           *config.Config
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.Println("[INFO] NimbusGRPC server starting...")

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("[FATAL] Failed to load configuration: %v", err)
	}

	// Initialize Kafka Producer
	if err := kafkaproducer.InitProducer(cfg); err != nil {
		log.Fatalf("[FATAL] Failed to initialize Kafka producer: %v", err)
	}
	defer func() {
		log.Println("[INFO] Closing Kafka producer...")
		kafkaproducer.CloseProducer(cfg.ShutdownTimeout)
		log.Println("[INFO] Kafka producer closed.")
	}()

	// Initialize Redis Client
	if err := redisclient.InitClient(cfg); err != nil {
		log.Fatalf("[FATAL] Failed to initialize Redis client: %v", err)
	}
	defer func() {
		log.Println("[INFO] Closing Redis client...")
		if err := redisclient.CloseClient(); err != nil {
			log.Printf("[ERROR] Error closing Redis client: %v", err)
		} else {
			log.Println("[INFO] Redis client closed.")
		}
	}()

	addr := fmt.Sprintf("0.0.0.0:%d", cfg.Port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("[FATAL] Failed to listen on %s: %v", addr, err)
	}
	log.Printf("[INFO] gRPC server listening on %s", addr)

	grpcServer := grpc.NewServer()

	// Initialize ClientStreamManager
	csm := NewClientStreamManager()

	// Get the initialized Redis client
	rdb := redisclient.GetClient()

	// The ClientStreamManager (csm) implements the redisclient.StreamSender interface.
	redisSub := redisclient.NewRedisSubscriber(rdb, cfg.RedisEventsChannel, csm)

	nimbusServer := &Server{
		clientStreamManager: csm,
		redisSub:            redisSub,
		appConfig:           cfg,
	}
	pb.RegisterNimbusServiceServer(grpcServer, nimbusServer)

	healthService := health.NewServer()
	grpc_health_v1.RegisterHealthServer(grpcServer, healthService)
	healthService.SetServingStatus("nimbus.NimbusService", grpc_health_v1.HealthCheckResponse_SERVING)
	healthService.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Println("[INFO] Starting gRPC server...")
		if errSrv := grpcServer.Serve(lis); errSrv != nil {
			log.Printf("[ERROR] gRPC server failed to serve: %v", errSrv)
		}
		log.Println("[INFO] gRPC server stopped.")
	}()

	subscriberCtx, subscriberCancel := context.WithCancel(context.Background())
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Println("[INFO] Starting Redis subscriber...")
		// Start method is now on nimbusServer.redisSub which is *redisclient.RedisSubscriber
		nimbusServer.redisSub.Start(subscriberCtx)
		log.Println("[INFO] Redis subscriber stopped.")
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	receivedSignal := <-sigChan
	log.Printf("[INFO] Received shutdown signal: %v. Initiating graceful shutdown...", receivedSignal)

	healthService.SetServingStatus("nimbus.NimbusService", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	healthService.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)

	subscriberCancel()

	stopped := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(stopped)
	}()

	select {
	case <-stopped:
		log.Println("[INFO] gRPC server gracefully stopped.")
	case <-time.After(cfg.ShutdownTimeout):
		log.Println("[WARN] gRPC server shutdown timed out. Forcing stop...")
		grpcServer.Stop()
	}

	log.Println("[INFO] Waiting for background goroutines to finish...")
	wg.Wait()
	log.Println("[INFO] NimbusGRPC server shut down gracefully.")
}
