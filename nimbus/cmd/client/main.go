package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/learningfun-dev/NimbusGRPC/nimbus/logger"
	pb "github.com/learningfun-dev/NimbusGRPC/nimbus/proto" // Adjust import path
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc" // For gRPC status codes
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultServerAddr = "localhost:50051"
	defaultClientID   = "client_default"
	defaultStartNum   = 1
	defaultEndNum     = 10
)

func main() {

	logger.Init()

	// Setup logging
	log.Info().Msg("Nimbus gRPC Client starting...")

	// Configuration flags
	serverAddr := flag.String("addr", defaultServerAddr, "The server address in the format host:port")
	clientID := flag.String("client_id", defaultClientID, "Client ID to be used for the stream")
	startNum := flag.Int("start", defaultStartNum, "Start of the number range for events")
	endNum := flag.Int("end", defaultEndNum, "End of the number range for events")
	flag.Parse()

	log.Info().
		Str("address", *serverAddr).
		Str("clientID", *clientID).
		Msg("Attempting to connect to server")

	// Set up a connection to the server.
	// Adding grpc.WithBlock() to make the connection attempt synchronous for a short period.
	// For production, consider more advanced connection strategies (e.g., retry with backoff).
	_, connCancel := context.WithTimeout(context.Background(), 10*time.Second) // Timeout for connection
	defer connCancel()

	conn, err := grpc.NewClient(*serverAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(), // Block until connection is up or context times out
	)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to connect to server")
	}
	defer func() {
		log.Info().Msg("Closing gRPC connection...")
		if err := conn.Close(); err != nil {
			log.Error().Err(err).Msg("Failed to close gRPC connection")
		}
		log.Info().Msg("gRPC connection closed.")
	}()

	log.Info().Str("address", *serverAddr).Msg("Successfully connected to server")
	c := pb.NewNimbusServiceClient(conn)

	// Create a context that can be canceled for graceful shutdown
	mainCtx, cancel := context.WithCancel(context.Background())
	defer cancel() // Ensure cancellation on exit to clean up goroutines

	// Handle OS signals for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		log.Info().Str("signal", sig.String()).Msg("Received OS signal, initiating client shutdown")
		cancel() // Cancel the main context
	}()

	// Call the function to send and receive events
	// Pass the mainCtx which can be used by sendAndReceiveEvents for its operations.
	err = sendAndReceiveEvents(mainCtx, c, *clientID, *startNum, *endNum)
	if err != nil {
		log.Error().Err(err).Msg("Error during event processing")
	}

	log.Info().Msg("Nimbus gRPC Client finished.")
}
