package main

import (
	"context"
	"flag" // For errors.Is
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	pb "github.com/learningfun-dev/NimbusGRPC/nimbus/proto" // Adjust import path
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultServerAddr = "localhost:50051"
	defaultClientID   = "client_default"
	defaultStartNum   = 1
	defaultEndNum     = 10
)

func main() {
	// Setup logging
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.Println("[INFO] Nimbus gRPC Client starting...")

	// Configuration flags
	serverAddr := flag.String("addr", defaultServerAddr, "The server address in the format host:port")
	clientID := flag.String("client_id", defaultClientID, "Client ID to be used for the stream")
	startNum := flag.Int("start", defaultStartNum, "Start of the number range for events")
	endNum := flag.Int("end", defaultEndNum, "End of the number range for events")
	flag.Parse()

	log.Printf("[INFO] Attempting to connect to server at: %s with ClientID: %s", *serverAddr, *clientID)

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
		log.Fatalf("[FATAL] Failed to connect to server: %v", err)
	}
	defer func() {
		log.Println("[INFO] Closing gRPC connection...")
		if err := conn.Close(); err != nil {
			log.Printf("[ERROR] Failed to close gRPC connection: %v", err)
		}
		log.Println("[INFO] gRPC connection closed.")
	}()

	log.Printf("[INFO] Successfully connected to server at: %s", *serverAddr)
	c := pb.NewNimbusServiceClient(conn)

	// Create a context that can be canceled for graceful shutdown
	mainCtx, cancel := context.WithCancel(context.Background())
	defer cancel() // Ensure cancellation on exit to clean up goroutines

	// Handle OS signals for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		log.Printf("[INFO] Received OS signal: %v. Initiating client shutdown...", sig)
		cancel() // Cancel the main context
	}()

	// Call the function to send and receive events
	// Pass the mainCtx which can be used by sendAndReceiveEvents for its operations.
	err = sendAndReceiveEvents(mainCtx, c, *clientID, *startNum, *endNum)
	if err != nil {
		log.Printf("[ERROR] Error during event processing: %v", err)
	}

	log.Println("[INFO] Nimbus gRPC Client finished.")
}
