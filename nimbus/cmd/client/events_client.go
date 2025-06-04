package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"

	pb "github.com/learningfun-dev/NimbusGRPC/nimbus/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// sendRequests sends a series of event requests to the server.
func sendRequests(ctx context.Context, stream pb.NimbusService_ProcessEventClient, clientID string, start int, end int) error {
	log.Printf("[INFO] Client %s: Starting to send events from %d to %d.", clientID, start, end)

	for i := start; i <= end; i++ {
		select {
		case <-ctx.Done():
			log.Printf("[INFO] Client %s: Context canceled during sending. Aborting.", clientID)
			return ctx.Err() // Return context error
		default:
			// Proceed with sending
		}

		req := &pb.EventRequest{
			EventName: "sq", // "sq" is the event type
			Number:    int32(i),
		}
		if err := stream.Send(req); err != nil {
			// If Send returns an error, the stream is likely broken.
			log.Printf("[ERROR] Client %s: Failed to send event for number %d: %v", clientID, i, err)
			return fmt.Errorf("failed to send event for number %d: %w", i, err)
		}
		log.Printf("[DEBUG] Client %s: Sent event for number: %d", clientID, i)
		// Optional: Add a small delay if needed for rate limiting or observation
		// time.Sleep(100 * time.Millisecond)
	}

	// After sending all requests, close the send direction of the stream.
	// This signals to the server that the client will not send any more messages.
	if err := stream.CloseSend(); err != nil {
		log.Printf("[ERROR] Client %s: Failed to close send stream: %v", clientID, err)
		return fmt.Errorf("failed to close send stream: %w", err)
	}
	log.Printf("[INFO] Client %s: Finished sending all events and closed send stream.", clientID)
	return nil
}

// receiveResponses receives event responses from the server.
func receiveResponses(ctx context.Context, stream pb.NimbusService_ProcessEventClient, doneReceiving chan<- struct{}) {
	defer close(doneReceiving) // Signal that this goroutine is finished
	log.Printf("[INFO] Client: Starting to receive responses from server.")

	for {
		select {
		case <-ctx.Done():
			log.Printf("[INFO] Client: Context canceled while waiting for responses. Stopping receiver.")
			return // Exit if context is canceled
		default:
			// Proceed to receive
		}

		res, err := stream.Recv()
		if err != nil {
			// io.EOF indicates the server has closed its sending side of the stream,
			// which is a normal termination for the receiving loop.
			if errors.Is(err, io.EOF) {
				log.Println("[INFO] Client: Server has closed the stream (EOF). No more responses.")
				return // Normal end of stream
			}
			// Check for gRPC specific cancellation codes
			st, ok := status.FromError(err)
			if ok && (st.Code() == codes.Canceled || st.Code() == codes.Unavailable) {
				log.Printf("[INFO] Client: Stream canceled or unavailable: %v", err)
				return // Context canceled or connection issue
			}

			log.Printf("[ERROR] Client: Failed to receive response from server: %v", err)
			return // Exit on other errors
		}
		log.Printf("[INFO] Client: Received result from server: EventName=%s, Number=%d, Result=%d",
			res.EventName, res.Number, res.Result)
	}
}

// sendAndReceiveEvents orchestrates the bidirectional streaming.
func sendAndReceiveEvents(ctx context.Context, client pb.NimbusServiceClient, clientID string, start int, end int) error {
	log.Printf("[INFO] Client %s: Initiating ProcessEvent stream.", clientID)

	// Create metadata and add it to the outgoing context for the stream.
	md := metadata.Pairs("client_id", clientID)
	streamCtx := metadata.NewOutgoingContext(ctx, md) // Use the cancellable main context

	// Call the RPC method to get the stream.
	stream, err := client.ProcessEvent(streamCtx)
	if err != nil {
		log.Printf("[ERROR] Client %s: Failed to establish ProcessEvent stream: %v", clientID, err)
		return fmt.Errorf("failed to create ProcessEvent stream for client %s: %w", clientID, err)
	}
	log.Printf("[INFO] Client %s: ProcessEvent stream established.", clientID)

	doneReceiving := make(chan struct{}) // Channel to signal completion of receiving

	// Goroutine to receive responses
	go receiveResponses(streamCtx, stream, doneReceiving) // Pass streamCtx to receiver

	// Send requests in the current goroutine
	// If sendRequests returns an error, we should log it and potentially stop waiting for responses
	// if the error indicates a broken stream.
	sendErr := sendRequests(streamCtx, stream, clientID, start, end) // Pass streamCtx to sender
	if sendErr != nil {
		log.Printf("[ERROR] Client %s: Error during sending requests: %v. May not receive all responses.", clientID, sendErr)
		// Depending on the error, we might not want to wait for doneReceiving indefinitely.
		// If sendErr means the stream is broken, CloseSend might have also failed.
		// The receiveResponses goroutine should eventually exit due to stream error or EOF.
	}

	// Wait for the receiving goroutine to complete or for the main context to be canceled.
	log.Printf("[INFO] Client %s: Waiting for all responses or context cancellation...", clientID)
	select {
	case <-doneReceiving:
		log.Printf("[INFO] Client %s: Finished receiving responses.", clientID)
	case <-ctx.Done():
		log.Printf("[INFO] Client %s: Main context canceled while waiting for responses. %v", clientID, ctx.Err())
		// If the context is canceled, the receiveResponses goroutine should also detect it and exit.
	}

	// If sendRequests encountered an error, return it. Otherwise, return nil.
	// The overall success also depends on whether all expected responses were received,
	// which is harder to track without knowing the exact number of expected responses.
	return sendErr
}
