package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"

	pb "github.com/learningfun-dev/NimbusGRPC/nimbus/proto"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// sendRequests sends a series of event requests to the server.
func sendRequests(ctx context.Context, stream pb.NimbusService_ProcessEventClient, clientID string, start int, end int) error {
	log.Info().Str("clientID", clientID).Msgf("Starting to send events from %d to %d.", start, end)

	if start == end && end == 0 {
		log.Info().Str("clientID", clientID).Msg("Not sending any events, standing by for results from server.")
		return nil
	}

	for i := start; i <= end; i++ {
		select {
		case <-ctx.Done():
			log.Info().Str("clientID", clientID).Msg("Context canceled during sending. Aborting.")
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
			log.Error().Err(err).Str("clientID", clientID).Int("number", i).Msg("Failed to send event")
			return fmt.Errorf("failed to send event for number %d: %w", i, err)
		}
		log.Debug().Str("clientID", clientID).Int("number", i).Msg("Sent event")
		// Optional: Add a small delay if needed for rate limiting or observation
		time.Sleep(100 * time.Millisecond)
	}

	// // After sending all requests, close the send direction of the stream.
	// // This signals to the server that the client will not send any more messages.
	// if err := stream.CloseSend(); err != nil {
	// 	log.Error().Err(err).Str("clientID", clientID).Msg("Failed to close send stream")
	// 	return fmt.Errorf("failed to close send stream: %w", err)
	// }
	log.Info().Str("clientID", clientID).Msg("Finished sending all events.")
	return nil
}

// receiveResponses receives event responses from the server.
func receiveResponses(ctx context.Context, clientID string, stream pb.NimbusService_ProcessEventClient, doneReceiving chan<- struct{}) {
	defer close(doneReceiving) // Signal that this goroutine is finished
	log.Info().Str("clientID", clientID).Msg("Starting to receive responses from server.")

	for {
		select {
		case <-ctx.Done():
			log.Info().Str("clientID", clientID).Msg("Context canceled while waiting for responses. Stopping receiver.")
			return // Exit if context is canceled
		default:
			// Proceed to receive
		}

		res, err := stream.Recv()
		if err != nil {
			// io.EOF indicates the server has closed its sending side of the stream,
			// which is a normal termination for the receiving loop.
			if errors.Is(err, io.EOF) {
				log.Info().Str("clientID", clientID).Msg("Server has closed the stream (EOF). No more responses.")
				return // Normal end of stream
			}
			// Check for gRPC specific cancellation codes
			st, ok := status.FromError(err)
			if ok && (st.Code() == codes.Canceled || st.Code() == codes.Unavailable) {
				log.Info().Err(err).Str("clientID", clientID).Msg("Stream canceled or unavailable")
				return // Context canceled or connection issue
			}

			log.Error().Err(err).Str("clientID", clientID).Msg("Failed to receive response from server")
			return // Exit on other errors
		}

		log.Info().
			Str("clientID", clientID).
			Str("eventName", res.EventName).
			Int32("number", res.Number).
			Int32("result", res.Result).
			Msg("Received result from server")

		// DEBUG: log the message recived as file
		writeResultToFile(clientID, res)
	}
}

func writeResultToFile(clientId string, resp *pb.EventResponse) {
	dirPath := filepath.Join("./vol-data/", clientId)
	// Using os.MkdirAll is like 'mkdir -p', it creates parent directories if they don't exist.
	// Use proper directory permissions (0755 is common).
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		log.Error().Err(err).Str("path", dirPath).Msg("Error creating directory path")
		return // Exit if we can't create the directory.
	}

	// Convert the integer number to a string correctly for the filename.
	fileName := strconv.Itoa(int(resp.Number)) + ".json"
	fullPath := filepath.Join(dirPath, fileName)

	// Marshal the entire response object into JSON for structured data storage.
	fileContent, err := json.MarshalIndent(resp, "", "  ") // Using MarshalIndent for readable JSON
	if err != nil {
		log.Error().Err(err).Msg("Error marshaling response to JSON")
		return
	}

	// Write the content to the file.
	err = os.WriteFile(fullPath, fileContent, 0644)
	if err != nil {
		log.Error().Err(err).Str("path", fullPath).Msg("Error writing to file")
	} else {
		log.Debug().Str("path", fullPath).Msg("Successfully wrote result to file")
	}
}

// sendAndReceiveEvents orchestrates the bidirectional streaming.
func sendAndReceiveEvents(ctx context.Context, client pb.NimbusServiceClient, clientID string, start int, end int) error {
	log.Info().Str("clientID", clientID).Msg("Initiating ProcessEvent stream.")

	// Create metadata and add it to the outgoing context for the stream.
	md := metadata.Pairs("client_id", clientID)
	streamCtx := metadata.NewOutgoingContext(ctx, md) // Use the cancellable main context

	// Call the RPC method to get the stream.
	stream, err := client.ProcessEvent(streamCtx)
	if err != nil {
		log.Error().Err(err).Str("clientID", clientID).Msg("Failed to establish ProcessEvent stream")
		return fmt.Errorf("failed to create ProcessEvent stream for client %s: %w", clientID, err)
	}
	log.Info().Str("clientID", clientID).Msg("ProcessEvent stream established.")

	doneReceiving := make(chan struct{}) // Channel to signal completion of receiving

	// Goroutine to receive responses
	go receiveResponses(streamCtx, clientID, stream, doneReceiving) // Pass streamCtx to receiver

	// Send requests in the current goroutine
	// If sendRequests returns an error, we should log it and potentially stop waiting for responses
	// if the error indicates a broken stream.
	sendErr := sendRequests(streamCtx, stream, clientID, start, end) // Pass streamCtx to sender
	if sendErr != nil {
		log.Error().Err(sendErr).Str("clientID", clientID).Msg("Error during sending requests. May not receive all responses.")
	}

	// Wait for the receiving goroutine to complete or for the main context to be canceled.
	log.Info().Str("clientID", clientID).Msg("Waiting for all responses or context cancellation...")
	select {
	case <-doneReceiving:
		log.Info().Str("clientID", clientID).Msg("Finished receiving responses.")
	case <-ctx.Done():
		log.Info().Str("clientID", clientID).Err(ctx.Err()).Msg("Main context canceled while waiting for responses.")
	}

	// If sendRequests encountered an error, return it. Otherwise, return nil.
	// The overall success also depends on whether all expected responses were received,
	// which is harder to track without knowing the exact number of expected responses.
	return sendErr
}
