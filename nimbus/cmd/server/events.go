package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"

	"github.com/learningfun-dev/NimbusGRPC/nimbus/kafkaproducer"
	pb "github.com/learningfun-dev/NimbusGRPC/nimbus/proto"
	"github.com/learningfun-dev/NimbusGRPC/nimbus/redisclient"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// ClientStream represents a connected client's stream.
type ClientStream struct {
	Stream   pb.NimbusService_ProcessEventServer
	ClientID string
	Done     chan struct{} // Closed when the stream is finished or removed
}

// ClientStreamManager manages active client streams.
// This struct and its methods will be used by the server and by the redisclient.RedisSubscriber
// (via the redisclient.StreamSender interface that this manager's SendToClient method satisfies).
type ClientStreamManager struct {
	mu      sync.RWMutex
	streams map[string]*ClientStream
}

// NewClientStreamManager creates a new ClientStreamManager.
func NewClientStreamManager() *ClientStreamManager {
	return &ClientStreamManager{
		streams: make(map[string]*ClientStream),
	}
}

// Register adds a client stream to the manager.
func (csm *ClientStreamManager) Register(clientID string, stream pb.NimbusService_ProcessEventServer) *ClientStream {
	csm.mu.Lock()
	defer csm.mu.Unlock()

	cs := &ClientStream{
		Stream:   stream,
		ClientID: clientID,
		Done:     make(chan struct{}),
	}
	csm.streams[clientID] = cs
	log.Printf("[INFO] ClientStreamManager: Registered stream for clientID: %s", clientID)

	return cs
}

// Deregister removes a client stream from the manager.
func (csm *ClientStreamManager) Deregister(clientID string) {
	csm.mu.Lock()
	defer csm.mu.Unlock()

	if cs, ok := csm.streams[clientID]; ok {
		// Check if Done channel is already closed to prevent panic
		select {
		case <-cs.Done:
			// Already closed
		default:
			close(cs.Done) // Signal that this stream is done
		}
		delete(csm.streams, clientID)
		log.Printf("[INFO] ClientStreamManager: Deregistered stream for clientID: %s", clientID)
	} else {
		log.Printf("[WARN] ClientStreamManager: Attempted to deregister non-existent clientID: %s", clientID)
	}
}

// GetStream retrieves a client stream by clientID.
func (csm *ClientStreamManager) GetStream(clientID string) (*ClientStream, bool) {
	csm.mu.RLock()
	defer csm.mu.RUnlock()
	stream, ok := csm.streams[clientID]
	return stream, ok
}

// SendToClient sends a response to a specific client.
// It handles potential errors during sending.
// This method satisfies the redisclient.StreamSender interface.
func (csm *ClientStreamManager) SendToClient(clientID string, resp *pb.EventResponse) error {
	cs, ok := csm.GetStream(clientID)
	if !ok {
		log.Printf("[WARN] ClientStreamManager: Client stream not found for clientID during SendToClient: %s", clientID)
		return fmt.Errorf("client stream not found for clientID: %s", clientID)
	}

	select {
	case <-cs.Done:
		log.Printf("[WARN] ClientStreamManager: Attempted to send to a closed/done stream for clientID: %s", clientID)
		return fmt.Errorf("stream for clientID %s is already closed/done", clientID)
	default:
		// Proceed with send
		if err := cs.Stream.Send(resp); err != nil {
			log.Printf("[ERROR] ClientStreamManager: Failed to send message to clientID %s: %v", clientID, err)
			// Optionally, deregister the stream if send fails consistently,
			// as it might indicate a broken connection. This needs careful consideration
			// to avoid race conditions if Deregister is also called from ProcessEvent's defer.
			// csm.Deregister(clientID)
			return fmt.Errorf("failed to send to client stream for %s: %w", clientID, err)
		}
		log.Printf("[DEBUG] ClientStreamManager: Successfully sent message to clientID %s: EventName=%s", clientID, resp.EventName)
		return nil
	}
}

// ProcessEvent is the gRPC bidirectional streaming method.
// It's a method of the `Server` struct (defined in main.go).
func (s *Server) ProcessEvent(stream pb.NimbusService_ProcessEventServer) error {
	// Extract client_id from metadata
	md, ok := metadata.FromIncomingContext(stream.Context())
	if !ok {
		log.Println("[WARN] ProcessEvent: Failed to get metadata from context.")
		return status.Errorf(codes.DataLoss, "ProcessEvent: failed to get metadata")
	}

	clientIDValues := md.Get("client_id")
	if len(clientIDValues) == 0 || clientIDValues[0] == "" {
		log.Println("[WARN] ProcessEvent: client_id not found or empty in metadata.")
		return status.Errorf(codes.InvalidArgument, "client_id not received or empty in metadata")
	}
	clientID := clientIDValues[0]

	log.Printf("[INFO] ProcessEvent: Connection established for clientID: %s", clientID)

	// Register the stream with the manager
	// s.clientStreamManager is part of the Server struct
	clientStream := s.clientStreamManager.Register(clientID, stream)
	defer s.clientStreamManager.Deregister(clientID) // Ensure stream is deregistered on exit

	// Create redis key-->value for clientId --> Pod_redis_channel
	redisclient.SetKeyValue(stream.Context(), clientID, s.appConfig.RedisEventsChannel)
	defer redisclient.DeleteKey(stream.Context(), clientID)

	// Goroutine to handle context cancellation (client disconnect, server shutdown)
	// This helps in logging or cleanup if the stream context finishes independently.
	go func() {
		select {
		case <-stream.Context().Done():
			log.Printf("[INFO] ProcessEvent: Stream context done for clientID: %s. Error: %v", clientID, stream.Context().Err())
			// Deregistering here might be redundant due to the defer, but can be useful for immediate logging.
			// s.clientStreamManager.Deregister(clientID) // Be cautious about double deregistration
			return
		case <-clientStream.Done:
			log.Printf("[INFO] ProcessEvent: Client stream explicitly marked as done for clientID: %s.", clientID)
			return
		}
	}()

	// Receive messages from client
	for {
		// Check if context is done before trying to receive to allow faster exit
		select {
		case <-stream.Context().Done():
			log.Printf("[INFO] ProcessEvent: Stream context done for clientID %s before Recv(): %v", clientID, stream.Context().Err())
			return stream.Context().Err() // Client disconnected or server shutting down
		default:
			// Proceed to Recv
		}

		req, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				log.Printf("[INFO] ProcessEvent: Client %s closed the stream (EOF).", clientID)
				return nil // Clean disconnect
			}
			// Check for context cancellation error from Recv itself
			if status.Code(err) == codes.Canceled || errors.Is(err, context.Canceled) {
				log.Printf("[INFO] ProcessEvent: Stream canceled for client %s: %v", clientID, err)
				return nil // Context was canceled (e.g. client disconnect or server shutdown)
			}
			log.Printf("[ERROR] ProcessEvent: Error receiving message from client %s: %v", clientID, err)
			return status.Errorf(codes.Internal, "error receiving message: %v", err)
		}

		log.Printf("[INFO] ProcessEvent: Received message from clientID %s: EventName=%s, Number=%d",
			clientID, req.EventName, req.Number)

		// Use the RedisEventsChannel from the application configuration (s.appConfig)
		// s.appConfig is populated in main.go when the Server struct is initialized.
		if s.appConfig == nil {
			log.Printf("[ERROR] ProcessEvent: Application configuration (s.appConfig) is nil for clientID: %s", clientID)
			// This would be a critical setup error.
			// Optionally send an error back to the client.
			_ = stream.Send(&pb.EventResponse{EventName: req.EventName, Number: req.Number /* Add error info */})
			return status.Errorf(codes.Internal, "server configuration error")
		}
		redisChannelForKafka := s.appConfig.RedisEventsChannel
		if redisChannelForKafka == "" {
			log.Printf("[ERROR] ProcessEvent: RedisEventsChannel in appConfig is empty for clientID: %s", clientID)
			// Fallback or error handling if channel config is missing
			_ = stream.Send(&pb.EventResponse{EventName: req.EventName, Number: req.Number /* Add error info */})
			return status.Errorf(codes.Internal, "server configuration error: Redis channel for Kafka not set")
		}

		switch strings.ToLower(req.EventName) {
		case "sq": // Example: Square a number
			kafkaReq := &pb.KafkaEventRequest{
				EventName:    req.EventName,
				Number:       req.Number,
				ClientId:     clientID,
				RedisChannel: redisChannelForKafka,
			}
			// kafkaproducer.SendEventToKafkaTopic uses its internally configured default topic.
			// If the topic needs to be dynamic per request or from main config,
			// SendEventToKafkaTopic would need to accept it as a parameter.
			if err := kafkaproducer.SendEventToDefaultTopic(kafkaReq); err != nil {
				log.Printf("[ERROR] ProcessEvent: Failed to send event to Kafka for clientID %s: %v. Event: %+v",
					clientID, err, kafkaReq)
				// Optionally, send an error response back to the client on this stream
				if errSend := stream.Send(&pb.EventResponse{
					EventName: req.EventName, Number: req.Number, // Result: 0, or add an error field
				}); errSend != nil {
					log.Printf("[ERROR] ProcessEvent: Failed to send Kafka error notification to client %s: %v", clientID, errSend)
				}
			} else {
				log.Printf("[INFO] ProcessEvent: Event sent to Kafka for clientID %s: %+v", clientID, kafkaReq)
			}
		default:
			log.Printf("[WARN] ProcessEvent: Received unknown event name '%s' from clientID %s", req.EventName, clientID)
			// Send an error back to the client for this specific request
			if errSend := stream.Send(&pb.EventResponse{
				EventName: req.EventName,
				Number:    req.Number,
				// Result might be 0 or you could add an error message field to EventResponse
			}); errSend != nil {
				log.Printf("[ERROR] ProcessEvent: Failed to send unknown event error response to clientID %s: %v", clientID, errSend)
				// Depending on severity, you might return an error that closes the stream
				// return status.Errorf(codes.Internal, "failed to send error response for unknown event: %v", errSend)
			}
		}
	}
}
