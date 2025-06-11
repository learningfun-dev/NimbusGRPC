package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"

	"github.com/learningfun-dev/NimbusGRPC/nimbus/kafkaadmin"
	"github.com/learningfun-dev/NimbusGRPC/nimbus/kafkaproducer"
	pb "github.com/learningfun-dev/NimbusGRPC/nimbus/proto"
	"github.com/learningfun-dev/NimbusGRPC/nimbus/redisclient"
	"github.com/redis/go-redis/v9"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	// Redis Key Suffixes for State Management
	redisStatusKeySuffix          = "-status"
	redisLocationKeySuffix        = "-location"
	redisTargetOffsetKeySuffix    = "-replay-target-offset"
	redisLastAckedOffsetKeySuffix = "-last-acked-offset"

	// Redis Status Values
	REDIS_CLIENT_STATUS_LIVE    = "LIVE"
	REDIS_CLIENT_STATUS_REPLAY  = "REPLAYING"
	REDIS_CLIENT_STATUS_OFFLINE = "OFFLINE"
)

// ClientStream represents a connected client's stream.
type ClientStream struct {
	Stream   pb.NimbusService_ProcessEventServer
	ClientID string
	Done     chan struct{}
}

// ClientStreamManager manages active client streams.
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
func (csm *ClientStreamManager) Register(redisChannel string, clientID string, stream pb.NimbusService_ProcessEventServer) *ClientStream {
	csm.mu.Lock()
	defer csm.mu.Unlock()
	cs := &ClientStream{
		Stream:   stream,
		ClientID: clientID,
		Done:     make(chan struct{}),
	}
	csm.streams[clientID] = cs
	log.Printf("[INFO] ClientStreamManager: Registered stream for clientID '%s' on redis channel '%s'", clientID, redisChannel)
	return cs
}

// Deregister removes a client stream from the manager.
func (csm *ClientStreamManager) Deregister(clientID string) {
	csm.mu.Lock()
	defer csm.mu.Unlock()
	if cs, ok := csm.streams[clientID]; ok {
		select {
		case <-cs.Done:
		default:
			close(cs.Done)
		}
		delete(csm.streams, clientID)
		log.Printf("[INFO] ClientStreamManager: Deregistered stream for clientID '%s'", clientID)
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
			return fmt.Errorf("failed to send to client stream for %s: %w", clientID, err)
		}
		return nil
	}
}

// handleClientConnection sets up the initial state for a client in Redis
// and implements the "Dam and Drain" logic.
func (s *Server) handleClientConnection(ctx context.Context, clientID string) error {
	statusKey := clientID + redisStatusKeySuffix

	// Check the client's current status BEFORE setting anything.
	currentStatus, err := redisclient.GetKeyValue(ctx, statusKey)

	// Handle potential Redis errors first.
	if err != nil && !errors.Is(err, redis.Nil) {
		return status.Errorf(codes.Internal, "failed to get client status: %v", err)
	}

	// This is the core logic change.
	// If the key doesn't exist (err is redis.Nil), it's a brand new client.
	if errors.Is(err, redis.Nil) {
		log.Printf("[INFO] ProcessEvent: New client detected '%s'. Setting status to LIVE.", clientID)
		// For a new client, just set their status to LIVE and their location.
		if err := redisclient.SetKeyValue(ctx, statusKey, REDIS_CLIENT_STATUS_LIVE); err != nil {
			return status.Errorf(codes.Internal, "failed to set new client status to LIVE: %v", err)
		}
		if err := redisclient.SetKeyValue(ctx, clientID+redisLocationKeySuffix, s.appConfig.RedisResultsChannel); err != nil {
			return status.Errorf(codes.Internal, "failed to set new client location: %v", err)
		}
		return nil // New client is ready to go.
	}

	// If we are here, the client already has a status.
	// If the existing status is not LIVE, it's a reconnection that needs a replay.
	if currentStatus != REDIS_CLIENT_STATUS_LIVE {
		log.Printf("[INFO] ProcessEvent: Reconnection detected for client '%s'. Initiating replay.", clientID)

		// Set client's new location.
		if err := redisclient.SetKeyValue(ctx, clientID+redisLocationKeySuffix, s.appConfig.RedisResultsChannel); err != nil {
			return status.Errorf(codes.Internal, "failed to set reconnecting client location: %v", err)
		}

		// The "Dam and Drain" Logic for Reconnection
		// 1. Build the Dam: Ensure status is REPLAYING.
		if err := redisclient.SetKeyValue(ctx, statusKey, REDIS_CLIENT_STATUS_REPLAY); err != nil {
			return status.Errorf(codes.Internal, "failed to set client status to REPLAYING: %v", err)
		}

		// 2. Determine the client's partition in the DLQ.
		dlqTopic := s.appConfig.KafkaDLQTopic
		partition, err := kafkaadmin.GetPartitionForClientKey(ctx, dlqTopic, clientID)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to determine replay partition: %v", err)
		}

		// 3. Get the "Finish Line" Offset.
		targetOffset, err := kafkaproducer.GetPartitionEndOffset(ctx, dlqTopic, partition)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to get replay target offset: %v", err)
		}

		// 4. Store the Finish Line in Redis.
		err = redisclient.SetKeyValue(ctx, clientID+redisTargetOffsetKeySuffix, targetOffset)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to set replay target offset: %v", err)
		}
		log.Printf("[INFO] ProcessEvent: Replay target for client '%s' set to offset %d.", clientID, targetOffset)
	} else {
		// The client was already marked as LIVE, maybe due to a fast reconnect.
		// Just update their location.
		log.Printf("[INFO] ProcessEvent: Client '%s' is already LIVE. Updating location.", clientID)
		if err := redisclient.SetKeyValue(ctx, clientID+redisLocationKeySuffix, s.appConfig.RedisResultsChannel); err != nil {
			return status.Errorf(codes.Internal, "failed to update LIVE client location: %v", err)
		}
	}

	return nil
}

// handleClientDisconnection cleans up the client's state in Redis.
func (s *Server) handleClientDisconnection(ctx context.Context, clientID string) {
	log.Printf("[INFO] ProcessEvent: Disconnecting client '%s'. Cleaning up Redis state.", clientID)

	// On disconnect, we only remove the client's location.
	// We leave the -status key alone. If it was LIVE, the next connection will see that.
	// If it was REPLAYING, we want it to stay that way so the replay continues on next connect.
	if err := redisclient.DeleteKey(ctx, clientID+redisLocationKeySuffix); err != nil {
		log.Printf("[ERROR] ProcessEvent: Failed to delete location key for '%s': %v", clientID, err)
	}
}

// ProcessEvent is the gRPC bidirectional streaming method.
func (s *Server) ProcessEvent(stream pb.NimbusService_ProcessEventServer) error {
	ctx := stream.Context()

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Errorf(codes.DataLoss, "failed to get metadata")
	}

	clientIDValues := md.Get("client_id")
	if len(clientIDValues) == 0 || clientIDValues[0] == "" {
		return status.Errorf(codes.InvalidArgument, "client_id not received or empty in metadata")
	}
	clientID := clientIDValues[0]

	// Register stream and setup deferred cleanup
	_ = s.clientStreamManager.Register(s.appConfig.RedisResultsChannel, clientID, stream)
	defer s.clientStreamManager.Deregister(clientID)
	defer s.handleClientDisconnection(ctx, clientID)

	// MODIFIED: Centralized connection handling logic.
	if err := s.handleClientConnection(ctx, clientID); err != nil {
		log.Printf("[ERROR] ProcessEvent: Failed during client connection setup for '%s': %v", clientID, err)
		return err
	}

	// Goroutine to handle context cancellation (client disconnect, server shutdown)
	go func() {
		<-ctx.Done()
		log.Printf("[INFO] ProcessEvent: Stream context done for clientID '%s'. Error: %v", clientID, ctx.Err())
	}()

	// Main loop to receive events from the client.
	for {
		req, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) || status.Code(err) == codes.Canceled {
				log.Printf("[INFO] ProcessEvent: Client '%s' closed the stream.", clientID)
				return nil // Clean disconnect.
			}
			log.Printf("[ERROR] ProcessEvent: Error receiving message from client '%s': %v", clientID, err)
			return status.Errorf(codes.Internal, "error receiving message: %v", err)
		}

		log.Printf("[INFO] ProcessEvent: Received '%s' event from client '%s' for number %d", req.EventName, clientID, req.Number)

		switch strings.ToLower(req.EventName) {
		case "sq":
			kafkaReq := &pb.KafkaEventReqest{
				EventName:    req.EventName,
				Number:       req.Number,
				ClientId:     clientID,
				RedisChannel: s.appConfig.RedisResultsChannel,
			}
			if err := kafkaproducer.SendEventToDefaultTopic(kafkaReq); err != nil {
				log.Printf("[ERROR] ProcessEvent: Failed to send event to Kafka for client '%s': %v", clientID, err)
			} else {
				log.Printf("[DEBUG] ProcessEvent: Event for client '%s' sent to Kafka.", clientID)
			}
		default:
			log.Printf("[WARN] ProcessEvent: Received unknown event name '%s' from client '%s'", req.EventName, clientID)
		}
	}
}
