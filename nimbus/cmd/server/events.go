package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/learningfun-dev/NimbusGRPC/nimbus/constants"
	"github.com/learningfun-dev/NimbusGRPC/nimbus/kafkaproducer"
	pb "github.com/learningfun-dev/NimbusGRPC/nimbus/proto"
	"github.com/learningfun-dev/NimbusGRPC/nimbus/redisclient"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
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
	log.Info().
		Str("clientID", clientID).
		Str("redisChannel", redisChannel).
		Msg("ClientStreamManager: Registered stream")
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
		log.Info().Str("clientID", clientID).Msg("ClientStreamManager: Deregistered stream")
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
		log.Warn().Str("clientID", clientID).Msg("ClientStreamManager: Client stream not found during SendToClient")
		return fmt.Errorf("client stream not found for clientID: %s", clientID)
	}

	select {
	case <-cs.Done:
		log.Warn().Str("clientID", clientID).Msg("ClientStreamManager: Attempted to send to a closed/done stream")
		return fmt.Errorf("stream for clientID %s is already closed/done", clientID)
	default:
		if err := cs.Stream.Send(resp); err != nil {
			log.Error().Err(err).Str("clientID", clientID).Msg("ClientStreamManager: Failed to send message to client")
			return fmt.Errorf("failed to send to client stream for %s: %w", clientID, err)
		}
		return nil
	}
}

// handleClientConnection sets up the initial state for a client in Redis.
func (s *Server) handleClientConnection(ctx context.Context, clientID string) error {
	newPodChannel := s.appConfig.RedisResultsChannel
	statusKey := clientID + constants.RedisStatusKeySuffix
	locationKey := clientID + constants.RedisLocationKeySuffix

	// Check the client's current status BEFORE setting anything.
	currentStatus, err := redisclient.GetKeyValue(ctx, statusKey)
	if err != nil && !errors.Is(err, redis.Nil) {
		return status.Errorf(codes.Internal, "failed to get client status: %v", err)
	}

	// If status is not LIVE (it's OFFLINE, REPLAYING, or NIL), we need to start/continue the replay process.
	if currentStatus != constants.REDIS_CLIENT_STATUS_LIVE {
		if errors.Is(err, redis.Nil) {
			log.Info().Str("clientID", clientID).Msg("New client detected. Setting status to LIVE.")
			// For a brand new client, just set their status to LIVE.
			if err := redisclient.SetKeyValue(ctx, statusKey, constants.REDIS_CLIENT_STATUS_LIVE); err != nil {
				return status.Errorf(codes.Internal, "failed to set new client status to LIVE: %v", err)
			}
		} else {
			// This is a returning client who was offline. Initiate replay.
			log.Info().Str("clientID", clientID).Str("previousStatus", currentStatus).Msg("Reconnection detected. Initiating replay.")
			if err := redisclient.SetKeyValue(ctx, statusKey, constants.REDIS_CLIENT_STATUS_REPLAY); err != nil {
				return status.Errorf(codes.Internal, "failed to set client status to REPLAYING: %v", err)
			}
		}
	} else {
		log.Info().Str("clientID", clientID).Msg("Client is already LIVE. No replay needed.")
	}

	// Always update the location to the current pod channel.
	if err := redisclient.SetKeyValue(ctx, locationKey, newPodChannel); err != nil {
		return status.Errorf(codes.Internal, "failed to update client location: %v", err)
	}

	return nil
}

// handleClientDisconnection cleans up the client's state in Redis.
func (s *Server) handleClientDisconnection(clientID string) {
	cleanupCtx := context.Background()
	log.Info().Str("clientID", clientID).Msg("Disconnecting client. Cleaning up Redis state.")

	// Set status to OFFLINE so the next connection triggers a replay check.
	if err := redisclient.SetKeyValue(cleanupCtx, clientID+constants.RedisStatusKeySuffix, constants.REDIS_CLIENT_STATUS_OFFLINE); err != nil {
		log.Error().Err(err).Str("clientID", clientID).Msg("Failed to set status to OFFLINE")
	}

	if err := redisclient.DeleteKey(cleanupCtx, clientID+constants.RedisLocationKeySuffix); err != nil {
		log.Error().Err(err).Str("clientID", clientID).Msg("Failed to delete location key")
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

	_ = s.clientStreamManager.Register(s.appConfig.RedisResultsChannel, clientID, stream)
	defer s.clientStreamManager.Deregister(clientID)
	defer s.handleClientDisconnection(clientID)

	if err := s.handleClientConnection(ctx, clientID); err != nil {
		log.Error().Err(err).Str("clientID", clientID).Msg("Failed during client connection setup")
		return err
	}

	go func() {
		<-ctx.Done()
		log.Info().Str("clientID", clientID).Err(ctx.Err()).Msg("Stream context done")
	}()

	for {
		req, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) || status.Code(err) == codes.Canceled {
				log.Info().Str("clientID", clientID).Msg("Client closed the stream.")
				return nil
			}
			log.Error().Err(err).Str("clientID", clientID).Msg("Error receiving message from client")
			return status.Errorf(codes.Internal, "error receiving message: %v", err)
		}

		log.Info().
			Str("clientID", clientID).
			Str("eventName", req.EventName).
			Int32("number", req.Number).
			Msg("Received event from client")

		switch strings.ToLower(req.EventName) {
		case "sq":
			kafkaReq := &pb.KafkaEventReqest{
				EventName:    req.EventName,
				Number:       req.Number,
				ClientId:     clientID,
				RedisChannel: s.appConfig.RedisResultsChannel,
			}
			if err := kafkaproducer.SendEventToDefaultTopic(kafkaReq); err != nil {
				log.Error().Err(err).Str("clientID", clientID).Msg("Failed to send event to Kafka")
			} else {
				log.Debug().Str("clientID", clientID).Msg("Event sent to Kafka.")
			}
		default:
			log.Warn().Str("clientID", clientID).Str("eventName", req.EventName).Msg("Received unknown event name")
		}
	}
}
