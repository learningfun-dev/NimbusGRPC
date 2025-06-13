package redisclient

import (
	"context"
	"errors"
	"net"
	"time"

	pb "github.com/learningfun-dev/NimbusGRPC/nimbus/proto"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/proto"
)

// StreamSender defines the interface required by RedisSubscriber
// to send messages back to the appropriate client stream.
// This interface will be implemented by your ClientStreamManager.
type StreamSender interface {
	SendToClient(clientID string, resp *pb.EventResponse) error
}

// RedisSubscriber handles subscribing to Redis and processing messages.
type RedisSubscriber struct {
	redisClient   *redis.Client
	channel       string
	streamManager StreamSender // Uses the interface for decoupling
}

// NewRedisSubscriber creates a new RedisSubscriber.
// It takes a Redis client, the channel to subscribe to, and a StreamSender implementation.
func NewRedisSubscriber(rdb *redis.Client, channelName string, sm StreamSender) *RedisSubscriber {
	if rdb == nil {
		log.Fatal().Msg("RedisSubscriber: Redis client cannot be nil")
	}
	if channelName == "" {
		log.Fatal().Msg("RedisSubscriber: Redis channel name cannot be empty")
	}
	if sm == nil {
		log.Fatal().Msg("RedisSubscriber: StreamManager cannot be nil")
	}
	return &RedisSubscriber{
		redisClient:   rdb,
		channel:       channelName,
		streamManager: sm,
	}
}

// Start begins subscribing to the Redis channel and processing messages.
// It runs until the provided context is canceled.
func (rs *RedisSubscriber) Start(ctx context.Context) {
	log.Info().Str("channel", rs.channel).Msg("RedisSubscriber: Starting subscription")
	var pubsub *redis.PubSub

	// Reconnection loop
	for {
		select {
		case <-ctx.Done():
			log.Info().Str("channel", rs.channel).Msg("RedisSubscriber: Context canceled, stopping subscriber.")
			if pubsub != nil {
				if err := pubsub.Close(); err != nil {
					log.Error().Err(err).Str("channel", rs.channel).Msg("RedisSubscriber: Error closing pubsub")
				}
			}
			return
		default:
			// Attempt to subscribe
			if pubsub == nil {
				pubsub = rs.redisClient.SSubscribe(ctx, rs.channel)
				// Check for initial subscription error
				_, err := pubsub.Receive(ctx) // This is a control message from go-redis
				if err != nil {
					log.Error().Err(err).Str("channel", rs.channel).Msg("RedisSubscriber: Failed to subscribe. Retrying in 5s...")
					if pubsub != nil {
						_ = pubsub.Close()
						pubsub = nil
					}
					// Respect context cancellation during sleep
					select {
					case <-time.After(5 * time.Second): // Backoff before retrying
					case <-ctx.Done():
						log.Info().Str("channel", rs.channel).Msg("RedisSubscriber: Context canceled during retry backoff.")
						return
					}
					continue
				}
				log.Info().Str("channel", rs.channel).Msg("RedisSubscriber: Successfully subscribed")
			}

			// Receive message with timeout to allow checking ctx.Done periodically
			msg, err := pubsub.ReceiveTimeout(ctx, 1*time.Second) // Use ReceiveTimeout
			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, redis.ErrClosed) {
					log.Info().Err(err).Str("channel", rs.channel).Msg("RedisSubscriber: Subscription closed or context canceled.")
					if pubsub != nil {
						_ = pubsub.Close()
						pubsub = nil
					}
					if errors.Is(err, context.Canceled) {
						return
					}
					continue // Attempt to resubscribe if not context canceled
				}
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					continue
				}
				log.Error().Err(err).Str("channel", rs.channel).Msg("RedisSubscriber: Error receiving message. Attempting to resubscribe...")
				if pubsub != nil {
					_ = pubsub.Close()
					pubsub = nil
				}
				select {
				case <-time.After(1 * time.Second):
				case <-ctx.Done():
					log.Info().Str("channel", rs.channel).Msg("RedisSubscriber: Context canceled during resubscribe delay.")
					return
				}
				continue
			}

			// Process the received message
			switch m := msg.(type) {
			case *redis.Message:
				log.Debug().Str("channel", m.Channel).Msg("RedisSubscriber: Received message")
				rs.processRedisMessage(ctx, m.Payload) // Pass context for ACK
			case *redis.Subscription:
				log.Info().
					Str("channel", m.Channel).
					Str("kind", m.Kind).
					Int("count", m.Count).
					Msg("RedisSubscriber: Subscription status update")
				if m.Count == 0 && (m.Kind == "unsubscribe" || m.Kind == "punsubscribe") {
					log.Info().Str("channel", rs.channel).Msg("RedisSubscriber: Unsubscribed. Will attempt to resubscribe.")
					if pubsub != nil {
						_ = pubsub.Close()
						pubsub = nil
					}
				}
			case *redis.Pong:
				log.Debug().Str("channel", rs.channel).Msg("RedisSubscriber: Received PONG")
			default:
				log.Warn().Str("channel", rs.channel).Interface("type", m).Msg("RedisSubscriber: Received unexpected message type")
			}
		}
	}
}

func (rs *RedisSubscriber) processRedisMessage(ctx context.Context, payload string) {
	var kafkaEventResp pb.KafkaEventResponse
	if err := proto.Unmarshal([]byte(payload), &kafkaEventResp); err != nil {
		log.Error().Err(err).Str("channel", rs.channel).Msg("RedisSubscriber: Error decoding protobuf message from Redis")
		return
	}

	log.Info().
		Str("channel", rs.channel).
		Str("clientID", kafkaEventResp.ClientId).
		Int64("offset", kafkaEventResp.KafkaOffset).
		Msg("RedisSubscriber: Decoded KafkaEventResponse from Redis")

	// Prepare the final response to send to the gRPC client.
	eventResp := &pb.EventResponse{
		EventName: kafkaEventResp.EventName,
		Number:    kafkaEventResp.Number,
		Result:    kafkaEventResp.Result,
	}

	// Send the message to the client via the stream manager.
	err := rs.streamManager.SendToClient(kafkaEventResp.ClientId, eventResp)
	if err != nil {
		log.Error().Err(err).Str("clientID", kafkaEventResp.ClientId).Msg("RedisSubscriber: Failed to send EventResponse to client")
		return // If send fails, we cannot ACK. The ReplayConsumer will time out and retry.
	}

	// This is the new ACK logic.
	// Check if this was a replayed message (indicated by a positive offset).
	if kafkaEventResp.KafkaOffset > 0 {
		ackKey := kafkaEventResp.ClientId + "-last-acked-offset"
		// Acknowledge the specific offset that was successfully delivered.
		err = SetKeyValue(ctx, ackKey, kafkaEventResp.KafkaOffset)
		if err != nil {
			log.Error().
				Err(err).
				Str("clientID", kafkaEventResp.ClientId).
				Int64("offset", kafkaEventResp.KafkaOffset).
				Msg("CRITICAL: RedisSubscriber: FAILED TO SEND ACK. This may cause a duplicate message.")
		} else {
			log.Info().
				Str("clientID", kafkaEventResp.ClientId).
				Int64("offset", kafkaEventResp.KafkaOffset).
				Msg("RedisSubscriber: Sent ACK.")
		}
	}
}
