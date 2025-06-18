package redisclient

import (
	"context"
	"net"
	"strconv"
	"time"

	"github.com/learningfun-dev/NimbusGRPC/nimbus/common"
	"github.com/learningfun-dev/NimbusGRPC/nimbus/constants"
	pb "github.com/learningfun-dev/NimbusGRPC/nimbus/proto"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/proto"
)

// StreamSender defines the interface required by RedisSubscriber
// to send messages back to the appropriate client stream.
type StreamSender interface {
	SendToClient(clientID string, resp *pb.EventResponse) error
}

// RedisSubscriber handles subscribing to Redis and processing messages.
type RedisSubscriber struct {
	redisClient   *redis.Client
	channel       string
	streamManager StreamSender
}

// NewRedisSubscriber creates a new RedisSubscriber.
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

// Start begins the message consumption loop.
func (rs *RedisSubscriber) Start(ctx context.Context) {
	log.Info().Str("channel", rs.channel).Msg("RedisSubscriber: Starting subscription")
	var pubsub *redis.PubSub

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
			if pubsub == nil {
				pubsub = rs.redisClient.SSubscribe(ctx, rs.channel)
				_, err := pubsub.Receive(ctx)
				if err != nil {
					log.Error().Err(err).Str("channel", rs.channel).Msg("RedisSubscriber: Failed to subscribe. Retrying in 5s...")
					if pubsub != nil {
						_ = pubsub.Close()
						pubsub = nil
					}
					select {
					case <-time.After(5 * time.Second):
					case <-ctx.Done():
						return
					}
					continue
				}
				log.Info().Str("channel", rs.channel).Msg("RedisSubscriber: Successfully subscribed")
			}

			msg, err := pubsub.ReceiveTimeout(ctx, 1*time.Second)
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					continue
				}
				log.Error().Err(err).Str("channel", rs.channel).Msg("RedisSubscriber: Error receiving message. Resetting subscription.")
				if pubsub != nil {
					_ = pubsub.Close()
					pubsub = nil
				}
				continue
			}

			if m, ok := msg.(*redis.Message); ok {
				rs.processRedisMessage(ctx, m.Payload)
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

	log.Debug().
		Str("clientID", kafkaEventResp.ClientId).
		Int64("offset", kafkaEventResp.KafkaOffset).
		Msg("RedisSubscriber: Decoded message from Redis")

	// Add a structured trace step.
	kafkaEventResp.Log = common.Append(kafkaEventResp.Log, common.TraceStepInfo{
		ServiceName: "RedisSubscriber",
		MethodName:  "processRedisMessage",
		Message:     "Received message from Redis, preparing to send to client.",
		Metadata: map[string]string{
			"redis_channel": rs.channel,
		},
	})

	eventResp := &pb.EventResponse{
		EventName: kafkaEventResp.EventName,
		Number:    kafkaEventResp.Number,
		Result:    kafkaEventResp.Result,
		Log:       kafkaEventResp.Log, // Pass the updated log entry
	}

	// Send the message to the client.
	err := rs.streamManager.SendToClient(kafkaEventResp.ClientId, eventResp)
	if err != nil {
		log.Error().Err(err).Str("clientID", kafkaEventResp.ClientId).Msg("RedisSubscriber: Failed to send EventResponse to client")
		return // If send fails, we cannot ACK. The ReplayConsumer will time out and retry.
	}

	// If the message has a Kafka offset, it means it's a replayed message that requires an ACK.
	if kafkaEventResp.KafkaOffset > 0 {
		ackKey := kafkaEventResp.ClientId + constants.RedisLastAckedOffsetKeySuffix
		// Acknowledge the specific offset that was successfully delivered.
		err = SetKeyValue(context.Background(), ackKey, strconv.FormatInt(kafkaEventResp.KafkaOffset, 10))
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
				Msg("RedisSubscriber: Sent ACK for replayed message.")
		}
	}
}
