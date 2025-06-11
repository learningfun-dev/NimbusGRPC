package redisclient

import (
	"context"
	"errors"
	"log"
	"net"
	"time"

	pb "github.com/learningfun-dev/NimbusGRPC/nimbus/proto"
	"github.com/redis/go-redis/v9"
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
		log.Fatal("[FATAL] RedisSubscriber: Redis client cannot be nil")
	}
	if channelName == "" {
		log.Fatal("[FATAL] RedisSubscriber: Redis channel name cannot be empty")
	}
	if sm == nil {
		log.Fatal("[FATAL] RedisSubscriber: StreamManager cannot be nil")
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
	log.Printf("[INFO] RedisSubscriber: Starting subscription to channel '%s'", rs.channel)
	var pubsub *redis.PubSub

	// Reconnection loop
	for {
		select {
		case <-ctx.Done():
			log.Printf("[INFO] RedisSubscriber: Context canceled for channel '%s', stopping subscriber.", rs.channel)
			if pubsub != nil {
				if err := pubsub.Close(); err != nil {
					log.Printf("[ERROR] RedisSubscriber: Error closing pubsub for channel '%s': %v", rs.channel, err)
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
					log.Printf("[ERROR] RedisSubscriber: Failed to subscribe to channel '%s': %v. Retrying in 5s...", rs.channel, err)
					if pubsub != nil {
						_ = pubsub.Close()
						pubsub = nil
					}
					// Respect context cancellation during sleep
					select {
					case <-time.After(5 * time.Second): // Backoff before retrying
					case <-ctx.Done():
						log.Printf("[INFO] RedisSubscriber: Context canceled during retry backoff for channel '%s'.", rs.channel)
						return
					}
					continue
				}
				log.Printf("[INFO] RedisSubscriber: Successfully subscribed to channel '%s'", rs.channel)
			}

			// Receive message with timeout to allow checking ctx.Done periodically
			msg, err := pubsub.ReceiveTimeout(ctx, 1*time.Second) // Use ReceiveTimeout
			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, redis.ErrClosed) {
					log.Printf("[INFO] RedisSubscriber: Subscription closed or context canceled for channel '%s'.", rs.channel)
					if pubsub != nil {
						_ = pubsub.Close()
						pubsub = nil
					}
					if errors.Is(err, context.Canceled) { // If context is done, the outer loop will catch it.
						return
					}
					continue // Attempt to resubscribe if not context canceled
				}
				// Check for timeout error specifically from ReceiveTimeout
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					// This is an expected timeout from ReceiveTimeout, continue to check ctx.Done
					continue
				}
				// For other errors, log and attempt to resubscribe
				log.Printf("[ERROR] RedisSubscriber: Error receiving message from channel '%s': %v. Attempting to resubscribe...", rs.channel, err)
				if pubsub != nil {
					_ = pubsub.Close()
					pubsub = nil
				}
				// Respect context cancellation during sleep
				select {
				case <-time.After(1 * time.Second): // Short delay before resubscribe attempt
				case <-ctx.Done():
					log.Printf("[INFO] RedisSubscriber: Context canceled during resubscribe delay for channel '%s'.", rs.channel)
					return
				}
				continue
			}

			// Process the received message
			switch m := msg.(type) {
			case *redis.Message:
				log.Printf("[DEBUG] RedisSubscriber: Received message from channel '%s': %s", m.Channel, m.Payload)
				rs.processRedisMessage(m.Payload)
			case *redis.Subscription:
				log.Printf("[INFO] RedisSubscriber: Subscription status for channel '%s': Kind=%s, Count=%d", m.Channel, m.Kind, m.Count)
				if m.Count == 0 && (m.Kind == "unsubscribe" || m.Kind == "punsubscribe") {
					log.Printf("[INFO] RedisSubscriber: Unsubscribed from channel '%s'. Will attempt to resubscribe.", rs.channel)
					if pubsub != nil {
						_ = pubsub.Close()
						pubsub = nil
					}
				}
			case *redis.Pong:
				log.Printf("[DEBUG] RedisSubscriber: Received PONG from channel '%s': %s", rs.channel, m.Payload)
			default:
				log.Printf("[WARN] RedisSubscriber: Received unexpected message type on channel '%s': %T", rs.channel, m)
			}
		}
	}
}

func (rs *RedisSubscriber) processRedisMessage(payload string) {
	var kafkaEventResp pb.KafkaEventResponse
	if err := proto.Unmarshal([]byte(payload), &kafkaEventResp); err != nil {
		log.Printf("[ERROR] RedisSubscriber: Error decoding Redis message payload on channel '%s': %v. Payload: %s", rs.channel, err, payload)
		return // Skip malformed messages
	}

	log.Printf("[INFO] RedisSubscriber: Decoded KafkaEventResponse from channel '%s' for clientID: %s, Event: %s, Number: %d, Result: %d",
		rs.channel, kafkaEventResp.ClientId, kafkaEventResp.EventName, kafkaEventResp.Number, kafkaEventResp.Result)

	// Prepare the final response to send to the gRPC client.
	eventResp := &pb.EventResponse{
		EventName: kafkaEventResp.EventName,
		Number:    kafkaEventResp.Number,
		Result:    kafkaEventResp.Result,
	}

	// Send the message to the client via the stream manager.
	err := rs.streamManager.SendToClient(kafkaEventResp.ClientId, eventResp)
	if err != nil {
		log.Printf("[ERROR] RedisSubscriber: Failed to send EventResponse to client '%s': %v", kafkaEventResp.ClientId, err)
		return // If send fails, we cannot ACK. The ReplayConsumer will time out and retry.
	}

	// MODIFIED: This is the new ACK logic.
	// Check if this was a replayed message (indicated by a positive offset).
	if kafkaEventResp.KafkaOffset > 0 {
		ackKey := kafkaEventResp.ClientId + "-last-acked-offset"
		// Acknowledge the specific offset that was successfully delivered.
		err = SetKeyValue(context.Background(), ackKey, kafkaEventResp.KafkaOffset)
		if err != nil {
			log.Printf("[CRITICAL] RedisSubscriber: FAILED TO SEND ACK for client '%s', offset %d. This may cause a duplicate message. Error: %v",
				kafkaEventResp.ClientId, kafkaEventResp.KafkaOffset, err)
		} else {
			log.Printf("[INFO] RedisSubscriber: Sent ACK for client '%s', offset %d.", kafkaEventResp.ClientId, kafkaEventResp.KafkaOffset)
		}
	}
}
