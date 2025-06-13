package redisclient

import (
	"context"
	"fmt"

	pb "github.com/learningfun-dev/NimbusGRPC/nimbus/proto"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/proto"
)

// Publish sends a KafkaEventResponse to the specified Redis channel.
// It uses the globally managed Redis client.
func Publish(ctx context.Context, e *pb.KafkaEventResponse) error {
	rdb := GetClient() // Get the initialized singleton client

	// Marshal the entire protobuf message for efficient transport.
	value, err := proto.Marshal(e)
	if err != nil {
		log.Error().Err(err).Msg("RedisPublisher: Failed to marshal KafkaEventResponse to proto")
		return fmt.Errorf("failed to marshal KafkaEventResponse: %w", err)
	}

	// Use the context passed from the caller.
	if ctx == nil {
		ctx = context.Background() // Fallback if no context is provided.
	}

	// Use SPUBLISH for Redis Cluster to ensure the message goes to the shard holding the channel.
	// For a single-instance Redis, PUBLISH is also fine.
	err = rdb.SPublish(ctx, e.RedisChannel, value).Err()
	if err != nil {
		log.Error().
			Err(err).
			Str("channel", e.RedisChannel).
			Msg("RedisPublisher: Failed to publish message to Redis channel")
		return fmt.Errorf("failed to publish to Redis channel %s: %w", e.RedisChannel, err)
	}

	log.Debug().
		Str("channel", e.RedisChannel).
		Str("clientID", e.ClientId).
		Int64("offset", e.KafkaOffset).
		Msg("RedisPublisher: Message successfully published to Redis channel")

	return nil
}
