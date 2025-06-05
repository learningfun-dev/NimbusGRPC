package redisclient

import (
	"context"
	"fmt"
	"log"

	pb "github.com/learningfun-dev/NimbusGRPC/nimbus/proto"
	"google.golang.org/protobuf/proto"
)

// Publish sends a KafkaEventResponse to the specified Redis channel.
// It uses the globally managed Redis client.
func Publish(ctx context.Context, e *pb.KafkaEventResponse) error {
	rdb := GetClient() // Get the initialized singleton client

	value, err := proto.Marshal(e)
	if err != nil {
		log.Printf("[ERROR] RedisPublisher: Failed to marshal KafkaEventResponse to proto: %v. Event: %+v", err, e)
		return fmt.Errorf("failed to marshal KafkaEventResponse: %w", err)
	}

	// It's good practice to use the context passed from the caller,
	// or a background context if the operation is truly detached.
	if ctx == nil {
		ctx = context.Background() // Fallback, but prefer caller-supplied context
	}

	err = rdb.SPublish(ctx, e.RedisChannel, value).Err()
	if err != nil {
		log.Printf("[ERROR] RedisPublisher: Failed to publish message to Redis channel '%s': %v. Payload: %s", e.RedisChannel, err, string(value))
		return fmt.Errorf("failed to publish to Redis channel %s: %w", e.RedisChannel, err)
	}

	log.Printf("[DEBUG] RedisPublisher: Message successfully published to Redis channel '%s'. Payload: %s", e.RedisChannel, string(value))
	return nil
}
