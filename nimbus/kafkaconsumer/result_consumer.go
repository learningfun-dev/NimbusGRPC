package kafkaconsumer

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/learningfun-dev/NimbusGRPC/nimbus/config"
	"github.com/learningfun-dev/NimbusGRPC/nimbus/kafkaproducer"
	pb "github.com/learningfun-dev/NimbusGRPC/nimbus/proto"
	"github.com/learningfun-dev/NimbusGRPC/nimbus/redisclient"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
)

const (
	// REDIS_CLIENT_STATUS_LIVE is the status value indicating a client is fully online and not replaying.
	REDIS_CLIENT_STATUS_LIVE = "LIVE"
)

// ResultConsumer consumes results from KafkaResultsTopic and publishes them to Redis.
type ResultConsumer struct {
	consumer   *kafka.Consumer
	appConfig  *config.Config
	wg         *sync.WaitGroup
	shutdownCh chan struct{}
}

// NewResultConsumer creates a new ResultConsumer.
func NewResultConsumer(cfg *config.Config, wg *sync.WaitGroup) (*ResultConsumer, error) {
	if cfg.KafkaResultsTopic == "" {
		return nil, fmt.Errorf("ResultConsumer: KafkaResultsTopic is not configured")
	}
	if cfg.KafkaDLQTopic == "" {
		return nil, fmt.Errorf("ResultConsumer: KafkaDLQTopic is not configured")
	}

	consumerConfig := &kafka.ConfigMap{
		"bootstrap.servers":  cfg.KafkaBrokers,
		"group.id":           "nimbus-results-publisher-group", // Unique group ID
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": "false",
	}

	c, err := kafka.NewConsumer(consumerConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create ResultConsumer: %w", err)
	}

	log.Printf("[INFO] ResultConsumer: Subscribing to topic: %s", cfg.KafkaResultsTopic)
	err = c.SubscribeTopics([]string{cfg.KafkaResultsTopic}, nil)
	if err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("failed to subscribe ResultConsumer to topic %s: %w", cfg.KafkaResultsTopic, err)
	}

	return &ResultConsumer{
		consumer:   c,
		appConfig:  cfg,
		wg:         wg,
		shutdownCh: make(chan struct{}),
	}, nil
}

// Start begins the message consumption loop.
func (rc *ResultConsumer) Start(ctx context.Context) {
	defer rc.wg.Done()
	log.Printf("[INFO] ResultConsumer: Starting to consume from topic '%s'", rc.appConfig.KafkaResultsTopic)

	run := true
	for run {
		select {
		case <-ctx.Done():
			log.Printf("[INFO] ResultConsumer: Context cancellation received. Shutting down consumer for topic '%s'.", rc.appConfig.KafkaResultsTopic)
			run = false
		case <-rc.shutdownCh:
			log.Printf("[INFO] ResultConsumer: Shutdown signal received. Stopping consumer for topic '%s'.", rc.appConfig.KafkaResultsTopic)
			run = false
		default:
			msg, err := rc.consumer.ReadMessage(1 * time.Second) // Poll with timeout
			if err != nil {
				if kerr, ok := err.(kafka.Error); ok && kerr.Code() == kafka.ErrTimedOut {
					continue // This is an expected timeout when no new messages are available.
				}
				log.Printf("[ERROR] ResultConsumer: Error reading message from topic '%s': %v", rc.appConfig.KafkaResultsTopic, err)
				if kerr, ok := err.(kafka.Error); ok && kerr.IsFatal() {
					log.Printf("[FATAL] ResultConsumer: Fatal error encountered on topic '%s', shutting down.", rc.appConfig.KafkaResultsTopic)
					run = false
				}
				continue
			}

			// A message was received, now process it.
			var eventResp pb.KafkaEventResponse
			if err := proto.Unmarshal(msg.Value, &eventResp); err != nil {
				log.Printf("[ERROR] ResultConsumer: Failed to unmarshal protobuf message from topic '%s'. Error: %v. Message will be sent to a poison pill queue.", *msg.TopicPartition.Topic, err)
				// Best practice: For malformed messages, we commit the offset so we don't process it again.
				// Optionally, send the raw message to a separate "poison pill" queue for inspection.
				if _, commitErr := rc.consumer.CommitMessage(msg); commitErr != nil {
					log.Printf("[FATAL] ResultConsumer: Failed to commit poison pill message offset: %v", commitErr)
					run = false // A failure to commit is a critical error.
				}
				continue
			}

			log.Printf("[INFO] ResultConsumer: Processing result for client '%s' from topic '%s'", eventResp.ClientId, *msg.TopicPartition.Topic)

			// The processing function now returns an error.
			processingErr := rc.routeOrDivertResult(ctx, &eventResp)

			// Logic to manually commit offset.
			if processingErr == nil {
				// Success! Commit the offset so we don't re-process this message.
				if _, commitErr := rc.consumer.CommitMessage(msg); commitErr != nil {
					log.Printf("[FATAL] ResultConsumer: Failed to commit offset after successful processing: %v", commitErr)
					run = false // A failure to commit is a critical error.
				}
			} else {
				// An error occurred during processing. We DO NOT commit the offset.
				// The message will be re-delivered by Kafka after the consumer session timeout.
				log.Printf("[ERROR] ResultConsumer: Failed to process message for client '%s'. Will retry later. Error: %v", eventResp.ClientId, processingErr)
			}
		}
	}
	log.Printf("[INFO] ResultConsumer: Message consumption loop stopped for topic '%s'.", rc.appConfig.KafkaResultsTopic)
}

// It now returns an error to signal success or failure of the operation.
func (rc *ResultConsumer) routeOrDivertResult(ctx context.Context, resp *pb.KafkaEventResponse) error {
	if resp.ClientId == "" {
		log.Printf("[WARN] ResultConsumer: Received a result with an empty ClientId. Discarding message: %+v", resp)
		return nil // Discarding is considered a successful operation for this message.
	}

	statusKey := resp.ClientId + "-status"
	clientStatus, err := redisclient.GetKeyValue(ctx, statusKey)

	// Case 1: Redis error. We can't know the client's status. Return an error to trigger a retry.
	if err != nil && !errors.Is(err, redis.Nil) {
		log.Printf("[ERROR] ResultConsumer: Could not get status for client '%s' from Redis. Error: %v. Retrying...", resp.ClientId, err)
		return err // Return the error to prevent offset commit.
	}

	// Case 2: Client status is NOT "LIVE". This means the client is offline (key is missing)
	// OR the client is explicitly in a non-live state (like REPLAYING). Divert to DLQ.
	if clientStatus != REDIS_CLIENT_STATUS_LIVE {
		log.Printf("[INFO] ResultConsumer: Client '%s' status is '%s' (not LIVE). Diverting result to DLQ.", resp.ClientId, clientStatus)
		return rc.publishToDLQ(ctx, resp) // Return the result of the publish operation.
	}

	// Case 3: Confirmed client's status is "LIVE". Publish to their pod's Redis channel.
	log.Printf("[INFO] ResultConsumer: Client '%s' status is LIVE. Publishing result to Redis channel '%s'.", resp.ClientId, resp.RedisChannel)
	return rc.publishToRedis(ctx, resp) // Return the result of the publish operation.
}

func (rc *ResultConsumer) publishToDLQ(ctx context.Context, resp *pb.KafkaEventResponse) error {
	err := kafkaproducer.PublishEventResponse(rc.appConfig.KafkaDLQTopic, resp)
	if err != nil {
		log.Printf("[CRITICAL] ResultConsumer: FAILED TO PUBLISH TO DLQ for client '%s'. Will be retried. Error: %v", resp.ClientId, err)
		return err // Return error to prevent offset commit.
	}
	log.Printf("[DEBUG] ResultConsumer: Successfully published result for client '%s' to DLQ topic '%s'.", resp.ClientId, rc.appConfig.KafkaDLQTopic)
	return nil // Success.
}

func (rc *ResultConsumer) publishToRedis(ctx context.Context, resp *pb.KafkaEventResponse) error {
	if resp.RedisChannel == "" {
		log.Printf("[WARN] ResultConsumer: Cannot publish to Redis for client '%s' because RedisChannel is empty. Diverting to DLQ.", resp.ClientId)
		return rc.publishToDLQ(ctx, resp) // Divert to DLQ and return its status.
	}
	err := redisclient.Publish(ctx, resp)
	if err != nil {
		log.Printf("[ERROR] ResultConsumer: Failed to publish result to Redis channel '%s' for client '%s'. Will be retried. Error: %v", resp.RedisChannel, resp.ClientId, err)
		return err // Return error to prevent offset commit.
	}
	return nil // Success.
}

// Shutdown gracefully stops the consumer.
func (rc *ResultConsumer) Shutdown() {
	log.Printf("[INFO] ResultConsumer: Initiating shutdown for consumer of topic '%s'...", rc.appConfig.KafkaResultsTopic)
	close(rc.shutdownCh)
	if rc.consumer != nil {
		log.Printf("[INFO] ResultConsumer: Closing Kafka consumer instance for topic '%s'.", rc.appConfig.KafkaResultsTopic)
		if err := rc.consumer.Close(); err != nil {
			log.Printf("[ERROR] ResultConsumer: Error closing Kafka consumer for topic '%s': %v", rc.appConfig.KafkaResultsTopic, err)
		}
	}
	log.Printf("[INFO] ResultConsumer: Shutdown complete for topic '%s'.", rc.appConfig.KafkaResultsTopic)
}
