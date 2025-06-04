package kafkaconsumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/learningfun-dev/NimbusGRPC/nimbus/config"      // Your config package
	pb "github.com/learningfun-dev/NimbusGRPC/nimbus/proto"    // Your proto package
	"github.com/learningfun-dev/NimbusGRPC/nimbus/redisclient" // Your Redis client package
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

	consumerConfig := &kafka.ConfigMap{
		"bootstrap.servers": cfg.KafkaBrokers,
		"group.id":          "nimbus-results-publisher-group", // Unique group ID
		"auto.offset.reset": "earliest",
		// "enable.auto.commit": false,
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
					continue
				}
				log.Printf("[ERROR] ResultConsumer: Error reading message from topic '%s': %v", rc.appConfig.KafkaResultsTopic, err)
				if kerr, ok := err.(kafka.Error); ok && kerr.IsFatal() {
					log.Printf("[FATAL] ResultConsumer: Fatal error encountered for topic '%s'. Shutting down.", rc.appConfig.KafkaResultsTopic)
					run = false
				}
				continue
			}

			log.Printf("[DEBUG] ResultConsumer: Received message on topic '%s': Partition=%d, Offset=%d, Key=%s",
				*msg.TopicPartition.Topic, msg.TopicPartition.Partition, msg.TopicPartition.Offset, string(msg.Key))

			var eventResp pb.KafkaEventResponse
			if err := json.Unmarshal(msg.Value, &eventResp); err != nil {
				log.Printf("[ERROR] ResultConsumer: Failed to unmarshal KafkaEventResponse from topic '%s': %v. Message: %s",
					*msg.TopicPartition.Topic, err, string(msg.Value))
				continue
			}

			log.Printf("[INFO] ResultConsumer: Processing result: %+v from topic '%s'", eventResp, *msg.TopicPartition.Topic)
			rc.publishToRedis(ctx, &eventResp) // Pass context for Redis publish

			// If using manual commits:
			// if _, err := rc.consumer.CommitMessage(msg); err != nil {
			// log.Printf("[ERROR] ResultConsumer: Failed to commit offset for message on topic '%s': %v", *msg.TopicPartition.Topic, err)
			// }
		}
	}
	log.Printf("[INFO] ResultConsumer: Message consumption loop stopped for topic '%s'.", rc.appConfig.KafkaResultsTopic)
}

func (rc *ResultConsumer) publishToRedis(ctx context.Context, resp *pb.KafkaEventResponse) {
	if resp.RedisChannel == "" {
		log.Printf("[WARN] ResultConsumer: RedisChannel is empty in KafkaEventResponse for clientID '%s'. Cannot publish to Redis.", resp.ClientId)
		return
	}

	// The redisclient.Publish function takes a context.
	err := redisclient.Publish(ctx, resp) // Pass the context
	if err != nil {
		log.Printf("[ERROR] ResultConsumer: Failed to publish result to Redis channel '%s' for clientID '%s': %v. Result: %+v",
			resp.RedisChannel, resp.ClientId, err, resp)
		// Implement retry logic or dead-letter queue for failed Redis publishes.
	} else {
		log.Printf("[INFO] ResultConsumer: Successfully published result to Redis channel '%s' for clientID '%s'. Result: %+v",
			resp.RedisChannel, resp.ClientId, resp)
	}
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
