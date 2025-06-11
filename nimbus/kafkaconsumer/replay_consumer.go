package kafkaconsumer

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/learningfun-dev/NimbusGRPC/nimbus/config"
	pb "github.com/learningfun-dev/NimbusGRPC/nimbus/proto"
	"github.com/learningfun-dev/NimbusGRPC/nimbus/redisclient"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
)

const (
	// REDIS_CLIENT_STATUS_REPLAY is the status indicating a client is currently replaying messages.
	REDIS_CLIENT_STATUS_REPLAY = "REPLAYING"

	// Key suffixes for Redis state management
	redisStatusKeySuffix   = "-status"
	redisLocationKeySuffix = "-location"
	redisLastAckKeySuffix  = "-last-acked-sequence"

	// Timeout for waiting for a delivery acknowledgement from the server pod.
	ackTimeout = 15 * time.Second
)

// ReplayConsumer consumes from the DLQ (undelivered-results-topic) and replays messages
// to reconnected clients in a durable, acknowledged manner.
type ReplayConsumer struct {
	consumer          *kafka.Consumer
	appConfig         *config.Config
	wg                *sync.WaitGroup
	shutdownCh        chan struct{}
	processingClients sync.Map // MODIFIED: Tracks clients currently being processed to prevent head-of-line blocking.
}

// NewReplayConsumer creates a new ReplayConsumer.
func NewReplayConsumer(cfg *config.Config, wg *sync.WaitGroup) (*ReplayConsumer, error) {
	if cfg.KafkaDLQTopic == "" {
		return nil, fmt.Errorf("ReplayConsumer: KafkaDLQTopic is not configured")
	}

	consumerConfig := &kafka.ConfigMap{
		"bootstrap.servers":  cfg.KafkaBrokers,
		"group.id":           "nimbus-replay-service-group", // Must be a unique group.id for this service.
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": "false", // We absolutely must manage offsets manually.
	}

	c, err := kafka.NewConsumer(consumerConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create ReplayConsumer: %w", err)
	}

	log.Printf("[INFO] ReplayConsumer: Subscribing to topic: %s", cfg.KafkaDLQTopic)
	err = c.SubscribeTopics([]string{cfg.KafkaDLQTopic}, nil)
	if err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("failed to subscribe ReplayConsumer to topic %s: %w", cfg.KafkaDLQTopic, err)
	}

	return &ReplayConsumer{
		consumer:   c,
		appConfig:  cfg,
		wg:         wg,
		shutdownCh: make(chan struct{}),
	}, nil
}

// Start begins the message consumption loop. It acts as a dispatcher.
func (rc *ReplayConsumer) Start(ctx context.Context) {
	defer rc.wg.Done()
	log.Printf("[INFO] ReplayConsumer: Starting to consume from DLQ topic '%s'", rc.appConfig.KafkaDLQTopic)

	run := true
	for run {
		select {
		case <-ctx.Done():
			run = false
		case <-rc.shutdownCh:
			run = false
		default:
			msg, err := rc.consumer.ReadMessage(1 * time.Second)
			if err != nil {
				if kerr, ok := err.(kafka.Error); ok && kerr.Code() == kafka.ErrTimedOut {
					continue
				}
				log.Printf("[ERROR] ReplayConsumer: Error reading message: %v", err)
				if kerr, ok := err.(kafka.Error); ok && kerr.IsFatal() {
					run = false
				}
				continue
			}

			// MODIFIED: Asynchronous, non-blocking dispatch logic.
			clientID := string(msg.Key)
			if clientID == "" {
				log.Printf("[WARN] ReplayConsumer: Message in DLQ has no Key (ClientId). Skipping and committing.")
				if _, commitErr := rc.consumer.CommitMessage(msg); commitErr != nil {
					log.Printf("[FATAL] ReplayConsumer: CRITICAL - Failed to commit skipped message. Error: %v", commitErr)
					run = false
				}
				continue
			}

			// Check if a goroutine is already processing this client.
			if _, loaded := rc.processingClients.LoadOrStore(clientID, true); loaded {
				// If loaded is true, another goroutine is already working on this client.
				// We DO NOT process this message now and DO NOT commit its offset.
				// This prevents a single stuck client from blocking the partition.
				// The main loop will continue to read messages for other clients.
				log.Printf("[DEBUG] ReplayConsumer: Replay for client '%s' is already in progress. Skipping message for now.", clientID)
				continue
			}

			// If we are the first to see a message for this client, start a new goroutine to process it.
			rc.wg.Add(1)
			go rc.processMessageAsynchronously(ctx, msg)
		}
	}
	log.Printf("[INFO] ReplayConsumer: Message consumption loop stopped for topic '%s'.", rc.appConfig.KafkaDLQTopic)
}

// processMessageAsynchronously handles the entire lifecycle of a single message in a separate goroutine.
func (rc *ReplayConsumer) processMessageAsynchronously(ctx context.Context, msg *kafka.Message) {
	defer rc.wg.Done()
	clientID := string(msg.Key)

	// IMPORTANT: Ensure we unlock this client for future messages, no matter what happens.
	defer rc.processingClients.Delete(clientID)

	processingErr := rc.replaySingleMessageAndWaitForAck(ctx, msg)

	if processingErr == nil {
		// Success! The message was delivered and acknowledged. Commit the offset.
		if _, commitErr := rc.consumer.CommitMessage(msg); commitErr != nil {
			log.Printf("[FATAL] ReplayConsumer: CRITICAL - Failed to commit offset after ACK for client '%s'. Error: %v", clientID, commitErr)
			// This is a critical state. A monitoring system should alert on this.
		}
	} else {
		// An error occurred (e.g., ACK timeout). We do not commit the offset.
		// The defer statement will unlock the client, allowing this same message to be picked up and retried later.
		log.Printf("[WARN] ReplayConsumer: Failed to process replayed message for client '%s'. Will be retried. Error: %v", clientID, processingErr)
	}
}

func (rc *ReplayConsumer) replaySingleMessageAndWaitForAck(ctx context.Context, msg *kafka.Message) error {
	var eventResp pb.KafkaEventResponse
	if err := proto.Unmarshal(msg.Value, &eventResp); err != nil {
		log.Printf("[ERROR] ReplayConsumer: Unmarshal error on DLQ message. Committing offset to skip. Error: %v", err)
		return nil
	}

	clientID := eventResp.ClientId

	// 1. Check if the client is actually ready to receive replayed messages.
	status, err := redisclient.GetKeyValue(ctx, clientID+redisStatusKeySuffix)
	if err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("failed to get client status from Redis: %w", err)
	}

	if status != REDIS_CLIENT_STATUS_REPLAY {
		return fmt.Errorf("client '%s' is not in REPLAYING state (current status: '%s')", clientID, status)
	}

	// 2. Get the client's current location.
	podChannel, err := redisclient.GetKeyValue(ctx, clientID+redisLocationKeySuffix)
	if err != nil {
		return fmt.Errorf("client '%s' is in REPLAYING state but has no location in Redis: %w", clientID, err)
	}
	eventResp.RedisChannel = podChannel

	// MODIFIED: Populate the Kafka offset from the consumed message.
	// This is the critical piece of information the server pod needs to send the ACK.
	eventResp.KafkaOffset = int64(msg.TopicPartition.Offset)

	// 3. Publish the message (now including the offset) to the pod's specific Redis channel.
	err = redisclient.Publish(ctx, &eventResp)
	if err != nil {
		return fmt.Errorf("failed to publish replayed message to Redis for client '%s': %w", clientID, err)
	}
	log.Printf("[DEBUG] ReplayConsumer: Published message for client '%s' to pod channel '%s'. Now waiting for ACK for offset %d.", clientID, podChannel, eventResp.KafkaOffset)

	// 4. Wait for the gRPC pod to acknowledge delivery by updating a key in Redis.
	ackKey := clientID + redisLastAckedOffsetKeySuffix
	expectedOffset := eventResp.KafkaOffset

	ctxWithTimeout, cancel := context.WithTimeout(ctx, ackTimeout)
	defer cancel()

	for {
		select {
		case <-ctxWithTimeout.Done():
			return fmt.Errorf("timed out waiting for ACK for client '%s' offset '%d'", clientID, expectedOffset)
		default:
			lastAckedStr, err := redisclient.GetKeyValue(ctx, ackKey)
			if err != nil && !errors.Is(err, redis.Nil) {
				return fmt.Errorf("error checking ACK in Redis: %w", err)
			}

			lastAcked, _ := strconv.ParseInt(lastAckedStr, 10, 64)
			if lastAcked >= expectedOffset {
				log.Printf("[INFO] ReplayConsumer: ACK received for client '%s', offset '%d'.", clientID, expectedOffset)
				return nil // SUCCESS! The message was delivered and acknowledged.
			}
			// ACK not yet received, wait a moment before polling again.
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// Shutdown gracefully stops the consumer.
func (rc *ReplayConsumer) Shutdown() {
	log.Printf("[INFO] ReplayConsumer: Initiating shutdown for consumer of topic '%s'...", rc.appConfig.KafkaDLQTopic)
	close(rc.shutdownCh)
	if rc.consumer != nil {
		log.Printf("[INFO] ReplayConsumer: Closing Kafka consumer instance for topic '%s'.", rc.appConfig.KafkaDLQTopic)
		if err := rc.consumer.Close(); err != nil {
			log.Printf("[ERROR] ReplayConsumer: Error closing Kafka consumer for topic '%s': %v", rc.appConfig.KafkaDLQTopic, err)
		}
	}
	log.Printf("[INFO] ReplayConsumer: Shutdown complete for topic '%s'.", rc.appConfig.KafkaDLQTopic)
}
