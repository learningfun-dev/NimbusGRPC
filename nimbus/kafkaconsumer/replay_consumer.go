package kafkaconsumer

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/learningfun-dev/NimbusGRPC/nimbus/config"
	pb "github.com/learningfun-dev/NimbusGRPC/nimbus/proto"
	"github.com/learningfun-dev/NimbusGRPC/nimbus/redisclient"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
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
	processingClients sync.Map
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

	log.Info().Str("topic", cfg.KafkaDLQTopic).Msg("ReplayConsumer: Subscribing to topic")
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
	log.Info().Str("topic", rc.appConfig.KafkaDLQTopic).Msg("ReplayConsumer: Starting to consume")

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
				log.Error().Err(err).Msg("ReplayConsumer: Error reading message")
				if kerr, ok := err.(kafka.Error); ok && kerr.IsFatal() {
					run = false
				}
				continue
			}

			clientID := string(msg.Key)
			if clientID == "" {
				log.Warn().Msg("ReplayConsumer: Message in DLQ has no Key (ClientId). Skipping and committing.")
				if _, commitErr := rc.consumer.CommitMessage(msg); commitErr != nil {
					log.Fatal().Err(commitErr).Msg("ReplayConsumer: CRITICAL - Failed to commit skipped message")
					run = false
				}
				continue
			}

			if _, loaded := rc.processingClients.LoadOrStore(clientID, true); loaded {
				log.Debug().Str("clientID", clientID).Msg("Replay for client is already in progress. Skipping message for now.")
				continue
			}

			// We are not using a goroutine here to avoid the offset commit race condition.
			// This means we have head-of-line blocking per partition, which is a trade-off for correctness for now.
			rc.processMessage(ctx, msg)
		}
	}
	log.Info().Str("topic", rc.appConfig.KafkaDLQTopic).Msg("ReplayConsumer: Message consumption loop stopped")
}

func (rc *ReplayConsumer) processMessage(ctx context.Context, msg *kafka.Message) {
	clientID := string(msg.Key)
	defer rc.processingClients.Delete(clientID)

	processingErr := rc.replaySingleMessageAndWaitForAck(ctx, msg)

	if processingErr == nil {
		if _, commitErr := rc.consumer.CommitMessage(msg); commitErr != nil {
			log.Fatal().Err(commitErr).Str("clientID", clientID).Msg("ReplayConsumer: CRITICAL - Failed to commit offset after ACK")
		}
	} else {
		log.Warn().Err(processingErr).Str("clientID", clientID).Msg("ReplayConsumer: Failed to process replayed message. Will be retried.")
	}
}

func (rc *ReplayConsumer) replaySingleMessageAndWaitForAck(ctx context.Context, msg *kafka.Message) error {
	var eventResp pb.KafkaEventResponse
	if err := proto.Unmarshal(msg.Value, &eventResp); err != nil {
		log.Error().Err(err).Msg("ReplayConsumer: Unmarshal error on DLQ message. Committing offset to skip.")
		return nil
	}

	clientID := eventResp.ClientId

	status, err := redisclient.GetKeyValue(ctx, clientID+redisStatusKeySuffix)
	if err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("failed to get client status from Redis: %w", err)
	}

	if status != REDIS_CLIENT_STATUS_REPLAY {
		return fmt.Errorf("client '%s' is not in REPLAYING state (current status: '%s')", clientID, status)
	}

	podChannel, err := redisclient.GetKeyValue(ctx, clientID+redisLocationKeySuffix)
	if err != nil {
		return fmt.Errorf("client '%s' is in REPLAYING state but has no location in Redis: %w", clientID, err)
	}
	eventResp.RedisChannel = podChannel
	eventResp.KafkaOffset = int64(msg.TopicPartition.Offset)

	err = redisclient.Publish(ctx, &eventResp)
	if err != nil {
		return fmt.Errorf("failed to publish replayed message to Redis for client '%s': %w", clientID, err)
	}
	log.Debug().
		Str("clientID", clientID).
		Str("podChannel", podChannel).
		Int64("offset", eventResp.KafkaOffset).
		Msg("ReplayConsumer: Published message to pod channel. Now waiting for ACK.")

	ackKey := clientID + redisLastAckKeySuffix
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
				log.Info().
					Str("clientID", clientID).
					Int64("offset", expectedOffset).
					Msg("ReplayConsumer: ACK received.")
				return nil // SUCCESS!
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// Shutdown gracefully stops the consumer.
func (rc *ReplayConsumer) Shutdown() {
	topic := "unknown"
	if rc.appConfig != nil {
		topic = rc.appConfig.KafkaDLQTopic
	}
	log.Info().Str("topic", topic).Msg("ReplayConsumer: Initiating shutdown...")
	close(rc.shutdownCh)
	if rc.consumer != nil {
		log.Info().Str("topic", topic).Msg("ReplayConsumer: Closing Kafka consumer instance")
		if err := rc.consumer.Close(); err != nil {
			log.Error().Err(err).Str("topic", topic).Msg("ReplayConsumer: Error closing Kafka consumer")
		}
	}
	log.Info().Str("topic", topic).Msg("ReplayConsumer: Shutdown complete.")
}
