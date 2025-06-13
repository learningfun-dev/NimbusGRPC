package kafkaconsumer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/learningfun-dev/NimbusGRPC/nimbus/config"
	"github.com/learningfun-dev/NimbusGRPC/nimbus/constants"
	"github.com/learningfun-dev/NimbusGRPC/nimbus/kafkaproducer"
	pb "github.com/learningfun-dev/NimbusGRPC/nimbus/proto"
	"github.com/learningfun-dev/NimbusGRPC/nimbus/redisclient"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/proto"
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

	log.Info().Str("topic", cfg.KafkaResultsTopic).Msg("ResultConsumer: Subscribing to topic")
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
	log.Info().Str("topic", rc.appConfig.KafkaResultsTopic).Msg("ResultConsumer: Starting to consume")

	run := true
	for run {
		select {
		case <-ctx.Done():
			run = false
		case <-rc.shutdownCh:
			run = false
		default:
			msg, err := rc.consumer.ReadMessage(1 * time.Second) // Poll with timeout
			if err != nil {
				if kerr, ok := err.(kafka.Error); ok && kerr.Code() == kafka.ErrTimedOut {
					continue // This is an expected timeout when no new messages are available.
				}
				log.Error().Err(err).Str("topic", rc.appConfig.KafkaResultsTopic).Msg("ResultConsumer: Error reading message")
				if kerr, ok := err.(kafka.Error); ok && kerr.IsFatal() {
					log.Fatal().Err(err).Str("topic", rc.appConfig.KafkaResultsTopic).Msg("ResultConsumer: Fatal error encountered, shutting down.")
					run = false
				}
				continue
			}

			var eventResp pb.KafkaEventResponse
			if err := proto.Unmarshal(msg.Value, &eventResp); err != nil {
				log.Error().Err(err).Str("topic", *msg.TopicPartition.Topic).Msg("ResultConsumer: Failed to unmarshal message. Committing to skip.")
				if _, commitErr := rc.consumer.CommitMessage(msg); commitErr != nil {
					log.Fatal().Err(commitErr).Msg("ResultConsumer: Failed to commit poison pill message offset")
					run = false
				}
				continue
			}

			log.Info().Str("clientID", eventResp.ClientId).Str("topic", *msg.TopicPartition.Topic).Msg("ResultConsumer: Processing result")
			processingErr := rc.routeOrDivertResult(ctx, &eventResp)

			if processingErr == nil {
				if _, commitErr := rc.consumer.CommitMessage(msg); commitErr != nil {
					log.Fatal().Err(commitErr).Msg("ResultConsumer: Failed to commit offset after successful processing")
					run = false
				}
			} else {
				log.Error().Err(processingErr).Str("clientID", eventResp.ClientId).Msg("ResultConsumer: Failed to process message. Will retry later.")
			}
		}
	}
	log.Info().Str("topic", rc.appConfig.KafkaResultsTopic).Msg("ResultConsumer: Message consumption loop stopped")
}

// routeOrDivertResult implements the core routing logic, now with location validation.
func (rc *ResultConsumer) routeOrDivertResult(ctx context.Context, resp *pb.KafkaEventResponse) error {
	if resp.ClientId == "" {
		log.Warn().Msg("ResultConsumer: Received a result with an empty ClientId. Discarding message.")
		return nil // Discarding is considered a successful operation for this message.
	}

	statusKey := resp.ClientId + "-status"
	clientStatus, err := redisclient.GetKeyValue(ctx, statusKey)
	if err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("could not get client status from Redis: %w", err)
	}

	if clientStatus != constants.REDIS_CLIENT_STATUS_LIVE {
		log.Info().
			Str("clientID", resp.ClientId).
			Str("status", clientStatus).
			Msg("Client status is not LIVE. Diverting result to DLQ.")
		return rc.publishToDLQ(ctx, resp)
	}

	locationKey := resp.ClientId + "-location"
	currentLocation, err := redisclient.GetKeyValue(ctx, locationKey)
	if err != nil {
		log.Warn().
			Err(err).
			Str("clientID", resp.ClientId).
			Msg("Inconsistency: Status is LIVE but no location key found. Diverting to DLQ.")
		return rc.publishToDLQ(ctx, resp)
	}

	if currentLocation != resp.RedisChannel {
		log.Info().
			Str("clientID", resp.ClientId).
			Str("currentLocation", currentLocation).
			Str("staleChannel", resp.RedisChannel).
			Msg("Stale message detected. Diverting to DLQ.")
		return rc.publishToDLQ(ctx, resp)
	}

	log.Info().
		Str("clientID", resp.ClientId).
		Str("location", currentLocation).
		Msg("Client is LIVE at correct location. Publishing result to Redis.")
	return rc.publishToRedis(ctx, resp)
}

func (rc *ResultConsumer) publishToDLQ(ctx context.Context, resp *pb.KafkaEventResponse) error {
	err := kafkaproducer.PublishEventResponse(rc.appConfig.KafkaDLQTopic, resp)
	if err != nil {
		log.Error().
			Err(err).
			Str("clientID", resp.ClientId).
			Str("topic", rc.appConfig.KafkaDLQTopic).
			Msg("CRITICAL: FAILED TO PUBLISH TO DLQ. Will be retried.")
		return err
	}
	log.Debug().
		Str("clientID", resp.ClientId).
		Str("topic", rc.appConfig.KafkaDLQTopic).
		Msg("Successfully published result to DLQ topic.")
	return nil
}

func (rc *ResultConsumer) publishToRedis(ctx context.Context, resp *pb.KafkaEventResponse) error {
	if resp.RedisChannel == "" {
		log.Warn().
			Str("clientID", resp.ClientId).
			Msg("Cannot publish to Redis because RedisChannel is empty. Diverting to DLQ.")
		return rc.publishToDLQ(ctx, resp)
	}
	err := redisclient.Publish(ctx, resp)
	if err != nil {
		log.Error().
			Err(err).
			Str("clientID", resp.ClientId).
			Str("channel", resp.RedisChannel).
			Msg("Failed to publish result to Redis channel. Will be retried.")
		return err
	}
	return nil
}

// Shutdown gracefully stops the consumer.
func (rc *ResultConsumer) Shutdown() {
	topic := "unknown"
	if rc.appConfig != nil {
		topic = rc.appConfig.KafkaResultsTopic
	}
	log.Info().Str("topic", topic).Msg("ResultConsumer: Initiating shutdown...")
	close(rc.shutdownCh)
	if rc.consumer != nil {
		log.Info().Str("topic", topic).Msg("ResultConsumer: Closing Kafka consumer instance")
		if err := rc.consumer.Close(); err != nil {
			log.Error().Err(err).Str("topic", topic).Msg("ResultConsumer: Error closing Kafka consumer")
		}
	}
	log.Info().Str("topic", topic).Msg("ResultConsumer: Shutdown complete.")
}
