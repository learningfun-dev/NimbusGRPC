package kafkaconsumer

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/learningfun-dev/NimbusGRPC/nimbus/common"
	"github.com/learningfun-dev/NimbusGRPC/nimbus/config"
	"github.com/learningfun-dev/NimbusGRPC/nimbus/kafkaproducer"
	pb "github.com/learningfun-dev/NimbusGRPC/nimbus/proto"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/proto"
)

// EventConsumer consumes events from KafkaEventsTopic, processes them,
// and produces results to KafkaResultsTopic.
type EventConsumer struct {
	consumer   *kafka.Consumer
	appConfig  *config.Config
	wg         *sync.WaitGroup
	shutdownCh chan struct{}
}

// NewEventConsumer creates a new EventConsumer.
func NewEventConsumer(cfg *config.Config, wg *sync.WaitGroup) (*EventConsumer, error) {
	if cfg.KafkaEventsTopic == "" {
		return nil, fmt.Errorf("EventConsumer: KafkaEventsTopic is not configured")
	}
	if cfg.KafkaResultsTopic == "" {
		return nil, fmt.Errorf("EventConsumer: KafkaResultsTopic is not configured")
	}

	consumerConfig := &kafka.ConfigMap{
		"bootstrap.servers":  cfg.KafkaBrokers,
		"group.id":           "nimbus-events-processor-group", // Unique group ID for this consumer
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": "false", // Using manual commits for safety.
	}

	c, err := kafka.NewConsumer(consumerConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create EventConsumer: %w", err)
	}

	log.Info().Str("topic", cfg.KafkaEventsTopic).Msg("EventConsumer: Subscribing to topic")
	err = c.SubscribeTopics([]string{cfg.KafkaEventsTopic}, nil)
	if err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("failed to subscribe EventConsumer to topic %s: %w", cfg.KafkaEventsTopic, err)
	}

	return &EventConsumer{
		consumer:   c,
		appConfig:  cfg,
		wg:         wg,
		shutdownCh: make(chan struct{}),
	}, nil
}

// Start begins the message consumption loop.
func (ec *EventConsumer) Start(ctx context.Context) {
	defer ec.wg.Done()
	log.Info().Str("topic", ec.appConfig.KafkaEventsTopic).Msg("EventConsumer: Starting to consume")

	run := true
	for run {
		select {
		case <-ctx.Done():
			run = false
		case <-ec.shutdownCh:
			run = false
		default:
			msg, err := ec.consumer.ReadMessage(1 * time.Second)
			if err != nil {
				if kerr, ok := err.(kafka.Error); ok && kerr.Code() == kafka.ErrTimedOut {
					continue
				}
				log.Error().Err(err).Msg("EventConsumer: Error reading message")
				if kerr, ok := err.(kafka.Error); ok && kerr.IsFatal() {
					run = false
				}
				continue
			}

			var eventReq pb.KafkaEventReqest
			if err := proto.Unmarshal(msg.Value, &eventReq); err != nil {
				log.Error().Err(err).Msg("EventConsumer: Failed to unmarshal message. Committing to skip.")
				if _, commitErr := ec.consumer.CommitMessage(msg); commitErr != nil {
					log.Fatal().Err(commitErr).Msg("EventConsumer: Failed to commit poison pill message")
					run = false
				}
				continue
			}

			log.Info().Str("clientID", eventReq.ClientId).Msg("EventConsumer: Processing event")
			processingErr := ec.processAndProduceResult(&eventReq)

			if processingErr == nil {
				if _, commitErr := ec.consumer.CommitMessage(msg); commitErr != nil {
					log.Fatal().Err(commitErr).Msg("EventConsumer: Failed to commit offset after successful processing")
					run = false
				}
			} else {
				log.Error().Err(processingErr).Str("clientID", eventReq.ClientId).Msg("EventConsumer: Failed to process event. Will retry later.")
			}
		}
	}
	log.Info().Str("topic", ec.appConfig.KafkaEventsTopic).Msg("EventConsumer: Message consumption loop stopped")
}

func (ec *EventConsumer) processAndProduceResult(req *pb.KafkaEventReqest) error {
	var resultValue int32

	switch strings.ToLower(req.EventName) {
	case "sq":
		resultValue = req.Number * req.Number
	default:
		log.Warn().Str("eventName", req.EventName).Str("clientID", req.ClientId).Msg("EventConsumer: Received unknown event name. Skipping.")
		return nil // Consider it success, we don't want to retry unknown events.
	}

	logEntry := common.Append(req.Log, common.TraceStepInfo{
		ServiceName: "EventConsumer",
		MethodName:  "processAndProduceResult",
		Message:     "Event processed successfully.",
		Metadata: map[string]string{
			"produced_to_topic": ec.appConfig.KafkaResultsTopic,
		},
	})

	resultEvent := &pb.KafkaEventResponse{
		EventName:    req.EventName,
		Number:       req.Number,
		Result:       resultValue,
		ClientId:     req.ClientId,
		RedisChannel: req.RedisChannel, // Pass through the original Redis channel
		Log:          logEntry,         // Pass the updated log along
	}

	// Produce the result to KafkaResultsTopic.
	err := kafkaproducer.PublishEventResponse(ec.appConfig.KafkaResultsTopic, resultEvent)
	if err != nil {
		log.Error().
			Err(err).
			Str("topic", ec.appConfig.KafkaResultsTopic).
			Str("clientID", req.ClientId).
			Msg("EventConsumer: Failed to produce result to Kafka")
		return err // Return error so we don't commit the original message.
	}

	log.Info().
		Str("topic", ec.appConfig.KafkaResultsTopic).
		Str("clientID", req.ClientId).
		Msg("EventConsumer: Successfully produced result to Kafka")
	return nil // Success!
}

// Shutdown gracefully stops the consumer.
func (ec *EventConsumer) Shutdown() {
	topic := "unknown"
	if ec.appConfig != nil {
		topic = ec.appConfig.KafkaEventsTopic
	}
	log.Info().Str("topic", topic).Msg("EventConsumer: Initiating shutdown...")
	close(ec.shutdownCh) // Signal the consumption loop to stop
	if ec.consumer != nil {
		log.Info().Str("topic", topic).Msg("EventConsumer: Closing Kafka consumer instance")
		if err := ec.consumer.Close(); err != nil {
			log.Error().Err(err).Str("topic", topic).Msg("EventConsumer: Error closing Kafka consumer")
		}
	}
	log.Info().Str("topic", topic).Msg("EventConsumer: Shutdown complete.")
}
