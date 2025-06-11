package kafkaconsumer

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/learningfun-dev/NimbusGRPC/nimbus/config"        // Your config package
	"github.com/learningfun-dev/NimbusGRPC/nimbus/kafkaproducer" // Your Kafka producer package
	pb "github.com/learningfun-dev/NimbusGRPC/nimbus/proto"      // Your proto package
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
		"enable.auto.commit": "false", // MODIFIED: Using manual commits for safety.
	}

	c, err := kafka.NewConsumer(consumerConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create EventConsumer: %w", err)
	}

	log.Printf("[INFO] EventConsumer: Subscribing to topic: %s", cfg.KafkaEventsTopic)
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
	log.Printf("[INFO] EventConsumer: Starting to consume from topic '%s'", ec.appConfig.KafkaEventsTopic)

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
				log.Printf("[ERROR] EventConsumer: Error reading message: %v", err)
				if kerr, ok := err.(kafka.Error); ok && kerr.IsFatal() {
					run = false
				}
				continue
			}

			var eventReq pb.KafkaEventReqest
			if err := proto.Unmarshal(msg.Value, &eventReq); err != nil {
				log.Printf("[ERROR] EventConsumer: Failed to unmarshal message. Committing to skip. Error: %v", err)
				if _, commitErr := ec.consumer.CommitMessage(msg); commitErr != nil {
					run = false
				}
				continue
			}

			log.Printf("[INFO] EventConsumer: Processing event for client '%s'", eventReq.ClientId)
			processingErr := ec.processAndProduceResult(&eventReq)

			if processingErr == nil {
				if _, commitErr := ec.consumer.CommitMessage(msg); commitErr != nil {
					log.Printf("[FATAL] EventConsumer: Failed to commit offset after successful processing: %v", commitErr)
					run = false
				}
			} else {
				log.Printf("[ERROR] EventConsumer: Failed to process event for client '%s'. Will retry later. Error: %v", eventReq.ClientId, processingErr)
			}
		}
	}
	log.Printf("[INFO] EventConsumer: Message consumption loop stopped for topic '%s'.", ec.appConfig.KafkaEventsTopic)
}

func (ec *EventConsumer) processAndProduceResult(req *pb.KafkaEventReqest) error {
	var resultValue int32

	switch strings.ToLower(req.EventName) {
	case "sq":
		resultValue = req.Number * req.Number
	default:
		log.Printf("[WARN] EventConsumer: Received unknown event name '%s' for clientID '%s'. Skipping.", req.EventName, req.ClientId)
		return nil // Consider it success, we don't want to retry unknown events.
	}

	resultEvent := &pb.KafkaEventResponse{
		EventName:    req.EventName,
		Number:       req.Number,
		Result:       resultValue,
		ClientId:     req.ClientId,
		RedisChannel: req.RedisChannel, // Pass through the original Redis channel
	}

	// Produce the result to KafkaResultsTopic. This function already retries internally.
	err := kafkaproducer.PublishEventResponse(ec.appConfig.KafkaResultsTopic, resultEvent)
	if err != nil {
		log.Printf("[ERROR] EventConsumer: Failed to produce result to Kafka topic '%s' for client '%s'. Error: %v",
			ec.appConfig.KafkaResultsTopic, req.ClientId, err)
		return err // Return error so we don't commit the original message.
	}

	log.Printf("[INFO] EventConsumer: Successfully produced result to Kafka topic '%s' for clientID '%s'.",
		ec.appConfig.KafkaResultsTopic, req.ClientId)
	return nil // Success!
}

// Shutdown gracefully stops the consumer.
func (ec *EventConsumer) Shutdown() {
	log.Printf("[INFO] EventConsumer: Initiating shutdown for consumer of topic '%s'...", ec.appConfig.KafkaEventsTopic)
	close(ec.shutdownCh)
	if ec.consumer != nil {
		log.Printf("[INFO] EventConsumer: Closing Kafka consumer instance for topic '%s'.", ec.appConfig.KafkaEventsTopic)
		if err := ec.consumer.Close(); err != nil {
			log.Printf("[ERROR] EventConsumer: Error closing Kafka consumer for topic '%s': %v", ec.appConfig.KafkaEventsTopic, err)
		}
	}
	log.Printf("[INFO] EventConsumer: Shutdown complete for topic '%s'.", ec.appConfig.KafkaEventsTopic)
}
