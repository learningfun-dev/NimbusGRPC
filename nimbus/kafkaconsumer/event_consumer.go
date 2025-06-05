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
		"bootstrap.servers": cfg.KafkaBrokers,
		"group.id":          "nimbus-events-processor-group", // Unique group ID for this consumer
		"auto.offset.reset": "earliest",
		// "enable.auto.commit": false, // Consider manual commits for more control
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
		case <-ctx.Done(): // Context canceled, initiate shutdown
			log.Printf("[INFO] EventConsumer: Context cancellation received. Shutting down consumer for topic '%s'.", ec.appConfig.KafkaEventsTopic)
			run = false
		case <-ec.shutdownCh: // Explicit shutdown signal
			log.Printf("[INFO] EventConsumer: Shutdown signal received. Stopping consumer for topic '%s'.", ec.appConfig.KafkaEventsTopic)
			run = false
		default:
			// Poll for messages with a timeout to allow checking shutdown conditions.
			msg, err := ec.consumer.ReadMessage(1 * time.Second)
			if err != nil {
				// Check if it's a timeout error, which is expected.
				if kerr, ok := err.(kafka.Error); ok && kerr.Code() == kafka.ErrTimedOut {
					continue // No message, continue loop
				}
				// For other errors, log and potentially break or implement retry logic.
				log.Printf("[ERROR] EventConsumer: Error reading message from topic '%s': %v", ec.appConfig.KafkaEventsTopic, err)
				// Depending on the error, you might want to stop or implement a backoff.
				// If it's a fatal error for the consumer, run = false
				if kerr, ok := err.(kafka.Error); ok && kerr.IsFatal() {
					log.Printf("[FATAL] EventConsumer: Fatal error encountered for topic '%s'. Shutting down.", ec.appConfig.KafkaEventsTopic)
					run = false
				}
				continue
			}

			log.Printf("[DEBUG] EventConsumer: Received message on topic '%s': Partition=%d, Offset=%d, Key=%s",
				*msg.TopicPartition.Topic, msg.TopicPartition.Partition, msg.TopicPartition.Offset, string(msg.Key))

			var eventReq pb.KafkaEventRequest
			if err := proto.Unmarshal(msg.Value, &eventReq); err != nil {
				log.Printf("[ERROR] EventConsumer: Failed to unmarshal KafkaEventRequest from topic '%s': %v. Message: %s",
					*msg.TopicPartition.Topic, err, string(msg.Value))
				// Consider sending to a dead-letter queue or logging for manual inspection.
				continue
			}

			log.Printf("[INFO] EventConsumer: Processing event: %+v from topic '%s'", eventReq, *msg.TopicPartition.Topic)
			ec.processAndProduceResult(&eventReq)

			// If using manual commits:
			// if _, err := ec.consumer.CommitMessage(msg); err != nil {
			//     log.Printf("[ERROR] EventConsumer: Failed to commit offset for message on topic '%s': %v", *msg.TopicPartition.Topic, err)
			// }
		}
	}
	log.Printf("[INFO] EventConsumer: Message consumption loop stopped for topic '%s'.", ec.appConfig.KafkaEventsTopic)
}

func (ec *EventConsumer) processAndProduceResult(req *pb.KafkaEventRequest) {
	var resultValue int32

	switch strings.ToLower(req.EventName) {
	case "sq":
		resultValue = req.Number * req.Number
	default:
		log.Printf("[WARN] EventConsumer: Received unknown event name '%s' for clientID '%s'. Skipping.", req.EventName, req.ClientId)
		// Optionally produce an error message to the results topic or a specific error topic.
		return
	}

	resultEvent := &pb.KafkaEventResponse{
		EventName:    req.EventName,
		Number:       req.Number,
		Result:       resultValue,
		ClientId:     req.ClientId,
		RedisChannel: req.RedisChannel, // Pass through the original Redis channel
	}

	// Produce the result to KafkaResultsTopic
	err := kafkaproducer.PublishEventResponse(ec.appConfig.KafkaResultsTopic, resultEvent)
	if err != nil {
		log.Printf("[ERROR] EventConsumer: Failed to produce result to Kafka topic '%s' for clientID '%s': %v. Result: %+v",
			ec.appConfig.KafkaResultsTopic, req.ClientId, err, resultEvent)
		// Implement retry logic or dead-letter queue for failed productions.
	} else {
		log.Printf("[INFO] EventConsumer: Successfully produced result to Kafka topic '%s' for clientID '%s'. Result: %+v",
			ec.appConfig.KafkaResultsTopic, req.ClientId, resultEvent)
	}
}

// Shutdown gracefully stops the consumer.
func (ec *EventConsumer) Shutdown() {
	log.Printf("[INFO] EventConsumer: Initiating shutdown for consumer of topic '%s'...", ec.appConfig.KafkaEventsTopic)
	close(ec.shutdownCh) // Signal the consumption loop to stop
	// The consumer.Close() will be called by the main application's defer or explicit shutdown logic.
	// However, for a cleaner shutdown of this specific component:
	if ec.consumer != nil {
		log.Printf("[INFO] EventConsumer: Closing Kafka consumer instance for topic '%s'.", ec.appConfig.KafkaEventsTopic)
		if err := ec.consumer.Close(); err != nil {
			log.Printf("[ERROR] EventConsumer: Error closing Kafka consumer for topic '%s': %v", ec.appConfig.KafkaEventsTopic, err)
		}
	}
	log.Printf("[INFO] EventConsumer: Shutdown complete for topic '%s'.", ec.appConfig.KafkaEventsTopic)
}
