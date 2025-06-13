package kafkaproducer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/learningfun-dev/NimbusGRPC/nimbus/config"
	pb "github.com/learningfun-dev/NimbusGRPC/nimbus/proto"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/proto"
)

const (
	defaultDeliveryTimeout = 10 * time.Second // Timeout for message delivery
)

var (
	producer           *kafka.Producer
	producerMutex      sync.Mutex
	isProducerClosing  bool   = false
	isInitialized      bool   = false
	defaultEventsTopic string // Stores the default topic for KafkaEventRequest
)

// InitProducer initializes the Kafka producer using settings from the provided configuration.
func InitProducer(cfg *config.Config) error {
	producerMutex.Lock()
	defer producerMutex.Unlock()

	if isInitialized {
		log.Warn().Msg("KafkaProducer: Producer already initialized.")
		return nil
	}

	if cfg == nil {
		return fmt.Errorf("kafka producer configuration is nil")
	}
	if cfg.KafkaBrokers == "" {
		return fmt.Errorf("kafka brokers configuration is empty in provided config")
	}
	if cfg.KafkaEventsTopic == "" {
		return fmt.Errorf("kafka events topic (default for requests) is empty in provided config")
	}

	log.Info().
		Str("brokers", cfg.KafkaBrokers).
		Str("defaultTopic", cfg.KafkaEventsTopic).
		Msg("KafkaProducer: Initializing")

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": cfg.KafkaBrokers,
		// Add other relevant producer configurations for production here
	})

	if err != nil {
		err = fmt.Errorf("failed to create Kafka producer: %w", err)
		log.Fatal().Err(err).Msg("KafkaProducer: Failed to create producer")
		return err
	}
	producer = p
	defaultEventsTopic = cfg.KafkaEventsTopic // Store the default topic for requests
	isProducerClosing = false
	isInitialized = true

	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Error().
						Err(ev.TopicPartition.Error).
						Str("key", string(ev.Key)).
						Str("topicPartition", ev.TopicPartition.String()).
						Msg("KafkaProducer: Delivery failed")
				} else {
					log.Debug().
						Str("key", string(ev.Key)).
						Str("topicPartition", ev.TopicPartition.String()).
						Msg("KafkaProducer: Message delivered")
				}
			case kafka.Error:
				log.Error().Err(ev).Msgf("KafkaProducer: Producer error (Code: %d, Fatal: %t)", ev.Code(), ev.IsFatal())
				if ev.IsFatal() {
					log.Fatal().Err(ev).Msg("KafkaProducer: Fatal producer error encountered")
				}
			default:
				log.Info().Interface("event", ev).Msg("KafkaProducer: Ignored event from producer")
			}
		}
		log.Info().Msg("KafkaProducer: Delivery report handler goroutine stopped.")
	}()

	log.Info().Msg("KafkaProducer: Successfully initialized and delivery report handler started.")
	return nil
}

// sendMessageInternal handles the core logic of producing a message to Kafka.
func sendMessageInternal(topic string, messageKey string, messagePayload proto.Message) error {
	producerMutex.Lock()
	if !isInitialized {
		producerMutex.Unlock()
		return fmt.Errorf("kafka producer not initialized")
	}
	if isProducerClosing {
		producerMutex.Unlock()
		return fmt.Errorf("kafka producer is closing, cannot send to topic %s", topic)
	}
	currentProducer := producer
	producerMutex.Unlock()

	value, err := proto.Marshal(messagePayload)
	if err != nil {
		return fmt.Errorf("failed to marshal message payload for topic %s: %w", topic, err)
	}

	deliveryChan := make(chan kafka.Event, 1)

	err = currentProducer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(messageKey),
		Value:          value,
	}, deliveryChan)

	if err != nil {
		close(deliveryChan)
		return fmt.Errorf("failed to enqueue message to Kafka topic %s: %w", topic, err)
	}

	select {
	case deliveryEvent := <-deliveryChan:
		m := deliveryEvent.(*kafka.Message)
		if m.TopicPartition.Error != nil {
			return fmt.Errorf("kafka message delivery failed to topic %s: %w", topic, m.TopicPartition.Error)
		}
		log.Debug().
			Str("key", string(m.Key)).
			Str("topic", *m.TopicPartition.Topic).
			Int32("partition", m.TopicPartition.Partition).
			Stringer("offset", m.TopicPartition.Offset).
			Msg("KafkaProducer: Message successfully delivered")
		return nil
	case <-time.After(defaultDeliveryTimeout):
		return fmt.Errorf("timeout waiting for Kafka delivery confirmation to topic %s for key %s", topic, messageKey)
	}
}

// PublishEventRequest sends a KafkaEventRequest to the specified Kafka topic.
func PublishEventRequest(topic string, req *pb.KafkaEventReqest) error {
	if !isInitialized {
		return fmt.Errorf("kafka producer not initialized, cannot publish event request")
	}
	if req.ClientId == "" {
		return fmt.Errorf("cannot publish event request with empty ClientId as key")
	}
	log.Debug().Str("topic", topic).Str("key", req.ClientId).Msg("KafkaProducer: Publishing EventRequest")
	return sendMessageInternal(topic, req.ClientId, req)
}

// PublishEventResponse sends a KafkaEventResponse to the specified Kafka topic.
func PublishEventResponse(topic string, resp *pb.KafkaEventResponse) error {
	if !isInitialized {
		return fmt.Errorf("kafka producer not initialized, cannot publish event response")
	}
	if resp.ClientId == "" {
		return fmt.Errorf("cannot publish event response with empty ClientId as key")
	}
	log.Debug().Str("topic", topic).Str("key", resp.ClientId).Msg("KafkaProducer: Publishing EventResponse")
	return sendMessageInternal(topic, resp.ClientId, resp)
}

// SendEventToDefaultTopic sends a KafkaEventRequest to the default events topic.
func SendEventToDefaultTopic(req *pb.KafkaEventReqest) error {
	if !isInitialized {
		return fmt.Errorf("kafka producer not initialized, cannot send event to default topic")
	}
	return PublishEventRequest(defaultEventsTopic, req)
}

// CloseProducer flushes pending messages and closes the Kafka producer.
func CloseProducer(timeout time.Duration) {
	producerMutex.Lock()
	if !isInitialized || producer == nil {
		producerMutex.Unlock()
		log.Info().Msg("KafkaProducer: Producer not initialized or already closed.")
		return
	}
	if isProducerClosing {
		producerMutex.Unlock()
		log.Info().Msg("KafkaProducer: Producer is already in the process of closing.")
		return
	}
	isProducerClosing = true
	p := producer
	producerMutex.Unlock()

	log.Info().Msg("KafkaProducer: Attempting to flush and close producer...")
	var flushTimeoutMs int = -1
	if timeout > 0 {
		flushTimeoutMs = int(timeout.Milliseconds())
	}

	numStillInQueue := p.Flush(flushTimeoutMs)
	if numStillInQueue > 0 {
		log.Warn().Int("remaining", numStillInQueue).Msg("KafkaProducer: Messages still in queue after flush timeout. These may be lost.")
	} else {
		log.Info().Msg("KafkaProducer: All messages flushed successfully or queue was empty.")
	}

	p.Close()

	producerMutex.Lock()
	isInitialized = false
	producer = nil
	log.Info().Msg("KafkaProducer: Producer closed.")
	producerMutex.Unlock()
}

// GetPartitionEndOffset gets the current high watermark (end offset).
func GetPartitionEndOffset(ctx context.Context, topic string, partition int32) (int64, error) {
	producerMutex.Lock()
	if !isInitialized {
		producerMutex.Unlock()
		return -1, fmt.Errorf("kafka producer not initialized")
	}
	currentProducer := producer
	producerMutex.Unlock()

	timeoutMs := 3000
	_, high, err := currentProducer.QueryWatermarkOffsets(topic, partition, timeoutMs)
	if err != nil {
		return -1, fmt.Errorf("could not query watermark offset for topic %s partition %d: %w", topic, partition, err)
	}

	return high, nil
}
