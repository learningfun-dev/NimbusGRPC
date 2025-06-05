package kafkaproducer

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/learningfun-dev/NimbusGRPC/nimbus/config"   // Your config package
	pb "github.com/learningfun-dev/NimbusGRPC/nimbus/proto" // Your proto package
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
// This should be called once at application startup.
func InitProducer(cfg *config.Config) error {
	producerMutex.Lock()
	defer producerMutex.Unlock()

	if isInitialized {
		log.Println("[WARN] KafkaProducer: Producer already initialized.")
		return nil
	}

	if cfg == nil {
		return fmt.Errorf("kafka producer configuration is nil")
	}
	if cfg.KafkaBrokers == "" {
		return fmt.Errorf("kafka brokers configuration is empty in provided config")
	}
	if cfg.KafkaEventsTopic == "" { // We'll use this as the default for event requests
		return fmt.Errorf("kafka events topic (default for requests) is empty in provided config")
	}

	log.Printf("[INFO] KafkaProducer: Initializing with brokers: %s. Default events topic: %s", cfg.KafkaBrokers, cfg.KafkaEventsTopic)

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": cfg.KafkaBrokers,
		// Add other relevant producer configurations for production here
		// "acks": "all",
		// "retries": 3,
		// "linger.ms": 20,
	})

	if err != nil {
		log.Printf("[FATAL] KafkaProducer: Failed to create Kafka producer: %v", err)
		return fmt.Errorf("failed to create Kafka producer: %w", err)
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
					log.Printf("[ERROR] KafkaProducer: Delivery failed for message to %v: %v", ev.TopicPartition, ev.TopicPartition.Error)
				} else {
					log.Printf("[DEBUG] KafkaProducer: Message delivered to %v (offset %d)", ev.TopicPartition, ev.TopicPartition.Offset)
				}
			case kafka.Error:
				log.Printf("[ERROR] KafkaProducer: Producer error: %v (Code: %d, Fatal: %t)", ev, ev.Code(), ev.IsFatal())
				if ev.IsFatal() {
					log.Printf("[FATAL] KafkaProducer: Fatal producer error encountered: %v.", ev)
				}
			default:
				log.Printf("[INFO] KafkaProducer: Ignored event from producer: %s", ev)
			}
		}
		log.Println("[INFO] KafkaProducer: Delivery report handler goroutine stopped.")
	}()

	log.Println("[INFO] KafkaProducer: Successfully initialized and delivery report handler started.")
	return nil
}

// sendMessageInternal handles the core logic of producing a message to Kafka.
// messagePayload should be a struct that can be marshaled to proto.
func sendMessageInternal(topic string, messagePayload proto.Message) error {
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

	deliveryChan := make(chan kafka.Event, 1) // Buffered channel to avoid blocking Produce if select is slow

	err = currentProducer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          value,
	}, deliveryChan)

	if err != nil {
		close(deliveryChan) // Ensure channel is closed on Produce error
		return fmt.Errorf("failed to enqueue message to Kafka topic %s: %w", topic, err)
	}

	// Wait for delivery report
	select {
	case deliveryEvent := <-deliveryChan:
		m := deliveryEvent.(*kafka.Message)
		if m.TopicPartition.Error != nil {
			return fmt.Errorf("kafka message delivery failed to topic %s: %w", topic, m.TopicPartition.Error)
		}
		log.Printf("[DEBUG] KafkaProducer: Message successfully delivered to topic %s, partition %d, offset %v",
			*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
		return nil
	case <-time.After(defaultDeliveryTimeout):
		return fmt.Errorf("timeout waiting for Kafka delivery confirmation to topic %s", topic)
	}
}

// PublishEventRequest sends a KafkaEventRequest to the specified Kafka topic.
func PublishEventRequest(topic string, req *pb.KafkaEventRequest) error {
	if !isInitialized {
		return fmt.Errorf("kafka producer not initialized, cannot publish event request")
	}
	log.Printf("[DEBUG] KafkaProducer: Publishing EventRequest to topic '%s': %+v", topic, req)
	return sendMessageInternal(topic, req)
}

// PublishEventResponse sends a KafkaEventResponse to the specified Kafka topic.
func PublishEventResponse(topic string, resp *pb.KafkaEventResponse) error {
	if !isInitialized {
		return fmt.Errorf("kafka producer not initialized, cannot publish event response")
	}
	log.Printf("[DEBUG] KafkaProducer: Publishing EventResponse to topic '%s': %+v", topic, resp)
	return sendMessageInternal(topic, resp)
}

// SendEventToDefaultTopic sends a KafkaEventRequest to the default events topic configured during InitProducer.
// This replaces the old SendEventToKafkaTopic.
func SendEventToDefaultTopic(req *pb.KafkaEventRequest) error {
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
		log.Println("[INFO] KafkaProducer: Producer not initialized or already closed.")
		return
	}
	if isProducerClosing {
		producerMutex.Unlock()
		log.Println("[INFO] KafkaProducer: Producer is already in the process of closing.")
		return
	}
	isProducerClosing = true
	p := producer
	producerMutex.Unlock()

	log.Println("[INFO] KafkaProducer: Attempting to flush and close producer...")
	var flushTimeoutMs int = -1 // Default to wait indefinitely for Flush
	if timeout > 0 {
		flushTimeoutMs = int(timeout.Milliseconds())
	}

	numStillInQueue := p.Flush(flushTimeoutMs)
	if numStillInQueue > 0 {
		log.Printf("[WARN] KafkaProducer: %d messages still in queue after flush (timeout: %dms). These messages may be lost.", numStillInQueue, flushTimeoutMs)
	} else {
		log.Printf("[INFO] KafkaProducer: All messages flushed successfully or queue was empty.")
	}

	p.Close()

	producerMutex.Lock()
	isInitialized = false
	producer = nil
	log.Println("[INFO] KafkaProducer: Producer closed.")
	producerMutex.Unlock()
}
