package kafkaadmin

import (
	"context"
	"fmt"
	"hash/fnv"
	"log"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/learningfun-dev/NimbusGRPC/nimbus/config"
)

var (
	adminClient     *kafka.AdminClient
	adminClientOnce sync.Once
	initErr         error
)

// InitAdminClient initializes the singleton Kafka Admin Client
func InitAdminClient(cfg *config.Config) error {
	adminClientOnce.Do(func() {
		log.Println("[INFO] KafkaAdmin: Initializing Admin Client...")
		ac, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": cfg.KafkaBrokers})
		if err != nil {
			initErr = fmt.Errorf("failed to create Kafka Admin Client: %w", err)
			log.Printf("[FATAL] KafkaAdmin: %v", initErr)
			return
		}
		adminClient = ac
		log.Println("[INFO] KafkaAdmin: Admin Client initialized successfully.")
	})
	return initErr
}

// GetAdminClient returns the singleton Kafka Admin Client instance.
func GetAdminClient() *kafka.AdminClient {
	if adminClient == nil {
		log.Fatal("[FATAL] KafkaAdmin: GetAdminClient called before successful initialization.")
		return nil
	}
	return adminClient
}

// CloseAdminClient closes the Kafka Admin Client connection.
func CloseAdminClient() {
	if adminClient != nil {
		log.Println("[INFO] KafkaAdmin: Closing Admin Client...")
		adminClient.Close()
	}
}

// getTopicMetadata retrieves metadata for a specific topic, primarily to get partition count.
func getTopicMetadata(ctx context.Context, topic string, ac *kafka.AdminClient) (*kafka.Metadata, error) {
	_, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	return ac.GetMetadata(&topic, false, 5000) // 5-second request timeout
}

// GetPartitionForClientKey determines the target partition for a given key.
// This is necessary for the gRPC server to know which partition's offset to check.
func GetPartitionForClientKey(ctx context.Context, topic string, key string) (int32, error) {
	ac := GetAdminClient()
	metadata, err := getTopicMetadata(ctx, topic, ac)
	if err != nil {
		return -1, fmt.Errorf("could not get metadata for topic %s: %w", topic, err)
	}

	topicMetadata, ok := metadata.Topics[topic]
	if !ok || topicMetadata.Error.Code() != kafka.ErrNoError {
		return -1, fmt.Errorf("error in topic metadata for %s: %w", topic, topicMetadata.Error)
	}

	numPartitions := int32(len(topicMetadata.Partitions))
	if numPartitions == 0 {
		return -1, fmt.Errorf("topic %s has zero partitions", topic)
	}

	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(key))
	partition := int32(hasher.Sum32()&0x7fffffff) % numPartitions

	return partition, nil
}

func CreateTopics(topics []string) error {
	ac := GetAdminClient()
	kafkaTopics := []kafka.TopicSpecification{}

	for _, topic := range topics {
		kafkaTopics = append(kafkaTopics, kafka.TopicSpecification{
			Topic:         topic,
			NumPartitions: 8,
		})
	}
	ctx := context.Background()
	result, err := ac.CreateTopics(ctx, kafkaTopics)
	if err != nil {
		log.Printf("[ERROR] KafkaAdmin: could not create topics %v: %w", topics, err)
		return fmt.Errorf("KafkaAdmin: could not create topics %v: %w", topics, err)
	}
	log.Printf("[INFO] KafkaAdmin: Topics creation results %v", result)
	return nil
}
