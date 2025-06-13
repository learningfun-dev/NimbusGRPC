package kafkaadmin

import (
	"context"
	"fmt"
	"hash/fnv"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/learningfun-dev/NimbusGRPC/nimbus/config"
	"github.com/rs/zerolog/log"
)

var (
	adminClient     *kafka.AdminClient
	adminClientOnce sync.Once
	initErr         error
)

// InitAdminClient initializes the singleton Kafka Admin Client
func InitAdminClient(cfg *config.Config) error {
	adminClientOnce.Do(func() {
		log.Info().Msg("KafkaAdmin: Initializing Admin Client...")
		ac, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": cfg.KafkaBrokers})
		if err != nil {
			initErr = fmt.Errorf("failed to create Kafka Admin Client: %w", err)
			log.Fatal().Err(initErr).Msg("KafkaAdmin: Failed to create Kafka Admin Client")
			return
		}
		adminClient = ac
		log.Info().Msg("KafkaAdmin: Admin Client initialized successfully.")
	})
	return initErr
}

// GetAdminClient returns the singleton Kafka Admin Client instance.
func GetAdminClient() *kafka.AdminClient {
	if adminClient == nil {
		log.Fatal().Msg("KafkaAdmin: GetAdminClient called before successful initialization.")
		return nil
	}
	return adminClient
}

// CloseAdminClient closes the Kafka Admin Client connection.
func CloseAdminClient() {
	if adminClient != nil {
		log.Info().Msg("KafkaAdmin: Closing Admin Client...")
		adminClient.Close()
	}
}

// getTopicMetadata retrieves metadata for a specific topic, primarily to get partition count.
func getTopicMetadata(ctx context.Context, topic string) (*kafka.Metadata, error) {
	ac := GetAdminClient()
	return ac.GetMetadata(&topic, false, 5000) // 5-second request timeout
}

// GetPartitionForClientKey determines the target partition for a given key.
// This is necessary for the gRPC server to know which partition's offset to check.
func GetPartitionForClientKey(ctx context.Context, topic string, key string) (int32, error) {
	metadata, err := getTopicMetadata(ctx, topic)
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

// CreateTopics is a utility function to create topics if they don't exist.
func CreateTopics(topics []string) error {
	ac := GetAdminClient()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var kafkaTopics []kafka.TopicSpecification
	for _, topic := range topics {
		kafkaTopics = append(kafkaTopics, kafka.TopicSpecification{
			Topic:             topic,
			NumPartitions:     8, // Defaulting to 8 partitions, can be made configurable
			ReplicationFactor: 1, // Suitable for local testing, should be 3 for production
		})
	}

	results, err := ac.CreateTopics(ctx, kafkaTopics)
	if err != nil {
		return fmt.Errorf("failed to create topics command: %w", err)
	}

	for _, result := range results {
		if result.Error.Code() != kafka.ErrNoError && result.Error.Code() != kafka.ErrTopicAlreadyExists {
			log.Error().Err(result.Error).Str("topic", result.Topic).Msg("Failed to create topic")
			return fmt.Errorf("failed to create topic %s: %v", result.Topic, result.Error)
		}
		log.Info().Str("topic", result.Topic).Str("result", result.Error.String()).Msg("KafkaAdmin: Topic creation result")
	}

	return nil
}
