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

// GetPartitionLag calculates the consumer lag for a specific group on a specific topic/partition.
func GetPartitionLag(ctx context.Context, groupID, topic string, partition int32) (int64, error) {
	ac := GetAdminClient()
	ctxWithTimeout, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	result, err := ac.ListConsumerGroupOffsets(ctxWithTimeout, []kafka.ConsumerGroupTopicPartitions{
		{
			Group: groupID,
			Partitions: []kafka.TopicPartition{
				{Topic: &topic, Partition: partition},
			},
		},
	})
	if err != nil {
		return -1, fmt.Errorf("failed to list group offsets: %w", err)
	}

	var committedOffset kafka.Offset = -1
	if len(result.ConsumerGroupsTopicPartitions) > 0 {
		for _, p := range result.ConsumerGroupsTopicPartitions[0].Partitions {
			if p.Topic != nil && *p.Topic == topic && p.Partition == partition {
				committedOffset = p.Offset
				break
			}
		}
	}

	md, err := ac.GetMetadata(nil, false, 5000)
	if err != nil {
		return -1, fmt.Errorf("failed to get cluster metadata to create temp producer: %w", err)
	}
	if len(md.Brokers) == 0 {
		return -1, fmt.Errorf("no brokers found in cluster metadata")
	}

	brokerAddress := fmt.Sprintf("%s:%d", md.Brokers[0].Host, md.Brokers[0].Port)
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": brokerAddress})
	if err != nil {
		return -1, fmt.Errorf("failed to create temporary producer for offset query: %w", err)
	}
	defer p.Close()

	_, high, err := p.QueryWatermarkOffsets(topic, partition, 3000)
	if err != nil {
		return -1, fmt.Errorf("could not query watermark offset: %w", err)
	}

	if int64(committedOffset) < 0 {
		low, _, err := p.QueryWatermarkOffsets(topic, partition, 3000)
		if err != nil {
			return -1, fmt.Errorf("could not query low watermark offset: %w", err)
		}
		return high - low, nil
	}

	lag := high - int64(committedOffset)
	if lag < 0 {
		return 0, nil
	}

	return lag, nil
}
