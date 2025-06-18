package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
	"github.com/rs/zerolog/log"
)

// Constants for default configuration values.
const (
	DefaultPort                = 50051
	DefaultRedisAddress        = "localhost:6379"
	DefaultKafkaBrokers        = "localhost:9092"
	DefaultRedisResultsChannel = "events_results_default_pod" // Default channel for Redis events results
	DefaultShutdownTimeout     = 15 * time.Second             // Default ShutdownTimeout
	DefaultKafkaEventsTopic    = "events-v1-topic"            // Default Kafka topic for events topic
	DefaultKafkaResultsTopic   = "results-v1-topic"           // Default Kafka topic for result topic
	DefaultKafkaDLQTopic       = "dlq-v1-topic"               // Default Kafka topic for DLQ
)

// Config holds all application configuration.
type Config struct {
	Port                int
	RedisAddress        string
	KafkaBrokers        string
	RedisResultsChannel string
	ShutdownTimeout     time.Duration
	KafkaEventsTopic    string
	KafkaResultsTopic   string
	KafkaDLQTopic       string
}

// Load loads configuration from environment variables, falling back to defaults.
func Load() (*Config, error) {

	err := godotenv.Load() // Tries to load .env from current directory
	if err != nil {
		log.Info().Msg("No .env file found or error loading .env, using system environment variables or defaults.")
	}

	cfg := &Config{
		Port:                DefaultPort,
		RedisAddress:        DefaultRedisAddress,
		KafkaBrokers:        DefaultKafkaBrokers,
		RedisResultsChannel: DefaultRedisResultsChannel,
		ShutdownTimeout:     DefaultShutdownTimeout,
		KafkaEventsTopic:    DefaultKafkaEventsTopic,
		KafkaResultsTopic:   DefaultKafkaResultsTopic,
		KafkaDLQTopic:       DefaultKafkaDLQTopic,
	}

	// Read Port from environment variable
	if portStr := os.Getenv("NIMBUS_PORT"); portStr != "" {
		port, err := strconv.Atoi(portStr)
		if err != nil {
			return nil, fmt.Errorf("invalid NIMBUS_PORT value '%s': %w", portStr, err)
		}
		cfg.Port = port
	}

	// Read Redis Address from environment variable
	if redisAddr := os.Getenv("NIMBUS_REDIS_ADDRESS"); redisAddr != "" {
		cfg.RedisAddress = redisAddr
	}

	// Read Kafka Brokers from environment variable
	if kafkaBrokers := os.Getenv("NIMBUS_KAFKA_BROKERS"); kafkaBrokers != "" {
		cfg.KafkaBrokers = kafkaBrokers
	}

	// Read Redis Events Channel from environment variable
	if redisChannel := os.Getenv("NIMBUS_REDIS_EVENTS_CHANNEL"); redisChannel != "" {
		cfg.RedisResultsChannel = redisChannel
	}

	// Read Kafka Events topic from environment variable
	if kafkaEventsTopicEnv := os.Getenv("NIMBUS_KAFKA_EVENTS_TOPIC"); kafkaEventsTopicEnv != "" {
		cfg.KafkaEventsTopic = kafkaEventsTopicEnv
	}

	// Read Kafka Results topic from environment variable
	if kafkaResultsTopicEnv := os.Getenv("NIMBUS_KAFKA_RESULTS_TOPIC"); kafkaResultsTopicEnv != "" {
		cfg.KafkaResultsTopic = kafkaResultsTopicEnv
	}

	// Read Kafka DLQ topic from environment variable
	if kafkaDLQTopicEnv := os.Getenv("NIMBUS_KAFKA_DLQ_TOPIC"); kafkaDLQTopicEnv != "" {
		cfg.KafkaDLQTopic = kafkaDLQTopicEnv
	}

	// Read Shutdown timeout from environment variable
	if shutdownTimeoutEnv := os.Getenv("NIMBUS_SHUTDOWN_TIMEOUT_SEC"); shutdownTimeoutEnv != "" {
		seconds, err := strconv.Atoi(shutdownTimeoutEnv)
		if err != nil {
			// Handle the error, e.g., log it and use a default value
			log.Warn().
				Str("value", shutdownTimeoutEnv).
				Err(err).
				Msg("Invalid NIMBUS_SHUTDOWN_TIMEOUT_SEC value. Using default.")
			cfg.ShutdownTimeout = DefaultShutdownTimeout // Or some other default
		} else {
			cfg.ShutdownTimeout = time.Duration(seconds) * time.Second
		}
	} else {
		// Environment variable not set, use a default
		cfg.ShutdownTimeout = DefaultShutdownTimeout
	}

	log.Info().
		Int("port", cfg.Port).
		Str("redis_address", cfg.RedisAddress).
		Str("kafka_brokers", cfg.KafkaBrokers).
		Str("redis_results_channel", cfg.RedisResultsChannel).
		Str("kafka_events_topic", cfg.KafkaEventsTopic).
		Str("kafka_results_topic", cfg.KafkaResultsTopic).
		Str("kafka_dlq_topic", cfg.KafkaDLQTopic).
		Dur("shutdown_timeout", cfg.ShutdownTimeout).
		Msg("Configuration loaded")

	return cfg, nil
}
