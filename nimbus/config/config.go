package config

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
)

// Constants for default configuration values.
const (
	DefaultPort               = 50051
	DefaultRedisAddress       = "localhost:6379"
	DefaultKafkaBrokers       = "localhost:9092"
	DefaultRedisEventsChannel = "events_results_default_pod" // Default channel for Redis events results
	DefaultShutdownTimeout    = 15 * time.Second             // Default ShutdownTimeout
	DefaultKafkaEventsTopic   = "events-v1-topic"            // Default Kafka topic for producer and consumer
	DefaultKafkaResultsTopic  = "results-v1-topic"           // Default Kafka topic for producer and consumer
)

// Config holds all application configuration.
type Config struct {
	Port               int
	RedisAddress       string
	KafkaBrokers       string
	RedisEventsChannel string
	ShutdownTimeout    time.Duration
	KafkaEventsTopic   string
	KafkaResultsTopic  string
}

// Load loads configuration from environment variables, falling back to defaults.
func Load() (*Config, error) {
	err := godotenv.Load() // Tries to load .env from current directory
	if err != nil {
		log.Println("[INFO] No .env file found or error loading .env, using system environment variables or defaults.")
	}
	cfg := &Config{
		Port:               DefaultPort,
		RedisAddress:       DefaultRedisAddress,
		KafkaBrokers:       DefaultKafkaBrokers,
		RedisEventsChannel: DefaultRedisEventsChannel,
		ShutdownTimeout:    DefaultShutdownTimeout,
		KafkaEventsTopic:   DefaultKafkaEventsTopic,
		KafkaResultsTopic:  DefaultKafkaResultsTopic,
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
		cfg.RedisEventsChannel = redisChannel
	}

	// Read Kafka Events topic from environment variable
	if kafkaEventsTopicEnv := os.Getenv("NIMBUS_KAFKA_EVENTS_TOPIC"); kafkaEventsTopicEnv != "" {
		cfg.KafkaEventsTopic = kafkaEventsTopicEnv
	}

	// Read Kafka Results topic from environment variable
	if kafkaResultsTopicEnv := os.Getenv("NIMBUS_KAFKA_RESULTS_TOPIC"); kafkaResultsTopicEnv != "" {
		cfg.KafkaResultsTopic = kafkaResultsTopicEnv
	}

	// Read Shutdown timeout from environment variable
	if shutdownTimeoutEnv := os.Getenv("NIMBUS_SHUTDOWN_TIMEOUT_SEC"); shutdownTimeoutEnv != "" {
		seconds, err := strconv.Atoi(shutdownTimeoutEnv)
		if err != nil {
			// Handle the error, e.g., log it and use a default value
			log.Printf("[WARN] Invalid NIMBUS_SHUTDOWN_TIMEOUT_SEC value '%s': %v. Using default.", shutdownTimeoutEnv, err)
			cfg.ShutdownTimeout = DefaultShutdownTimeout // Or some other default
		} else {
			cfg.ShutdownTimeout = time.Duration(seconds) * time.Second
		}
	} else {
		// Environment variable not set, use a default
		cfg.ShutdownTimeout = DefaultShutdownTimeout
	}

	log.Printf("[INFO] Configuration loaded: Port=%d, RedisAddress=%s, KafkaBrokers=%s, RedisEventsChannel=%s, KafkaEventsTopic=%s, KafkaResultsTopic=%s, ShutdownTimeout=%s",
		cfg.Port, cfg.RedisAddress, cfg.KafkaBrokers, cfg.RedisEventsChannel, cfg.KafkaEventsTopic, cfg.KafkaResultsTopic, cfg.ShutdownTimeout)
	return cfg, nil
}
