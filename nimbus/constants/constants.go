package constants

// This file contains shared constants used across the NimbusGRPC application,
// including Redis key suffixes and client status strings.

// --- Redis Key Suffixes ---
// These are appended to a clientID to create specific keys in Redis for state management.
const (
	// RedisStatusKeySuffix is used for the key that stores a client's current status (LIVE, REPLAYING, OFFLINE).
	// Example: "client-123-status"
	RedisStatusKeySuffix = "-status"

	// RedisLocationKeySuffix is used for the key that stores the pod-specific Redis channel a client is connected to.
	// Example: "client-123-location"
	RedisLocationKeySuffix = "-location"

	// RedisTargetOffsetKeySuffix is used for the key that stores the "finish line" Kafka offset for a replay session.
	// Example: "client-123-replay-target-offset"
	RedisTargetOffsetKeySuffix = "-replay-target-offset"

	// RedisLastAckedOffsetKeySuffix is used for the key that stores the last successfully delivered Kafka offset during a replay.
	// Example: "client-123-last-acked-offset"
	RedisLastAckedOffsetKeySuffix = "-last-acked-offset"
)

// --- Redis Status Values ---
// These are the possible values for the client status key in Redis.
const (
	// REDIS_CLIENT_STATUS_LIVE indicates a client is fully online and ready for real-time messages.
	REDIS_CLIENT_STATUS_LIVE = "LIVE"

	// REDIS_CLIENT_STATUS_REPLAY indicates a client has reconnected and is currently replaying missed messages from the DLQ.
	// New messages for this client will be diverted to the DLQ until the status is LIVE again.
	REDIS_CLIENT_STATUS_REPLAY = "REPLAYING"

	// REDIS_CLIENT_STATUS_OFFLINE indicates a client has disconnected.
	// The next time they connect, this status will trigger the replay process.
	REDIS_CLIENT_STATUS_OFFLINE = "OFFLINE"
)
