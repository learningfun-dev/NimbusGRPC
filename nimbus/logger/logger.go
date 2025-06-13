package logger

import (
	"os"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// Init configures the global zerolog logger with a pretty console writer
// and sets the global log level. Call this once at the start of your main() function.
func Init() {
	// Use ConsoleWriter for a human-friendly, colored output during development.
	// The default output is JSON, which is ideal for production environments.
	log.Logger = log.Output(zerolog.ConsoleWriter{
		Out:        os.Stderr,
		TimeFormat: time.RFC3339,
	})

	// You can set a global log level. For example, to see Debug messages:
	// zerolog.SetGlobalLevel(zerolog.DebugLevel)
	// For production, you would typically set it to InfoLevel or WarnLevel,
	// potentially read from your config.
	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	log.Info().Msg("Global logger initialized.")
}
