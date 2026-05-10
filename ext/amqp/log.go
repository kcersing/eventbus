package amqp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
)

var errNoAMQP = errors.New("amqp: publisher not configured")

func logInfo(format string, args ...any) {
	if slog.Default().Enabled(context.Background(), slog.LevelInfo) {
		slog.Info(fmt.Sprintf(format, args...))
	}
}

func logWarn(format string, args ...any) {
	if slog.Default().Enabled(context.Background(), slog.LevelWarn) {
		slog.Warn(fmt.Sprintf(format, args...))
	}
}

func logError(format string, args ...any) {
	if slog.Default().Enabled(context.Background(), slog.LevelError) {
		slog.Error(fmt.Sprintf(format, args...))
	}
}
