package eventbus

import (
	"context"
	"fmt"
	"log/slog"
)

// logInfo / logWarn / logError 是 slog 的格式化封装，供内部使用。
func logInfo(format string, args ...any)  { if slog.Default().Enabled(context.Background(), slog.LevelInfo) { slog.Info(fmt.Sprintf(format, args...)) } }
func logWarn(format string, args ...any)  { if slog.Default().Enabled(context.Background(), slog.LevelWarn) { slog.Warn(fmt.Sprintf(format, args...)) } }
func logError(format string, args ...any) { if slog.Default().Enabled(context.Background(), slog.LevelError) { slog.Error(fmt.Sprintf(format, args...)) } }
