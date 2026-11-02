package amqpclt

import (
	"context"
	"fmt"
	"log/slog"
)

// logWarn 输出 Warn 级别日志；仅在对应级别启用时格式化参数。
func logWarn(ctx context.Context, format string, args ...any) {
	if !slog.Default().Enabled(ctx, slog.LevelWarn) {
		return
	}
	slog.WarnContext(ctx, fmt.Sprintf(format, args...))
}

// logError 输出 Error 级别日志；仅在对应级别启用时格式化参数。
func logError(ctx context.Context, format string, args ...any) {
	if !slog.Default().Enabled(ctx, slog.LevelError) {
		return
	}
	slog.ErrorContext(ctx, fmt.Sprintf(format, args...))
}
