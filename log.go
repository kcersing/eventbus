package eventbus

import (
	"fmt"
	"log/slog"
)

// logInfo / logWarn / logError 是 slog 的格式化封装，供内部使用。
func logInfo(format string, args ...any)  { slog.Info(fmt.Sprintf(format, args...)) }
func logWarn(format string, args ...any)  { slog.Warn(fmt.Sprintf(format, args...)) }
func logError(format string, args ...any) { slog.Error(fmt.Sprintf(format, args...)) }
