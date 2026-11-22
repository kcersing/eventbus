package amqpclt

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
)

// captureSlog 将默认 logger 临时替换为写入 buf 的 TextHandler。
func captureSlog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(old) })
	return &buf
}

func TestLogWarn(t *testing.T) {
	buf := captureSlog(t)
	logWarn(context.Background(), "batch message %d: %v", 3, "boom")

	out := buf.String()
	if !strings.Contains(out, "level=WARN") {
		t.Fatalf("want WARN level, got %q", out)
	}
	if !strings.Contains(out, "batch message 3: boom") {
		t.Fatalf("want formatted message, got %q", out)
	}
}

func TestLogError(t *testing.T) {
	buf := captureSlog(t)
	logError(context.Background(), "cannot unmarshal message body: %v", "bad json")

	out := buf.String()
	if !strings.Contains(out, "level=ERROR") {
		t.Fatalf("want ERROR level, got %q", out)
	}
	if !strings.Contains(out, "cannot unmarshal message body: bad json") {
		t.Fatalf("want formatted message, got %q", out)
	}
}

// 日志级别关闭时不应格式化/输出，保持与原 Enabled 检查一致的语义。
func TestLogRespectsLevel(t *testing.T) {
	var buf bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelError})))
	t.Cleanup(func() { slog.SetDefault(old) })

	logWarn(context.Background(), "should be filtered")
	if buf.Len() != 0 {
		t.Fatalf("warn should be filtered at error level, got %q", buf.String())
	}

	logError(context.Background(), "should appear")
	if !strings.Contains(buf.String(), "should appear") {
		t.Fatalf("error should appear even at error level, got %q", buf.String())
	}
}
