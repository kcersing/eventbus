package eventbus

// Middleware 中间件类型：接收下一个 Handler，返回包装后的 Handler。
//
// 典型用法（日志中间件）：
//
//	func LoggingMiddleware() Middleware {
//	    return func(next Handler) Handler {
//	        return EventHandlerFunc(func(ctx context.Context, event *Event) error {
//	            slog.Info("before", "topic", event.Topic)
//	            err := next.Handle(ctx, event)
//	            slog.Info("after", "topic", event.Topic, "err", err)
//	            return err
//	        })
//	    }
//	}
type Middleware func(next Handler) Handler
