package eventbus

// Middleware 接收下一个 Handler，返回包装后的 Handler。
//
// 示例（日志中间件）：
//
//	func Logging() eventbus.Middleware {
//	    return func(next eventbus.Handler) eventbus.Handler {
//	        return eventbus.HandlerFunc(func(ctx context.Context, e *eventbus.Event) error {
//	            slog.Info("event", "topic", e.Topic)
//	            return next.Handle(ctx, e)
//	        })
//	    }
//	}
type Middleware func(next Handler) Handler
