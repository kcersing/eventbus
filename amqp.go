package eventbus

import (
	"context"

	"github.com/kcersing/amqpclt"
)

// AMQPListener RabbitMQ → 内存总线的单向桥接。
// 职责单一：从 MQ 消费消息，转为 Event 后发布到本地 EventBus。
// 不拦截本地事件，不自动回发到 MQ（避免消息循环）。
type AMQPListener struct {
	eventBus   *EventBus          // 内存事件总线
	subscriber *amqpclt.Subscribe // AMQP 订阅者
	cancel     context.CancelFunc // 取消函数
	done       chan struct{}      // 关闭信号
}

// NewAMQPListener 创建 AMQP 监听器。
func NewAMQPListener(eventBus *EventBus, subscriber *amqpclt.Subscribe) *AMQPListener {

	return &AMQPListener{
		eventBus:   eventBus,
		subscriber: subscriber,
		done:       make(chan struct{}),
	}
}

// StartListener 启动监听，从 RabbitMQ 消费消息并转发到本地 EventBus。
// 转发的 Event.Source 标记为 "amqp"，便于区分来源。
func (listener *AMQPListener) StartListener(ctx context.Context) error {
	ctx, listener.cancel = context.WithCancel(ctx)
	go func() {
		defer close(listener.done)

		msgCh, cleanup, err := listener.subscriber.Subscribe(ctx)
		if err != nil {
			logError("[AMQP监听器] 订阅失败: %v", err)
			return
		}
		defer cleanup()

		logInfo("[AMQP监听器] 已启动，等待 RabbitMQ 消息...")

		for {
			select {
			case <-ctx.Done():
				logInfo("[AMQP监听器] 已关闭")
				return

			case msg, ok := <-msgCh:
				if !ok {
					logWarn("[AMQP监听器] 消息通道已关闭")
					return
				}

				// 将 AMQP 消息转换为内存事件
				event := &Event{
					Id:        msg.CorrelationId,
					Topic:     msg.Event,
					Payload:   msg.Payload,
					Timestamp: msg.Timestamp,
					Source:    "amqp", // 标记为来自MQ的事件
					Version:   1,
				}

				// 发布到内存总线让本服务处理
				listener.eventBus.Publish(ctx, event)
				logInfo("[AMQP监听器] 事件已从 MQ 转发到内存总线, topic=%s, eventId=%s", event.Topic, event.Id)
			}
		}
	}()

	return nil
}

// Stop 停止监听，等待内部 goroutine 退出。
func (listener *AMQPListener) Stop() error {
	listener.cancel()
	<-listener.done
	return nil
}
