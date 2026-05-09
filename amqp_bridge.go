package eventbus

import (
	"context"

	"github.com/cloudwego/kitex/pkg/klog"
	"github.com/kcersing/amqpclt"
)

// ============ AMQP 监听桥接 - 单向职责：从MQ消费事件 ============

// AMQPListener AMQP 事件监听器 - 负责从RabbitMQ监听消息并转发到内存总线
// 职责单一：只从MQ消费，转发到内存总线
// 不拦截内存事件，不自动发送回MQ（避免死循环）
type AMQPListener struct {
	eventBus   *EventBus          // 内存事件总线
	subscriber *amqpclt.Subscribe // AMQP 订阅者
	ctx        context.Context    // 生命周期上下文
	cancel     context.CancelFunc // 取消函数
	done       chan struct{}      // 关闭信号
}

// NewAMQPListener 创建AMQP监听器
func NewAMQPListener(eventBus *EventBus, subscriber *amqpclt.Subscribe) *AMQPListener {
	ctx, cancel := context.WithCancel(context.Background())
	return &AMQPListener{
		eventBus:   eventBus,
		subscriber: subscriber,
		ctx:        ctx,
		cancel:     cancel,
		done:       make(chan struct{}),
	}
}

// StartListener 启动监听 - 从RabbitMQ消费消息并转发到内存总线
// 特点：
//   - 单向流向：MQ → 内存总线
//   - 不拦截内存事件
//   - 标记事件Source为"amqp"，便于追踪
//   - 与程序生命周期同步
func (listener *AMQPListener) StartListener(ctx context.Context) error {
	go func() {
		defer close(listener.done)

		msgCh, cleanup, err := listener.subscriber.Subscribe(listener.ctx)
		if err != nil {
			klog.Errorf("[AMQPListener] failed to subscribe: %v", err)
			return
		}
		defer cleanup()

		klog.Infof("[AMQPListener] started, waiting for messages from RabbitMQ...")

		for {
			select {
			case <-listener.ctx.Done():
				klog.Infof("[AMQPListener] shutdown")
				return

			case msg, ok := <-msgCh:
				if !ok {
					klog.Warn("[AMQPListener] message channel closed")
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
				klog.Infof("[AMQPListener] event forwarded from MQ to memory bus, topic=%s, eventId=%s", event.Topic, event.Id)
			}
		}
	}()

	return nil
}

// Stop 优雅关闭监听
func (listener *AMQPListener) Stop() error {
	listener.cancel()
	<-listener.done
	klog.Infof("[AMQPListener] stopped")
	return nil
}

// ============ 向后兼容：保留AMQPBridge别名 ============

// AMQPBridge 别名 - 为了向后兼容，保留原有名称
// 建议新代码使用 AMQPListener
type AMQPBridge = AMQPListener

// NewAMQPBridge 兼容函数
func NewAMQPBridge(eventBus *EventBus, publisher *amqpclt.Publish, subscriber *amqpclt.Subscribe) *AMQPBridge {
	// 忽略publisher参数，只使用subscriber
	return NewAMQPListener(eventBus, subscriber)
}
