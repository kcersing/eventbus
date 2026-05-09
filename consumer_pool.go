package eventbus

import (
	"context"
	"sync"
	"time"

	"github.com/cloudwego/kitex/pkg/klog"
)

// FailHandler 定义了当事件无法入队时的处理函数
type FailHandler func(event *Event, err error)

// PoolOptions 定义了消费者池的配置选项
type PoolOptions struct {
	QueueSize      int
	HandlerTimeout time.Duration
	FailHandler    FailHandler
}

// WithQueueSize 设置消费者池的队列大小
func WithQueueSize(size int) func(*PoolOptions) {
	return func(o *PoolOptions) {
		o.QueueSize = size
	}
}

// WithHandlerTimeout 设置处理器的超时时间
func WithHandlerTimeout(timeout time.Duration) func(*PoolOptions) {
	return func(o *PoolOptions) {
		o.HandlerTimeout = timeout
	}
}

// WithFailHandler 设置自定义的失败处理器
func WithFailHandler(handler FailHandler) func(*PoolOptions) {
	return func(o *PoolOptions) {
		o.FailHandler = handler
	}
}

// ConsumerPool 消费者池 - 用于高吞吐场景
type ConsumerPool struct {
	name      string             // 消费者池的名字（用于日志/识别）
	handler   Handler            // 事件处理器（处理每个事件）
	workerNum int32              // 工作线程数量（并发度）
	queue     chan *Event        // 事件队列（缓冲通道）
	wg        sync.WaitGroup     // 等待组（确保优雅关闭）
	ctx       context.Context    // 上下文（用于控制）
	cancel    context.CancelFunc // 取消函数（停止所有 worker）
	options   PoolOptions        // 池的配置选项
}

// NewConsumerPool 创建消费者池
func NewConsumerPool(name string, handler Handler, workerNum int32, opts ...func(*PoolOptions)) *ConsumerPool {
	// 默认选项
	options := PoolOptions{
		QueueSize:      DefaultConfig.QueueSize,
		HandlerTimeout: DefaultConfig.HandlerTimeout,
		FailHandler: func(event *Event, err error) { // 默认失败处理器
			klog.Warnf("警告: 消费者池 %s 队列已满或已关闭，丢弃事件. Topic: %s", name, event.Topic)
		},
	}
	// 应用用户提供的选项
	for _, opt := range opts {
		opt(&options)
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &ConsumerPool{
		name:      name,
		handler:   handler,
		workerNum: workerNum,
		queue:     make(chan *Event, options.QueueSize),
		ctx:       ctx,
		cancel:    cancel,
		options:   options,
	}
}

// Start 启动消费者池
func (cp *ConsumerPool) Start() {
	for i := int32(0); i < cp.workerNum; i++ {
		cp.wg.Add(1)
		go cp.worker()
	}
}

func (cp *ConsumerPool) worker() {
	defer cp.wg.Done()
	for {
		select {
		case <-cp.ctx.Done():
			return
		case event, ok := <-cp.queue:
			if !ok {
				return
			}
			func() {
				defer func() {
					if r := recover(); r != nil {
						klog.Errorf("[Pool Recover] pool=%s panic: %v", cp.name, r)
					}
				}()

				handlerCtx := cp.ctx
				if cp.options.HandlerTimeout > 0 {
					var cancel context.CancelFunc
					handlerCtx, cancel = context.WithTimeout(cp.ctx, cp.options.HandlerTimeout)
					defer cancel()
				}
				if err := cp.handler.Handle(handlerCtx, event); err != nil {

					klog.Errorf("[Pool Handler Error] pool=%s error: %v event:%s", cp.name, err, event)
				}
			}()
		}
	}
}

// Consume 消费事件
func (cp *ConsumerPool) Consume(event *Event) {
	select {
	case <-cp.ctx.Done():
		cp.options.FailHandler(event, context.Canceled)
		return
	case cp.queue <- event:
		// 写入成功
	default:
		cp.options.FailHandler(event, ErrQueueFull)
	}
}

// Stop 停止消费者池
func (cp *ConsumerPool) Stop() {
	cp.cancel()
	cp.wg.Wait()
}
