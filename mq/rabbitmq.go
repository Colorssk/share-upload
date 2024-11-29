package rabbitmq

import (
	"fmt"
	"github.com/streadway/amqp"
	"gopkg.in/ini.v1"
	"log"
	"time"

	"sync"
)

// rabbitMQ结构体
type RabbitMQ struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	//队列名称
	QueueName string
	//交换机名称
	Exchange string
	//bind Key 名称
	Key string
	//连接信息
	Mqurl      string
	MaxRetries int
	sync.Mutex
}

// 创建结构体实例
func NewRabbitMQ(queueName string, exchange string, key string, cfg *ini.File) *RabbitMQ {
	return &RabbitMQ{QueueName: queueName, Exchange: exchange, Key: key, Mqurl: cfg.Section("rabbitmq").Key("uri").String()}
}

// 断开channel 和 connection
func (r *RabbitMQ) Destory() {
	r.channel.Close()
	r.conn.Close()
}

// 错误处理函数
func (r *RabbitMQ) failOnErr(err error, message string) {
	if err != nil {
		log.Fatalf("%s:%s", message, err)
		panic(fmt.Sprintf("%s:%s", message, err))
	}
}

// 创建简单模式下RabbitMQ实例
func NewRabbitMQTopic(exchangeName string, routineKey string, cfg *ini.File) *RabbitMQ {
	//创建RabbitMQ实例
	rabbitmq := NewRabbitMQ("", exchangeName, routineKey, cfg)
	var err error
	//获取connection
	rabbitmq.conn, err = amqp.Dial(rabbitmq.Mqurl)
	rabbitmq.failOnErr(err, "failed to connect rabb"+
		"itmq!")
	//获取channel
	rabbitmq.channel, err = rabbitmq.conn.Channel()
	rabbitmq.failOnErr(err, "failed to open a channel")
	return rabbitmq
}

// topic模式, 生产者
func (r *RabbitMQ) PublishTopic(message string) error {
	r.Lock()
	defer r.Unlock()
	//1.申请队列，如果队列不存在会自动创建，存在则跳过创建
	err := r.channel.ExchangeDeclare(
		r.Exchange,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		r.failOnErr(err, "topic模式尝试创建exchange失败。")
		return err
	}
	//调用channel 发送消息到队列中
	err = r.channel.Publish(
		r.Exchange,
		r.Key,
		//如果为true，根据自身exchange类型和routekey规则无法找到符合条件的队列会把消息返还给发送者
		false,
		//如果为true，当exchange发送消息到队列后发现队列上没有消费者，则会把消息返还给发送者
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message),
		})
	if err != nil {
		r.failOnErr(err, "topic模式发送消息到队列失败。")
	}
	return nil
}

// topic模式, 消费者 "*"表示匹配一个单词。“#”表示匹配多个单词，亦可以是0个
func (r *RabbitMQ) ConsumeTopic(fn func(message string)) {
	//创建交换机，如果不存在会自动创建，存在则跳过创建
	err := r.channel.ExchangeDeclare(
		r.Exchange,
		"topic",
		true, //这里需要是true
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		r.failOnErr(err, "topic模式，消费者创建exchange失败。")
		fmt.Println(err)
	}
	// 创建队列，无需写队列名称
	q, err := r.channel.QueueDeclare(
		"",
		false,
		false,
		true,
		false,
		nil,
	)
	//消费者流控
	r.channel.Qos(
		1,     //当前消费者一次能接受的最大消息数量
		0,     //服务器传递的最大容量（以八位字节为单位）
		false, //如果设置为true 对channel可用
	)
	//将队列绑定到交换机里。
	err = r.channel.QueueBind(
		q.Name,
		r.Key,
		r.Exchange,
		false,
		nil,
	)

	//接收消息
	msgs, err := r.channel.Consume(
		q.Name, // queue
		//用来区分多个消费者
		"", // consumer
		//是否自动应答
		//这里要改掉，我们用手动应答
		false, // auto-ack
		//是否独有
		false, // exclusive
		//设置为true，表示 不能将同一个Conenction中生产者发送的消息传递给这个Connection中 的消费者
		false, // no-local
		//列是否阻塞
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		fmt.Println(err)
	}

	forever := make(chan bool)
	//启用协程处理消息
	go func() {
		for d := range msgs {
			//消息逻辑处理，可以自行设计逻辑
			log.Printf("Received a message: %s", d.Body)
			// 业务逻辑消费 package 变动的消息,例如执行websocker的write message
			fn(string([]byte(d.Body)))
			//为false表示确认当前消息
			d.Ack(false)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever

}

// 创建工作队列模式下RabbitMQ实例
func NewRabbitMQWorkQueue(queueName string, cfg *ini.File) *RabbitMQ {
	rabbitmq := NewRabbitMQ(queueName, "", "", cfg)
	var err error
	rabbitmq.conn, err = amqp.Dial(rabbitmq.Mqurl)
	rabbitmq.failOnErr(err, "failed to connect rabbitmq!")
	rabbitmq.channel, err = rabbitmq.conn.Channel()
	rabbitmq.failOnErr(err, "failed to open a channel")
	return rabbitmq
}

// 发送消息到队列 - 工作队列模式 项目中存储的需要合并的文件列表, "租户|fileName_1|fileName_2....&合并之后文件名称"
func (r *RabbitMQ) PublishWorkQueue(message string) error {
	r.Lock()
	defer r.Unlock()

	_, err := r.channel.QueueDeclare(
		r.QueueName,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		r.failOnErr(err, "申请队列失败。")
		return err
	}

	err = r.channel.Publish(
		"",
		r.QueueName,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message),
		})
	if err != nil {
		r.failOnErr(err, "发送消息到队列失败。")
	}
	return nil
}

// 从队列中消费消息 - 工作队列模式
func (r *RabbitMQ) ConsumeWorkQueue(processMessage func([]byte) error) {
	r.channel.Qos(1, 0, false)
	_, err := r.channel.QueueDeclare(
		r.QueueName,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		r.failOnErr(err, "申请队列失败。")
	}

	err = r.channel.Qos(
		1,
		0,
		false,
	)
	if err != nil {
		r.failOnErr(err, "设置消费者流控失败。")
	}

	msgs, err := r.channel.Consume(
		r.QueueName,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		r.failOnErr(err, "无法从队列中消费消息。")
	}

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)
			if err := processMessage(d.Body); err != nil {
				// 处理消息异常
				// 从消息中获取重试次数
				retries, ok := d.Headers["x-retries"].(int)
				if !ok {
					retries = 0
				}
				fmt.Println("处理消息失败，正在准备重试")
				// 检查重试次数是否达到最大值
				if retries >= r.MaxRetries {
					log.Printf("Max retries reached, sending message to dead letter queue: %s", d.Body)
					// 将消息发送到死信队列
					err := r.channel.Publish(
						"",                  // 交换机
						"dead_letter_queue", // 死信队列名称
						false,
						false,
						amqp.Publishing{
							ContentType: "text/plain",
							Body:        d.Body,
						},
					)
					if err != nil {
						// 引入日志可以把当前错误信息， 记录
						fmt.Println("消息发送死信失败", err)
					} else {
						fmt.Println("已发送死信队列")
						// 成功消息发送后， 就删除消息
						d.Ack(false)
					}

				} else {
					// 重新处理消息
					// 使用 Nack 将消息返回到队列，但不要立即重新排队
					d.Nack(false, false)
					// 更新重试次数并重新尝试处理消息
					retries++
					d.Headers["x-retries"] = retries
					fmt.Println("开始重新处理消息")
					time.Sleep(10 * time.Second)
					// 重新发送消息到队列
					err := r.channel.Publish(
						"",          // 交换机
						r.QueueName, // 队列名称
						false,
						false,
						amqp.Publishing{
							ContentType: "text/plain",
							Body:        d.Body,
							Headers:     d.Headers,
						},
					)
					if err != nil {
						// 引入日志可以把当前错误信息， 记录
						fmt.Println("启动重新处理失败", err)
					} else {
						// 成功消息发送后， 就删除消息
						d.Ack(false)
					}
				}
			} else {
				fmt.Println("消息处理成功")
				d.Ack(false)
			}
		}
	}()
	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
