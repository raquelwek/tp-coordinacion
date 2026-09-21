package middleware

import (
	"fmt"

	rmq "github.com/rabbitmq/amqp091-go"
)

const amqpURI = "amqp://guest:guest@%s:%d/"
const ExchangeTopic = "topic"

func CreateQueueMiddleware(queueName string, connectionSettings ConnSettings) (Middleware, error) {

	conn, err := rmq.Dial(formatURI(connectionSettings))

	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	_, err = ch.QueueDeclare(queueName, true, false, false, false, nil)
	//                                 durable, autoDelete, exclusive, noWait, , args
	if err != nil {
		return nil, err
	}
	closeErr := make(chan *rmq.Error, 1)
	conn.NotifyClose(closeErr)

	return &QueueMiddleware{
		baseMiddleware: baseMiddleware{
			queueName: queueName,
			conn:      conn,
			channel:   ch,
			closeErr:  closeErr,
		},
	}, nil
}

func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings ConnSettings) (Middleware, error) {
	conn, err := rmq.Dial(formatURI(connectionSettings))

	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	err = ch.ExchangeDeclare(exchange, ExchangeTopic, true, false, false, false, nil)
	if err != nil {
		return nil, err
	}
	queue, err := ch.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		return nil, err
	}
	for _, key := range keys {
		err = ch.QueueBind(queue.Name, key, exchange, false, nil)
		if err != nil {
			return nil, err
		}
	}
	closeErr := make(chan *rmq.Error, 1)
	conn.NotifyClose(closeErr)

	return &ExchangeMiddleware{
		baseMiddleware: baseMiddleware{
			queueName: queue.Name, // la queue interna generada
			conn:      conn,
			channel:   ch,
			closeErr:  closeErr,
		},
		exchangeName: exchange,
		routingKeys:  keys,
	}, nil
}

func formatURI(connectionSettings ConnSettings) string {
	return fmt.Sprintf(amqpURI, connectionSettings.Hostname, connectionSettings.Port)
}
