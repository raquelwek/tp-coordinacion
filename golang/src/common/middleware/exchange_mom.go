package middleware

import (
	"context"

	rmq "github.com/rabbitmq/amqp091-go"
)

type ExchangeMiddleware struct {
	baseMiddleware
	exchangeName string
	routingKeys  []string
}

func (em *ExchangeMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	tag := em.queueName + "-consumer"
	return em.startConsuming(tag, callbackFunc)
}

func (e *ExchangeMiddleware) Send(msg Message) error {
	for _, key := range e.routingKeys {
		if err := e.SendToKey(msg, key); err != nil {
			return err
		}
	}
	return nil
}

func (e *ExchangeMiddleware) SendToKey(msg Message, key string) error {
	err := e.channel.PublishWithContext(
		context.Background(),
		e.exchangeName,
		key,
		false,
		false,
		rmq.Publishing{
			ContentType:  PLAIN_TEXT_TYPE,
			Body:         []byte(msg.Body),
			DeliveryMode: rmq.Persistent,
		},
	)
	if err != nil {
		if e.isDisconnected() {
			return ErrMessageMiddlewareDisconnected
		}
		return ErrMessageMiddlewareMessage
	}
	return nil
}
