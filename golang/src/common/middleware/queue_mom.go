package middleware

import (
	"context"
	"errors"
	"fmt"
	"time"

	rmq "github.com/rabbitmq/amqp091-go"
)

type QueueMiddleware struct {
	baseMiddleware
}

func (q *QueueMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	tag := fmt.Sprintf("consumer-%d", time.Now().UnixNano())
	return q.startConsuming(tag, callbackFunc)
}

func (qm *QueueMiddleware) Send(msg Message) error {
	err := qm.channel.PublishWithContext(
		context.Background(),
		DEFAULT_EXCHANGE, // exchange: vacío = exchange por defecto
		qm.queueName,     // routing key = nombre de la queue
		false,            // mandatory
		false,            // immediate
		rmq.Publishing{
			ContentType:  PLAIN_TEXT_TYPE,
			Body:         []byte(msg.Body),
			DeliveryMode: rmq.Persistent,
		},
	)
	if err != nil {
		if qm.isDisconnected() || errors.Is(err, rmq.ErrClosed) {
			return ErrMessageMiddlewareDisconnected
		}
		return ErrMessageMiddlewareMessage
	}

	return nil
}
