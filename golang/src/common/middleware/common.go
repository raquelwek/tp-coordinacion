package middleware

import (
	"errors"

	rmq "github.com/rabbitmq/amqp091-go"
)

const DEFAULT_EXCHANGE = ""
const PLAIN_TEXT_TYPE = "text/plain"

type baseMiddleware struct {
	queueName   string
	consumerTag string
	conn        *rmq.Connection
	channel     *rmq.Channel
	closeErr    chan *rmq.Error
}

func (b *baseMiddleware) isDisconnected() bool {
	select {
	case <-b.closeErr:
		return true
	default:
		return b.conn != nil && b.conn.IsClosed()
	}
}

func (b *baseMiddleware) startConsuming(tag string, callbackFunc func(msg Message, ack func(), nack func())) error {
	deliveries, err := b.channel.Consume(
		b.queueName,
		tag,
		false, // autoAck: manual
		false, // exclusive
		false, // noLocal
		false, // noWait
		nil,
	)
	if err != nil {
		if b.isDisconnected() {
			return ErrMessageMiddlewareDisconnected
		}
		return ErrMessageMiddlewareMessage
	}

	b.consumerTag = tag
	go receiveMessages(deliveries, callbackFunc)
	return nil
}

func (b *baseMiddleware) StopConsuming() error {
	if b.consumerTag == "" {
		return nil
	}

	err := b.channel.Cancel(b.consumerTag, false)
	if err != nil {
		if b.isDisconnected() || errors.Is(err, rmq.ErrClosed) {
			return ErrMessageMiddlewareDisconnected
		}
		return ErrMessageMiddlewareMessage
	}

	b.consumerTag = ""
	return nil
}

func (b *baseMiddleware) Close() error {
	if b.conn == nil || b.conn.IsClosed() {
		return nil
	}

	if b.consumerTag != "" {
		if err := b.StopConsuming(); err != nil {
			return ErrMessageMiddlewareClose
		}
	}

	if b.channel != nil {
		_ = b.channel.Close()
	}

	if err := b.conn.Close(); err != nil {
		return ErrMessageMiddlewareClose
	}
	return nil
}

func receiveMessages(deliveries <-chan rmq.Delivery, callbackFunc func(msg Message, ack func(), nack func())) {
	for delivery := range deliveries {
		d := delivery
		msg := Message{Body: string(d.Body)}
		callbackFunc(msg,
			func() { d.Ack(false) },
			func() { d.Nack(false, true) },
		)
	}
}
