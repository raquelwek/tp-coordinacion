package messagehandler

import (
	"fmt"
	"sync/atomic"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

var clientCounter atomic.Uint64

type MessageHandler struct {
	clientId  string
	itemsSent uint64
}

func NewMessageHandler() MessageHandler {
	id := fmt.Sprintf("%d", clientCounter.Add(1))
	return MessageHandler{clientId: id}
}

func (messageHandler *MessageHandler) SerializeDataMessage(fruitRecord fruititem.FruitItem) (*middleware.Message, error) {
	messageHandler.itemsSent++
	data := []fruititem.FruitItem{fruitRecord}
	return inner.SerializeMessage(messageHandler.clientId, data)
}

func (messageHandler *MessageHandler) SerializeEOFMessage() (*middleware.Message, error) {
	return inner.SerializeEOFMessage(messageHandler.clientId, messageHandler.itemsSent)
}

// DeserializeResultMessage returns the fruit top if the message belongs to this client, nil if it belongs to another client.
func (messageHandler *MessageHandler) DeserializeResultMessage(message *middleware.Message) ([]fruititem.FruitItem, error) {
	clientId, fruitRecords, _, _, err := inner.DeserializeMessage(message)
	if err != nil {
		return nil, err
	}
	if clientId != messageHandler.clientId {
		return nil, nil
	}
	return fruitRecords, nil
}
