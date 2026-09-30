package inner

import (
	"encoding/json"
	"errors"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

func serializeJson(message []interface{}) ([]byte, error) {
	return json.Marshal(message)
}

func deserializeJson(message []byte) ([]interface{}, error) {
	var data []interface{}
	if err := json.Unmarshal(message, &data); err != nil {
		return nil, err
	}
	return data, nil
}

// SerializeMessage serializes a data message with format: [clientId, [[fruit, amount], ...], false]
func SerializeMessage(clientId string, fruitRecords []fruititem.FruitItem) (*middleware.Message, error) {
	records := []interface{}{}
	for _, fruitRecord := range fruitRecords {
		datum := []interface{}{
			fruitRecord.Fruit,
			fruitRecord.Amount,
		}
		records = append(records, datum)
	}

	data := []interface{}{clientId, records, false}

	body, err := serializeJson(data)
	if err != nil {
		return nil, err
	}
	return &middleware.Message{Body: string(body)}, nil
}

// SerializeEOFMessage serializes an EOF message with format: [clientId, [], true, totalItemsSent]
func SerializeEOFMessage(clientId string, totalItemsSent uint64) (*middleware.Message, error) {
	data := []interface{}{clientId, []interface{}{}, true, totalItemsSent}

	body, err := serializeJson(data)
	if err != nil {
		return nil, err
	}
	return &middleware.Message{Body: string(body)}, nil
}

// DeserializeMessage deserializes a message returning the clientId, fruit records, whether it's an EOF,
// the total items sent (only meaningful when isEOF is true), and any error.
func DeserializeMessage(message *middleware.Message) (string, []fruititem.FruitItem, bool, uint64, error) {
	data, err := deserializeJson([]byte((*message).Body))
	if err != nil {
		return "", nil, false, 0, err
	}

	if len(data) < 3 {
		return "", nil, false, 0, errors.New("message does not have expected [clientId, records, isEOF] format")
	}

	clientId, ok := data[0].(string)
	if !ok {
		return "", nil, false, 0, errors.New("clientId is not a string")
	}

	records, ok := data[1].([]interface{})
	if !ok {
		return "", nil, false, 0, errors.New("records is not an array")
	}

	isEOF, ok := data[2].(bool)
	if !ok {
		return "", nil, false, 0, errors.New("isEOF flag is not a bool")
	}

	fruitRecords := []fruititem.FruitItem{}
	for _, record := range records {
		fruitPair, ok := record.([]interface{})
		if !ok {
			return "", nil, false, 0, errors.New("datum is not an array")
		}

		fruit, ok := fruitPair[0].(string)
		if !ok {
			return "", nil, false, 0, errors.New("datum is not a (fruit, amount) pair")
		}

		fruitAmount, ok := fruitPair[1].(float64)
		if !ok {
			return "", nil, false, 0, errors.New("datum is not a (fruit, amount) pair")
		}

		fruitRecords = append(fruitRecords, fruititem.FruitItem{Fruit: fruit, Amount: uint32(fruitAmount)})
	}

	var totalItemsSent uint64
	if isEOF && len(data) >= 4 {
		if v, ok := data[3].(float64); ok {
			totalItemsSent = uint64(v)
		}
	}

	return clientId, fruitRecords, isEOF, totalItemsSent, nil
}
