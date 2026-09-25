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

// SerializeMessage serializes a message with format: [clientId, [[fruit, amount], ...]]
// An empty fruitRecords slice is used to signal EOF.
func SerializeMessage(clientId string, fruitRecords []fruititem.FruitItem) (*middleware.Message, error) {
	records := []interface{}{}
	for _, fruitRecord := range fruitRecords {
		datum := []interface{}{
			fruitRecord.Fruit,
			fruitRecord.Amount,
		}
		records = append(records, datum)
	}

	data := []interface{}{clientId, records}

	body, err := serializeJson(data)
	if err != nil {
		return nil, err
	}
	message := middleware.Message{Body: string(body)}

	return &message, nil
}

// DeserializeMessage deserializes a message returning the clientId, fruit records, whether it's an EOF, and any error.
func DeserializeMessage(message *middleware.Message) (string, []fruititem.FruitItem, bool, error) {
	data, err := deserializeJson([]byte((*message).Body))
	if err != nil {
		return "", nil, false, err
	}

	if len(data) != 2 {
		return "", nil, false, errors.New("Message does not have expected [clientId, records] format")
	}

	clientId, ok := data[0].(string)
	if !ok {
		return "", nil, false, errors.New("clientId is not a string")
	}

	records, ok := data[1].([]interface{})
	if !ok {
		return "", nil, false, errors.New("records is not an array")
	}

	fruitRecords := []fruititem.FruitItem{}
	for _, record := range records {
		fruitPair, ok := record.([]interface{})
		if !ok {
			return "", nil, false, errors.New("Datum is not an array")
		}

		fruit, ok := fruitPair[0].(string)
		if !ok {
			return "", nil, false, errors.New("Datum is not a (fruit, amount) pair")
		}

		fruitAmount, ok := fruitPair[1].(float64)
		if !ok {
			return "", nil, false, errors.New("Datum is not a (fruit, amount) pair")
		}

		fruitRecord := fruititem.FruitItem{Fruit: fruit, Amount: uint32(fruitAmount)}
		fruitRecords = append(fruitRecords, fruitRecord)
	}

	return clientId, fruitRecords, len(fruitRecords) == 0, nil
}
