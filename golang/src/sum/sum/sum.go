package sum

import (
	"fmt"
	"hash/fnv"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type SumConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	InputQueue        string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
}

type Sum struct {
	inputQueue     middleware.Middleware
	outputExchange middleware.Middleware
	// fruitItemMap is keyed by clientId -> fruit -> FruitItem
	fruitItemMap      map[string]map[string]fruititem.FruitItem
	aggregationAmount int
	aggregationPrefix string
	accumAmount       AccumAmount
	eventsExhcange    middleware.Middleware
}

func NewSum(config SumConfig) (*Sum, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputExchangeRouteKeys := make([]string, config.AggregationAmount)
	for i := range config.AggregationAmount {
		outputExchangeRouteKeys[i] = fmt.Sprintf("%s_%d", config.AggregationPrefix, i)
	}

	outputExchange, err1 := middleware.CreateExchangeMiddleware(config.AggregationPrefix, outputExchangeRouteKeys, connSettings)
	eventsExhcange, err2 := middleware.CreateExchangeMiddleware("events", []string{"#"}, connSettings)
	if err1 != nil || err2 != nil {
		inputQueue.Close()
		return nil, err
	}

	return &Sum{
		inputQueue:        inputQueue,
		outputExchange:    outputExchange,
		fruitItemMap:      map[string]map[string]fruititem.FruitItem{},
		aggregationAmount: config.AggregationAmount,
		aggregationPrefix: config.AggregationPrefix,
		accumAmount:       NewAccumAmount(),
		eventsExhcange:    eventsExhcange,
	}, nil
}

func (sum *Sum) Run() {
	go sum.handleSignals()
	go sum.handleEvent()
	sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleMessage(msg, ack, nack)
	})
}

func (sum *Sum) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	sum.inputQueue.StopConsuming()
}

func (sum *Sum) routingKeyForFruit(fruit string) string {
	h := fnv.New32a()
	h.Write([]byte(fruit))
	index := int(h.Sum32()) % sum.aggregationAmount
	return fmt.Sprintf("%s_%d", sum.aggregationPrefix, index)
}

func (sum *Sum) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack() // TO DO: Ver si tengo que hacerlo por separado

	clientId, fruitRecords, isEof, targetAmmount, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if isEof { // wait until every instance has already finished
		ch := sum.accumAmount.waitFor(clientId, targetAmmount)
		go func() {
			//defer sum.wg.Done()
			<-ch
			if err := sum.handleEndOfRecordMessage(clientId); err != nil {
				slog.Error("While handling end of record message", "err", err)
			}
			ack()
		}()
		// TODO: Agregar timeout?
	}

	if err := sum.handleDataMessage(clientId, fruitRecords); err != nil {
		slog.Error("While handling data message", "err", err)
	}
}

// @TO DO: Propagar EOF al resto de sums sin reenviar a los AGG para eliminar
// info del cliente.
func (sum *Sum) handleEndOfRecordMessage(clientId string) error {
	slog.Info("Received End Of Records message", "clientId", clientId)

	clientMap, ok := sum.fruitItemMap[clientId]
	if !ok {
		clientMap = map[string]fruititem.FruitItem{}
	}

	// Send each fruit's partial sum to the aggregator that owns that fruit
	for key := range clientMap {
		fruitRecord := []fruititem.FruitItem{clientMap[key]}
		message, err := inner.SerializeMessage(clientId, fruitRecord)
		if err != nil {
			slog.Debug("While serializing message", "err", err)
			return err
		}
		routingKey := sum.routingKeyForFruit(key)
		if err := sum.outputExchange.SendToKey(*message, routingKey); err != nil {
			slog.Debug("While sending message", "err", err)
			return err
		}
	}

	// Broadcast EOF to all aggregators so each one knows this Sum is done
	eofMessage := []fruititem.FruitItem{}
	message, err := inner.SerializeMessage(clientId, eofMessage)
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}
	if err := sum.outputExchange.Send(*message); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}

	delete(sum.fruitItemMap, clientId)
	return nil
}

func (sum *Sum) handleDataMessage(clientId string, fruitRecords []fruititem.FruitItem) error {
	if _, ok := sum.fruitItemMap[clientId]; !ok {
		sum.fruitItemMap[clientId] = map[string]fruititem.FruitItem{}
	}
	clientMap := sum.fruitItemMap[clientId]

	for _, fruitRecord := range fruitRecords {
		if _, ok := clientMap[fruitRecord.Fruit]; ok {
			clientMap[fruitRecord.Fruit] = clientMap[fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			clientMap[fruitRecord.Fruit] = fruitRecord
		}
	}
	body := fmt.Sprintf("%s,%d", clientId, len(fruitRecords))
	msg := middleware.Message{Body: body}
	sum.eventsExhcange.Send(msg)
	return nil
}

func (sum *Sum) handleEvent() error {
	sum.eventsExhcange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		values := strings.Split(msg.Body, ",")
		client_id, ammount := values[0], values[1]
		num, _ := strconv.ParseUint(ammount, 10, 64) //@TO DO: Constants
		sum.accumAmount.Add(client_id, num)
		sum.accumAmount.checkAndNotify(client_id)
	})
	return nil
}
