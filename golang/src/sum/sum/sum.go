package sum

import (
	"fmt"
	"hash/fnv"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"

	a "github.com/7574-sistemas-distribuidos/tp-coordinacion/common/accumamount"
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
	ammount        int
	// fruitItemMap is keyed by clientId -> fruit -> FruitItem
	fruitItemMap      map[string]map[string]fruititem.FruitItem
	fruitItemMapMu    sync.Mutex
	aggregationAmount int
	aggregationPrefix string
	accumAmount       a.AccumAmount
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
	eventsExhcange, err2 := middleware.CreateExchangeMiddleware("sum_events", []string{"#"}, connSettings)
	if err1 != nil || err2 != nil {
		inputQueue.Close()
		return nil, err
	}

	return &Sum{
		inputQueue:        inputQueue,
		outputExchange:    outputExchange,
		ammount:           config.SumAmount,
		fruitItemMap:      map[string]map[string]fruititem.FruitItem{},
		aggregationAmount: config.AggregationAmount,
		aggregationPrefix: config.AggregationPrefix,
		accumAmount:       a.NewAccumAmount(),
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
	clientId, fruitRecords, isEof, targetAmmount, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		ack()
		return
	}

	if isEof { // wait until every instance has already finished
		slog.Info("EOF received, waiting for accumulator", "clientId", clientId, "targetAmount", targetAmmount)
		ch := sum.accumAmount.WaitFor(clientId, targetAmmount)
		go func() {
			<-ch
			body := fmt.Sprintf("%s,ALL_RECEIVED", clientId)
			msg := middleware.Message{Body: body}
			sum.eventsExhcange.Send(msg)
			ack()
		}()
		// No ack here: the goroutine owns the ack for this message
		return
	}

	defer ack()
	if err := sum.handleDataMessage(clientId, fruitRecords); err != nil {
		slog.Error("While handling data message", "err", err)
	}
}
func (sum *Sum) sendParcialSums(clientId string) error {
	sum.fruitItemMapMu.Lock()
	clientMap, ok := sum.fruitItemMap[clientId]
	if !ok {
		clientMap = map[string]fruititem.FruitItem{}
	}
	items := make([]fruititem.FruitItem, 0, len(clientMap))
	for _, item := range clientMap {
		items = append(items, item)
	}
	sum.fruitItemMapMu.Unlock()

	slog.Info("Sending partial sums", "clientId", clientId, "count", len(items))
	// Log and send each fruit's partial sum to the aggregator that owns that fruit
	for _, item := range items {
		routingKey := sum.routingKeyForFruit(item.Fruit)
		slog.Info("Sending partial sum", "clientId", clientId, "fruit", item.Fruit, "amount", item.Amount, "routingKey", routingKey)
		fruitRecord := []fruititem.FruitItem{item}
		message, err := inner.SerializeMessage(clientId, fruitRecord)
		if err != nil {
			slog.Debug("While serializing message", "err", err)
			return err
		}
		if err := sum.outputExchange.SendToKey(*message, routingKey); err != nil {
			slog.Debug("While sending message", "err", err)
			return err
		}
	}
	return nil
}
func (sum *Sum) handleEndOfRecordMessage(clientId string) error {
	slog.Info("Received End Of Records message", "clientId", clientId)

	// Broadcast EOF to all aggregators so each one knows this Sum is done
	message, err := inner.SerializeEOFMessage(clientId, 0)
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}
	if err := sum.outputExchange.Send(*message); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}

	sum.fruitItemMapMu.Lock()
	delete(sum.fruitItemMap, clientId)
	sum.fruitItemMapMu.Unlock()
	return nil
}

func (sum *Sum) handleDataMessage(clientId string, fruitRecords []fruititem.FruitItem) error {
	sum.fruitItemMapMu.Lock()
	defer sum.fruitItemMapMu.Unlock()

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
	err := sum.eventsExhcange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		values := strings.Split(msg.Body, ",")
		client_id, value := values[0], values[1]
		switch value {
		case "ALL_RECEIVED":
			sum.sendParcialSums(client_id)
			sum.handleEndOfRecordMessage(client_id)
		default:
			num, _ := strconv.ParseUint(value, 10, 64) //@TO DO: Constants
			sum.accumAmount.Add(client_id, num)
			sum.accumAmount.CheckAndNotify(client_id)
		}

		ack()
	})
	return err
}
