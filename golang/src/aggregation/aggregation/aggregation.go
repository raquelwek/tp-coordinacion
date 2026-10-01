package aggregation

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"syscall"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type AggregationConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Aggregation struct {
	id            int
	outputQueue   middleware.Middleware
	inputExchange middleware.Middleware
	fruitItemMap  map[string]map[string]fruititem.FruitItem
	eofsReceived  map[string]int
	topSize       int
	sumAmount     int
}

func NewAggregation(config AggregationConfig) (*Aggregation, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		return nil, err
	}
	inputExchangeRoutingKey := []string{fmt.Sprintf("%s_%d", config.AggregationPrefix, config.Id)}
	inputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, inputExchangeRoutingKey, connSettings)

	if err != nil {
		outputQueue.Close()
		return nil, err
	}
	return &Aggregation{
		id:            config.Id,
		outputQueue:   outputQueue,
		inputExchange: inputExchange,
		fruitItemMap:  map[string]map[string]fruititem.FruitItem{},
		eofsReceived:  map[string]int{},
		topSize:       config.TopSize,
		sumAmount:     config.SumAmount,
	}, nil
}

func (aggregation *Aggregation) Run() {
	go aggregation.handleSignals()
	aggregation.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		aggregation.handleMessage(msg, ack, nack)
	})
	aggregation.inputExchange.Close()
	aggregation.outputQueue.Close()
}

func (aggregation *Aggregation) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	aggregation.inputExchange.StopConsuming()
}

func (aggregation *Aggregation) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	clientId, fruitRecords, isEof, _, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if isEof {
		if _, ok := aggregation.fruitItemMap[clientId]; !ok {
			aggregation.eofsReceived[clientId] = 0
		}

		if err := aggregation.handleEndOfRecordsMessage(clientId); err != nil {
			slog.Error("While handling end of record message", "err", err)
		}
		return
	}
	aggregation.handleDataMessage(clientId, fruitRecords)

}

func (aggregation *Aggregation) handleEndOfRecordsMessage(clientId string) error {
	itemCount := len(aggregation.fruitItemMap[clientId])
	slog.Info("Received End Of Records message", "clientId", clientId, "aggregationId", aggregation.id, "accumulatedFruits", itemCount)
	aggregation.eofsReceived[clientId]++
	if aggregation.eofsReceived[clientId] != aggregation.sumAmount {
		return nil
	}
	fruitTopRecords := aggregation.buildFruitTop(clientId)

	message, err := inner.SerializeMessage(clientId, fruitTopRecords)
	if err != nil {
		slog.Debug("While serializing top message", "err", err)
		return err
	}
	if err := aggregation.outputQueue.Send(*message); err != nil {
		slog.Debug("While sending top message", "err", err)
		return err
	}

	eofMessage, err := inner.SerializeEOFMessage(clientId, 0)
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}
	if err := aggregation.outputQueue.Send(*eofMessage); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}
	slog.Info("Sent partial top and EOF", "clientId", clientId, "aggregationId", aggregation.id)

	return nil
}

func (aggregation *Aggregation) handleDataMessage(clientId string, fruitRecords []fruititem.FruitItem) {
	if _, ok := aggregation.fruitItemMap[clientId]; !ok {
		aggregation.fruitItemMap[clientId] = map[string]fruititem.FruitItem{}
	}
	clientMap := aggregation.fruitItemMap[clientId]
	for _, fruitRecord := range fruitRecords {
		if _, ok := clientMap[fruitRecord.Fruit]; ok {
			clientMap[fruitRecord.Fruit] = clientMap[fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			clientMap[fruitRecord.Fruit] = fruitRecord
		}
	}
}

func (aggregation *Aggregation) buildFruitTop(clientId string) []fruititem.FruitItem {
	fruitItems := make([]fruititem.FruitItem, 0, len(aggregation.fruitItemMap[clientId]))
	for _, item := range aggregation.fruitItemMap[clientId] {
		fruitItems = append(fruitItems, item)
	}
	sort.SliceStable(fruitItems, func(i, j int) bool {
		return fruitItems[j].Less(fruitItems[i])
	})
	finalTopSize := min(aggregation.topSize, len(fruitItems))
	delete(aggregation.fruitItemMap, clientId)
	return fruitItems[:finalTopSize]
}
