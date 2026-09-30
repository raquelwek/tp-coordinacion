package join

import (
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"syscall"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type JoinConfig struct {
	MomHost           string
	MomPort           int
	InputQueue        string
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Join struct {
	inputQueue   middleware.Middleware
	outputQueue  middleware.Middleware
	fruitItemMap map[string][]fruititem.FruitItem
	topSize      int
}

func NewJoin(config JoinConfig) (*Join, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	return &Join{
		inputQueue:   inputQueue,
		outputQueue:  outputQueue,
		fruitItemMap: map[string][]fruititem.FruitItem{},
		topSize:      config.TopSize,
	}, nil
}

func (join *Join) Run() {
	go join.handleSignals()
	join.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		join.handleMessage(msg, ack, nack)
	})
}

func (join *Join) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	join.inputQueue.StopConsuming()
}

func (join *Join) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	clientId, fruitRecords, isEof, _, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if isEof {
		if err := join.handleEndOfRecordsMessage(clientId); err != nil {
			slog.Error("While handling end of record message", "err", err)
		}
		return
	}
	join.handleParcialTop(clientId, fruitRecords)
}

func (join *Join) handleEndOfRecordsMessage(clientId string) error {
	slog.Info("Received End Of Records message", "clientId", clientId)
	fruitItems := join.fruitItemMap[clientId]
	sort.SliceStable(fruitItems, func(i, j int) bool {
		return fruitItems[j].Less(fruitItems[i])
	})
	finalTopSize := min(join.topSize, len(fruitItems))
	delete(join.fruitItemMap, clientId)
	top := fruitItems[:finalTopSize]

	for _, item := range top {
		slog.Info("Sending final top", "clientId", clientId, "fruit", item.Fruit, "amount", item.Amount)
	}

	msg, err := inner.SerializeMessage(clientId, top)

	if err != nil {
		return err
	}
	if err := join.outputQueue.Send(*msg); err != nil {
		slog.Error("While sending top", "err", err)
	}
	return nil
}

func (join *Join) handleParcialTop(clientId string, fruitRecords []fruititem.FruitItem) {
	if _, ok := join.fruitItemMap[clientId]; !ok {
		join.fruitItemMap[clientId] = make([]fruititem.FruitItem, 0)
	}
	for _, fruitItem := range fruitRecords {
		join.fruitItemMap[clientId] = append(join.fruitItemMap[clientId], fruitItem)
	}

}
