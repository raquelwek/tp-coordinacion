package aggregation

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"syscall"

	a "github.com/7574-sistemas-distribuidos/tp-coordinacion/common/accumamount"
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
	outputQueue       middleware.Middleware
	inputExchange     middleware.Middleware
	fruitItemMap      map[string]fruititem.FruitItem
	topSize           int
	accumAmount       a.AccumAmount
	eventsExhcange    middleware.Middleware
	AggregationAmount int
}

func NewAggregation(config AggregationConfig) (*Aggregation, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	inputExchangeRoutingKey := []string{fmt.Sprintf("%s_%d", config.AggregationPrefix, config.Id)}
	inputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, inputExchangeRoutingKey, connSettings)
	eventsExhcange, err2 := middleware.CreateExchangeMiddleware("agg_events", []string{"#"}, connSettings)
	if err != nil || err2 != nil {
		outputQueue.Close()
		return nil, err
	}

	return &Aggregation{
		outputQueue:       outputQueue,
		inputExchange:     inputExchange,
		fruitItemMap:      map[string]fruititem.FruitItem{},
		topSize:           config.TopSize,
		accumAmount:       a.NewAccumAmount(),
		eventsExhcange:    eventsExhcange,
		AggregationAmount: config.AggregationAmount,
	}, nil
}

func (aggregation *Aggregation) Run() {
	go aggregation.handleSignals()
	go aggregation.handleEvent()
	aggregation.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		aggregation.handleMessage(msg, ack, nack)
	})
}

func (aggregation *Aggregation) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	aggregation.inputExchange.StopConsuming()
	aggregation.eventsExhcange.StopConsuming()
}

func (aggregation *Aggregation) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack() // @To DO: cambiarlo

	clientId, fruitRecords, isEof, _, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if isEof {
		body := fmt.Sprintf("%s", clientId)
		msg := middleware.Message{Body: body}
		aggregation.eventsExhcange.Send(msg)
		ch := aggregation.accumAmount.WaitFor(clientId, uint64(aggregation.AggregationAmount))

		go func() {
			<-ch
			if err := aggregation.handleEndOfRecordsMessage(clientId); err != nil {
				slog.Error("While handling end of record message", "err", err)
			}
			ack()
		}()
	} else {
		aggregation.handleDataMessage(fruitRecords)
	}
}

func (aggregation *Aggregation) handleEndOfRecordsMessage(clientId string) error {
	slog.Info("Received End Of Records message")

	fruitTopRecords := aggregation.buildFruitTop()
	message, err := inner.SerializeMessage("id", fruitTopRecords)
	if err != nil {
		slog.Debug("While serializing top message", "err", err)
		return err
	}
	if err := aggregation.outputQueue.Send(*message); err != nil {
		slog.Debug("While sending top message", "err", err)
		return err
	}
	if aggregation.accumAmount.ChannelHasBeenClosed(clientId, uint64(aggregation.AggregationAmount)) {
		return nil
	}
	eofMessage := []fruititem.FruitItem{}
	message, err = inner.SerializeMessage("id", eofMessage)
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}
	if err := aggregation.outputQueue.Send(*message); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}
	return nil
}

func (aggregation *Aggregation) handleDataMessage(fruitRecords []fruititem.FruitItem) {
	for _, fruitRecord := range fruitRecords {
		if _, ok := aggregation.fruitItemMap[fruitRecord.Fruit]; ok {
			aggregation.fruitItemMap[fruitRecord.Fruit] = aggregation.fruitItemMap[fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			aggregation.fruitItemMap[fruitRecord.Fruit] = fruitRecord
		}
	}
}

func (aggregation *Aggregation) buildFruitTop() []fruititem.FruitItem {
	fruitItems := make([]fruititem.FruitItem, 0, len(aggregation.fruitItemMap))
	for _, item := range aggregation.fruitItemMap {
		fruitItems = append(fruitItems, item)
	}
	sort.SliceStable(fruitItems, func(i, j int) bool {
		return fruitItems[j].Less(fruitItems[i])
	})
	finalTopSize := min(aggregation.topSize, len(fruitItems))
	return fruitItems[:finalTopSize]
}
func (aggregation *Aggregation) handleEvent() {
	aggregation.eventsExhcange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		client_id := msg.Body
		aggregation.accumAmount.Add(client_id, 1)
		aggregation.accumAmount.CheckAndNotify(client_id)
		ack()
	})
}
