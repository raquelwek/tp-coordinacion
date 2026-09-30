package main

import (
	"errors"
	"log/slog"
	"os"
	"strconv"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/aggregation"
)

func loadConfig() (aggregation.AggregationConfig, error) {
	id, err := strconv.Atoi(os.Getenv("ID"))
	if err != nil {
		return aggregation.AggregationConfig{}, err
	}

	sumAmount, err := strconv.Atoi(os.Getenv("SUM_AMOUNT"))
	if err != nil {
		return aggregation.AggregationConfig{}, err
	}

	aggregationAmount, err := strconv.Atoi(os.Getenv("AGGREGATION_AMOUNT"))
	if err != nil {
		return aggregation.AggregationConfig{}, err
	}

	topSize, err := strconv.Atoi(os.Getenv("TOP_SIZE"))
	if err != nil {
		return aggregation.AggregationConfig{}, err
	}

	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return aggregation.AggregationConfig{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return aggregation.AggregationConfig{}, errors.New("MOM_HOST environment variable is required")
	}

	outputQueue := os.Getenv("OUTPUT_QUEUE")
	if outputQueue == "" {
		return aggregation.AggregationConfig{}, errors.New("OUTPUT_QUEUE environment variable is required")
	}

	sumPrefix := os.Getenv("SUM_PREFIX")
	if sumPrefix == "" {
		return aggregation.AggregationConfig{}, errors.New("SUM_PREFIX environment variable is required")
	}

	aggregationPrefix := os.Getenv("AGGREGATION_PREFIX")
	if aggregationPrefix == "" {
		return aggregation.AggregationConfig{}, errors.New("AGGREGATION_PREFIX environment variable is required")
	}

	return aggregation.AggregationConfig{
		Id:                id,
		MomHost:           momHost,
		MomPort:           momPort,
		OutputQueue:       outputQueue,
		SumAmount:         sumAmount,
		SumPrefix:         sumPrefix,
		AggregationAmount: aggregationAmount,
		AggregationPrefix: aggregationPrefix,
		TopSize:           topSize,
	}, nil
}

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	slog.Info("Starting aggregation worker",
		"id", config.Id,
		"outputQueue", config.OutputQueue,
		"aggregationAmount", config.AggregationAmount,
		"aggregationPrefix", config.AggregationPrefix,
		"sumAmount", config.SumAmount,
		"sumPrefix", config.SumPrefix,
		"topSize", config.TopSize,
		"momHost", config.MomHost,
		"momPort", config.MomPort,
	)

	server, err := aggregation.NewAggregation(config)
	if err != nil {
		slog.Error("While initializing aggregation", "err", err)
		return 1
	}

	server.Run()
	return 0
}

func main() {
	os.Exit(run())
}
