package main

import (
	"errors"
	"log/slog"
	"os"
	"strconv"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/sum"
)

func loadConfig() (sum.SumConfig, error) {
	id, err := strconv.Atoi(os.Getenv("ID"))
	if err != nil {
		return sum.SumConfig{}, err
	}

	sumAmount, err := strconv.Atoi(os.Getenv("SUM_AMOUNT"))
	if err != nil {
		return sum.SumConfig{}, err
	}

	aggregationAmount, err := strconv.Atoi(os.Getenv("AGGREGATION_AMOUNT"))
	if err != nil {
		return sum.SumConfig{}, err
	}

	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return sum.SumConfig{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return sum.SumConfig{}, errors.New("MOM_HOST environment variable is required")
	}

	inputQueue := os.Getenv("INPUT_QUEUE")
	if inputQueue == "" {
		return sum.SumConfig{}, errors.New("INPUT_QUEUE environment variable is required")
	}

	sumPrefix := os.Getenv("SUM_PREFIX")
	if sumPrefix == "" {
		return sum.SumConfig{}, errors.New("SUM_PREFIX environment variable is required")
	}

	aggregationPrefix := os.Getenv("AGGREGATION_PREFIX")
	if aggregationPrefix == "" {
		return sum.SumConfig{}, errors.New("AGGREGATION_PREFIX environment variable is required")
	}

	return sum.SumConfig{
		Id:                id,
		MomHost:           momHost,
		MomPort:           momPort,
		InputQueue:        inputQueue,
		SumAmount:         sumAmount,
		SumPrefix:         sumPrefix,
		AggregationAmount: aggregationAmount,
		AggregationPrefix: aggregationPrefix,
	}, nil
}

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	slog.Info("Starting sum worker",
		"id", config.Id,
		"inputQueue", config.InputQueue,
		"sumAmount", config.SumAmount,
		"sumPrefix", config.SumPrefix,
		"aggregationAmount", config.AggregationAmount,
		"aggregationPrefix", config.AggregationPrefix,
		"momHost", config.MomHost,
		"momPort", config.MomPort,
	)

	server, err := sum.NewSum(config)
	if err != nil {
		slog.Error("While initializing sum", "err", err)
		return 1
	}

	server.Run()
	return 0
}

func main() {
	os.Exit(run())
}
