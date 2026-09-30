package main

import (
	"errors"
	"log/slog"
	"os"
	"strconv"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/join"
)

func loadConfig() (join.JoinConfig, error) {
	sumAmount, err := strconv.Atoi(os.Getenv("SUM_AMOUNT"))
	if err != nil {
		return join.JoinConfig{}, err
	}

	aggregationAmount, err := strconv.Atoi(os.Getenv("AGGREGATION_AMOUNT"))
	if err != nil {
		return join.JoinConfig{}, err
	}

	topSize, err := strconv.Atoi(os.Getenv("TOP_SIZE"))
	if err != nil {
		return join.JoinConfig{}, err
	}

	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return join.JoinConfig{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return join.JoinConfig{}, errors.New("MOM_HOST environment variable is required")
	}

	inputQueue := os.Getenv("INPUT_QUEUE")
	if inputQueue == "" {
		return join.JoinConfig{}, errors.New("INPUT_QUEUE environment variable is required")
	}

	outputQueue := os.Getenv("OUTPUT_QUEUE")
	if outputQueue == "" {
		return join.JoinConfig{}, errors.New("OUTPUT_QUEUE environment variable is required")
	}

	sumPrefix := os.Getenv("SUM_PREFIX")
	if sumPrefix == "" {
		return join.JoinConfig{}, errors.New("SUM_PREFIX environment variable is required")
	}

	aggregationPrefix := os.Getenv("AGGREGATION_PREFIX")
	if aggregationPrefix == "" {
		return join.JoinConfig{}, errors.New("AGGREGATION_PREFIX environment variable is required")
	}

	return join.JoinConfig{
		MomHost:           momHost,
		MomPort:           momPort,
		InputQueue:        inputQueue,
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

	slog.Info("Starting join worker",
		"inputQueue", config.InputQueue,
		"outputQueue", config.OutputQueue,
		"topSize", config.TopSize,
		"sumAmount", config.SumAmount,
		"sumPrefix", config.SumPrefix,
		"aggregationAmount", config.AggregationAmount,
		"aggregationPrefix", config.AggregationPrefix,
		"momHost", config.MomHost,
		"momPort", config.MomPort,
	)

	server, err := join.NewJoin(config)
	if err != nil {
		slog.Error("While initializing join", "err", err)
		return 1
	}

	server.Run()
	return 0
}

func main() {
	os.Exit(run())
}
