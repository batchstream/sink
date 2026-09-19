package app

import (
	"fmt"

	"github.com/liran/sink/internal/config"
	sinkmetrics "github.com/liran/sink/internal/metrics"
	queuekafka "github.com/liran/sink/internal/queue/kafka"
	"github.com/liran/sink/internal/service"
	"github.com/liran/sink/internal/worker"
)

func (app *Application) configureKafka(observed *sinkmetrics.Metrics) error {
	configured := app.config.Storage
	if !configured.Kafka.Enabled {
		return nil
	}
	topicOptions := queuekafka.TopicOptions{
		Brokers:             configured.Kafka.Brokers,
		Topics:              []string{configured.Kafka.Topic.Name, configured.Kafka.DeadLetter.Topic},
		Partitions:          configured.Kafka.Topic.Partitions,
		ReplicationFactor:   configured.Kafka.Topic.ReplicationFactor,
		Retention:           configured.Kafka.Topic.Retention,
		DeadLetterTopic:     configured.Kafka.DeadLetter.Topic,
		DeadLetterRetention: configured.Kafka.DeadLetter.Retention,
		MinInSyncReplicas:   configured.Kafka.Topic.MinInSyncReplicas,
		MaxRecordBytes:      configured.Kafka.Topic.MaxRecordBytes,
	}
	app.topics = queuekafka.NewTopicManager(topicOptions)
	if app.config.Mode != config.ModeEngine {
		return nil
	}
	publisherOptions := queuekafka.PublisherOptions{
		Store: configured.Name, Brokers: configured.Kafka.Brokers, Topics: app.topics,
		MaxRecordBytes:   configured.Kafka.Topic.MaxRecordBytes,
		MaxBufferedBytes: configured.Kafka.Producer.MaxBufferedBytes,
		Topic:            configured.Kafka.Topic.Name, Metrics: observed,
	}

	publisher, err := queuekafka.NewPublisher(publisherOptions)
	if err != nil {
		return fmt.Errorf("create Kafka publisher for store %q: %w", configured.Name, err)
	}
	app.kafkaPublisher = publisher
	app.publisher = publisher
	healthCheck := &configuredHealthCheck{service: kafkaHealthService(configured.Name), pinger: publisher}
	app.healthChecks = append(app.healthChecks, healthCheck)
	return nil
}

func (app *Application) configureWorker(server *service.Server, observed *sinkmetrics.Metrics) error {
	loaded := app.config
	configured := loaded.Storage
	processor, err := worker.NewProcessor(server)
	if err != nil {
		return err
	}
	workerOptions := queuekafka.WorkerOptions{
		ShutdownTimeout: loaded.ShutdownTimeout, Topics: app.topics,
		ProcessingTimeout: configured.Kafka.Consumer.ProcessingTimeout,
		MaxRecordBytes:    configured.Kafka.Topic.MaxRecordBytes,
		Brokers:           configured.Kafka.Brokers, Store: configured.Name,
		Topic: configured.Kafka.Topic.Name, GroupID: configured.Kafka.Consumer.GroupID,
		DeadLetterTopic: configured.Kafka.DeadLetter.Topic, Handler: processor,
		MaxPollRecords:   configured.Kafka.Consumer.MaxPollRecords,
		MaxRetryAttempts: configured.Kafka.Consumer.Retry.MaxAttempts,
		RetryBackoff:     configured.Kafka.Consumer.Retry.Backoff,
		MaxRetryBackoff:  configured.Kafka.Consumer.Retry.MaxBackoff, Metrics: observed,
	}
	kafkaWorker, err := queuekafka.NewWorker(workerOptions)
	if err != nil {
		return fmt.Errorf("create Kafka worker for store %q: %w", configured.Name, err)
	}
	app.worker = kafkaWorker
	workerHealth := &configuredHealthCheck{service: "sink.worker." + configured.Name, pinger: kafkaWorker}
	app.healthChecks = append(app.healthChecks, workerHealth)
	return nil
}
