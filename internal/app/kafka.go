package app

import (
	"fmt"

	"github.com/liran/sink/internal/config"
	sinkmetrics "github.com/liran/sink/internal/metrics"
	"github.com/liran/sink/internal/queue"
	queuekafka "github.com/liran/sink/internal/queue/kafka"
	"github.com/liran/sink/internal/service"
	"github.com/liran/sink/internal/worker"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func (app *Application) configureKafka(observed *sinkmetrics.Metrics) error {
	loaded := app.config
	var err error
	for _, configured := range loaded.Storages {
		if !configured.Kafka.Enabled {
			continue
		}
		topics := []string{configured.Kafka.Topic.Name, configured.Kafka.DeadLetter.Topic}
		topicOptions := queuekafka.TopicOptions{
			Brokers:             configured.Kafka.Brokers,
			Topics:              topics,
			Partitions:          configured.Kafka.Topic.Partitions,
			ReplicationFactor:   configured.Kafka.Topic.ReplicationFactor,
			Retention:           configured.Kafka.Topic.Retention,
			DeadLetterTopic:     configured.Kafka.DeadLetter.Topic,
			DeadLetterRetention: configured.Kafka.DeadLetter.Retention,
			MinInSyncReplicas:   configured.Kafka.Topic.MinInSyncReplicas,
			MaxRecordBytes:      configured.Kafka.Topic.MaxRecordBytes,
		}
		app.topics[configured.Name] = queuekafka.NewTopicManager(topicOptions)
	}
	if loaded.Mode == config.ModeServer || loaded.Mode == config.ModeAll {
		storePublishers := make(map[string]queue.Publisher)
		for _, configured := range loaded.Storages {
			if !configured.Kafka.Enabled {
				continue
			}
			publisherOptions := queuekafka.PublisherOptions{
				Store:            configured.Name,
				Brokers:          configured.Kafka.Brokers,
				Topics:           app.topics[configured.Name],
				MaxRecordBytes:   configured.Kafka.Topic.MaxRecordBytes,
				MaxBufferedBytes: configured.Kafka.Producer.MaxBufferedBytes,
				Topic:            configured.Kafka.Topic.Name,
				Metrics:          observed,
			}
			if configured.Driver == config.DriverElasticsearch || configured.Driver == config.DriverOpenSearch {
				publisherOptions.MutationKey = queue.MutationKeyWithoutNamespace
			}
			publisher, publisherErr := queuekafka.NewPublisher(publisherOptions)
			if publisherErr != nil {
				return fmt.Errorf("create Kafka publisher for store %q: %w", configured.Name, publisherErr)
			}
			app.kafkaPublishers = append(app.kafkaPublishers, publisher)

			healthCheck := &configuredHealthCheck{
				service: kafkaHealthService(configured.Name),
				pinger:  publisher,
			}
			app.healthChecks = append(app.healthChecks, healthCheck)
			storePublishers[configured.Name] = publisher
		}
		if len(storePublishers) > 0 {
			app.publisher, err = queue.NewRoutingPublisher(storePublishers)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (app *Application) configureWorkers(server *service.Server, observed *sinkmetrics.Metrics) error {
	loaded := app.config

	processor, processorErr := worker.NewProcessor(server)
	if processorErr != nil {
		return processorErr
	}
	for _, configured := range loaded.Storages {
		if !configured.Kafka.Enabled {
			continue
		}
		workerOptions := queuekafka.WorkerOptions{
			ShutdownTimeout:   loaded.ShutdownTimeout,
			Topics:            app.topics[configured.Name],
			ProcessingTimeout: configured.Kafka.Consumer.ProcessingTimeout,
			MaxRecordBytes:    configured.Kafka.Topic.MaxRecordBytes,
			Brokers:           configured.Kafka.Brokers,
			Store:             configured.Name,
			Topic:             configured.Kafka.Topic.Name,
			GroupID:           configured.Kafka.Consumer.GroupID,
			DeadLetterTopic:   configured.Kafka.DeadLetter.Topic,
			Handler:           processor,
			MaxPollRecords:    min(configured.Kafka.Consumer.MaxPollRecords, loaded.Service.Request.MaxOperations),
			MaxRetryAttempts:  configured.Kafka.Consumer.Retry.MaxAttempts,
			RetryBackoff:      configured.Kafka.Consumer.Retry.Backoff,
			MaxRetryBackoff:   configured.Kafka.Consumer.Retry.MaxBackoff,
			Metrics:           observed,
		}
		kafkaWorker, workerErr := queuekafka.NewWorker(workerOptions)
		if workerErr != nil {
			return fmt.Errorf("create Kafka worker for store %q: %w", configured.Name, workerErr)
		}
		workerInstance := configuredWorker{store: configured.Name, worker: kafkaWorker}
		app.workers = append(app.workers, workerInstance)
		workerHealth := &configuredHealthCheck{service: "sink.worker." + configured.Name, pinger: kafkaWorker}
		app.healthChecks = append(app.healthChecks, workerHealth)
		if app.health != nil {
			app.health.SetServingStatus(workerHealth.service, healthpb.HealthCheckResponse_NOT_SERVING)
		}
	}
	return nil
}
