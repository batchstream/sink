// Package kafka provides a durable Kafka mutation publisher and consumer.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	sinkmetrics "github.com/batchstream/sink/internal/metrics"
	"github.com/batchstream/sink/internal/queue"
	"github.com/batchstream/sink/internal/storage"
	"github.com/twmb/franz-go/pkg/kgo"
)

type PublisherOptions struct {
	Store            string
	Topics           *TopicManager
	Brokers          []string
	Topic            string
	ClientOptions    []kgo.Opt
	Metrics          *sinkmetrics.Metrics
	MaxRecordBytes   int
	MaxBufferedBytes int
}

type Publisher struct {
	store          string
	topics         *TopicManager
	client         *kgo.Client
	topic          string
	metrics        *sinkmetrics.Metrics
	maxRecordBytes int
}

func NewPublisher(opts PublisherOptions) (*Publisher, error) {
	if len(opts.Brokers) == 0 {
		return nil, errors.New("create Kafka publisher: brokers are required")
	}
	if opts.Topic == "" {
		return nil, errors.New("create Kafka publisher: topic is required")
	}
	if opts.MaxRecordBytes < 0 || opts.MaxBufferedBytes < 0 {
		return nil, errors.New("create Kafka publisher: byte limits cannot be negative")
	}
	if opts.MaxRecordBytes == 0 {
		opts.MaxRecordBytes = defaultMaxRecordBytes
	}
	if opts.MaxBufferedBytes == 0 {
		opts.MaxBufferedBytes = 64 << 20
	}
	if opts.MaxRecordBytes > opts.MaxBufferedBytes || opts.MaxRecordBytes > 64<<20 {
		return nil, errors.New("create Kafka publisher: record limit must fit the buffer and cannot exceed 64 MiB")
	}
	clientOptions := append([]kgo.Opt(nil), opts.ClientOptions...)
	requiredOptions := []kgo.Opt{
		kgo.SeedBrokers(opts.Brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.MaxBufferedBytes(opts.MaxBufferedBytes),
		kgo.ProducerBatchMaxBytes(int32(opts.MaxRecordBytes + kafkaRecordOverhead)),
		kgo.RecordDeliveryTimeout(30 * time.Second),
	}
	clientOptions = append(clientOptions, requiredOptions...)
	client, err := kgo.NewClient(clientOptions...)
	if err != nil {
		return nil, err
	}
	publisher := &Publisher{store: opts.Store, topics: opts.Topics, client: client, topic: opts.Topic, metrics: opts.Metrics,
		maxRecordBytes: opts.MaxRecordBytes}
	return publisher, nil
}

func (p *Publisher) Publish(ctx context.Context, req queue.PublishRequest) (queue.PublishResponse, error) {
	started := time.Now()
	response := queue.PublishResponse{
		Results: make([]queue.PublishResult, len(req.Mutations)),
	}
	if err := p.topics.Ping(ctx); err != nil {
		p.metrics.ObserveKafkaPublish(p.store, time.Since(started), 0, len(req.Mutations))
		for index := range response.Results {
			response.Results[index].Status = queue.PublishStatusFailed
			response.Results[index].Err = storage.BackendError(err)
		}
		return response, nil
	}
	records := make([]*kgo.Record, len(req.Mutations))
	for index, mutation := range req.Mutations {
		key, err := queue.MutationKey(mutation)
		if err != nil {
			response.Results[index] = queue.PublishResult{Status: queue.PublishStatusFailed, Err: storage.InvalidArgumentError(err)}
			continue
		}
		messageBytes := queue.MutationSize(mutation)
		if messageBytes > p.maxRecordBytes-len(key) {
			cause := fmt.Errorf("kafka mutation exceeds %d bytes including its address and expanded Lua source", p.maxRecordBytes)
			response.Results[index] = queue.PublishResult{Status: queue.PublishStatusFailed, Err: storage.InvalidArgumentError(cause)}
			continue
		}
		value, err := queue.MarshalMutation(mutation)
		if err != nil {
			response.Results[index] = queue.PublishResult{Status: queue.PublishStatusFailed, Err: storage.InvalidArgumentError(err)}
			continue
		}
		record := &kgo.Record{Topic: p.topic, Key: key, Value: value}
		records[index] = record
	}

	var pending sync.WaitGroup
	for index, record := range records {
		if record == nil {
			continue
		}
		pending.Add(1)
		p.client.TryProduce(ctx, record, func(_ *kgo.Record, err error) {
			response.Results[index].Err = err
			pending.Done()
		})
	}
	pending.Wait()
	accepted := 0
	for index := range response.Results {
		result := &response.Results[index]
		if result.Status == queue.PublishStatusFailed {
			continue
		}
		if result.Err != nil {
			result.Status = queue.PublishStatusFailed
			result.Err = publishError(result.Err)
			continue
		}
		result.Status = queue.PublishStatusAccepted
		accepted++
	}
	p.metrics.ObserveKafkaPublish(p.store, time.Since(started), accepted, len(response.Results)-accepted)
	level := slog.LevelDebug
	if accepted != len(response.Results) {
		level = slog.LevelWarn
	}
	slog.Log(ctx, level, "Kafka publication completed", "component", "kafka", "event", "kafka_published",
		"store", p.store, "topic", p.topic, "operations", len(response.Results), "failed", len(response.Results)-accepted,
		"duration_ms", time.Since(started).Milliseconds())
	return response, nil
}

func (p *Publisher) Ping(ctx context.Context) error {
	if err := p.topics.Ping(ctx); err != nil {
		return err
	}
	return p.client.Ping(ctx)
}

func (p *Publisher) Close() {
	p.client.Close()
}
