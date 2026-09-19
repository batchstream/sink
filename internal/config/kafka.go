package config

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

func resolveKafka(prefix string, file kafkaFile, v *validator) Kafka {
	var loaded Kafka
	loaded.Enabled = file.Enabled
	loaded.Brokers = nonEmptyValues(file.Brokers)
	topic := &loaded.Topic
	topic.Name = strings.TrimSpace(file.Topic.Name)
	topic.Partitions = v.bounded(prefix+".topic.partitions", file.Topic.Partitions, 4, 1<<31-1)
	topic.ReplicationFactor = v.bounded(prefix+".topic.replication_factor", file.Topic.ReplicationFactor, 2, 1<<15-1)
	topic.Retention = v.duration(prefix+".topic.retention", file.Topic.Retention, 72*time.Hour)
	topic.MinInSyncReplicas = v.bounded(prefix+".topic.min_insync_replicas", file.Topic.MinInSyncReplicas, min(2, topic.ReplicationFactor), topic.ReplicationFactor)

	topic.MaxRecordBytes = v.bytes(prefix+".topic.max_record_bytes", file.Topic.MaxRecordBytes, 900<<10, 64<<20)
	loaded.DeadLetter.Topic = strings.TrimSpace(file.DeadLetter.Topic)
	if topic.Name != "" && loaded.DeadLetter.Topic == "" {
		loaded.DeadLetter.Topic = topic.Name + ".dlq"
	}
	loaded.DeadLetter.Retention = v.duration(prefix+".dead_letter.retention", file.DeadLetter.Retention, 720*time.Hour)
	if topic.Retention < time.Millisecond {
		v.reject(fmt.Errorf("%s.topic.retention must be at least 1ms", prefix))
	}
	if loaded.DeadLetter.Retention < time.Millisecond {
		v.reject(fmt.Errorf("%s.dead_letter.retention must be at least 1ms", prefix))
	}
	return loaded
}

func resolveProducer(file *producerFile, v *validator) Producer {
	if file == nil {
		file = &producerFile{}
	}
	loaded := Producer{MaxBufferedBytes: v.bytes("producer.max_buffered_bytes", file.MaxBufferedBytes, 64<<20, 1<<30)}
	return loaded
}

func resolveConsumer(file *consumerFile, v *validator) Consumer {
	if file == nil {
		file = &consumerFile{}
	}
	var loaded Consumer
	consumer := &loaded
	consumer.GroupID = strings.TrimSpace(file.GroupID)
	consumer.MaxPollRecords = v.integer("consumer.max_poll_records", file.MaxPollRecords, 500)
	consumer.ProcessingTimeout = v.duration("consumer.processing_timeout", file.ProcessingTimeout, 20*time.Second)
	if consumer.ProcessingTimeout > 20*time.Second {
		v.reject(errors.New("consumer.processing_timeout must not exceed 20s"))
	}
	consumer.Retry.MaxAttempts = v.integer("consumer.retry.max_attempts", file.Retry.MaxAttempts, 10)
	consumer.Retry.Backoff = v.duration("consumer.retry.backoff", file.Retry.Backoff, 100*time.Millisecond)
	consumer.Retry.MaxBackoff = v.duration("consumer.retry.max_backoff", file.Retry.MaxBackoff, 10*time.Second)
	if consumer.Retry.MaxBackoff < consumer.Retry.Backoff {
		v.reject(errors.New("consumer.retry.max_backoff must be at least consumer.retry.backoff"))
	}
	// Worker retries double their delay before capping it. Reject durations
	// whose doubling would overflow into an immediate retry.
	if consumer.Retry.MaxBackoff > time.Duration(1<<63-1)/2 {
		v.reject(errors.New("consumer.retry.max_backoff is too large to double safely"))
	}
	return loaded
}

func validateKafkaResources(loaded Config) error {
	kafka := loaded.Storage.Kafka
	if !kafka.Enabled {
		if loaded.Mode == ModeWorker {
			return errors.New("worker requires kafka.enabled")
		}
		return nil
	}
	if len(kafka.Brokers) == 0 || kafka.Topic.Name == "" {
		return errors.New("kafka.brokers and kafka.topic.name are required")
	}
	if loaded.Mode == ModeWorker && kafka.Consumer.GroupID == "" {
		return errors.New("consumer.group_id is required in worker mode")
	}
	if kafka.DeadLetter.Topic == kafka.Topic.Name {
		return errors.New("kafka.dead_letter.topic must differ from topic.name")
	}
	return nil
}
