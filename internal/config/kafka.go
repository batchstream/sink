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
	loaded.Producer.MaxBufferedBytes = v.bytes(prefix+".producer.max_buffered_bytes", file.Producer.MaxBufferedBytes, 64<<20, 1<<30)
	topic.MaxRecordBytes = v.bytes(prefix+".topic.max_record_bytes", file.Topic.MaxRecordBytes, 900<<10, min(64<<20, loaded.Producer.MaxBufferedBytes))
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
	consumer := &loaded.Consumer
	consumer.GroupID = strings.TrimSpace(file.Consumer.GroupID)
	consumer.MaxPollRecords = v.integer(prefix+".consumer.max_poll_records", file.Consumer.MaxPollRecords, 500)
	consumer.ProcessingTimeout = v.duration(prefix+".consumer.processing_timeout", file.Consumer.ProcessingTimeout, 20*time.Second)
	if consumer.ProcessingTimeout > 20*time.Second {
		v.reject(fmt.Errorf("%s.consumer.processing_timeout must not exceed 20s", prefix))
	}
	consumer.Retry.MaxAttempts = v.integer(prefix+".consumer.retry.max_attempts", file.Consumer.Retry.MaxAttempts, 10)
	consumer.Retry.Backoff = v.duration(prefix+".consumer.retry.backoff", file.Consumer.Retry.Backoff, 100*time.Millisecond)
	consumer.Retry.MaxBackoff = v.duration(prefix+".consumer.retry.max_backoff", file.Consumer.Retry.MaxBackoff, 10*time.Second)
	if consumer.Retry.MaxBackoff < consumer.Retry.Backoff {
		v.reject(fmt.Errorf("%s.consumer.retry.max_backoff must be at least %s.consumer.retry.backoff", prefix, prefix))
	}
	// Worker retries double their delay before capping it. Reject durations
	// whose doubling would overflow into an immediate retry.
	if consumer.Retry.MaxBackoff > time.Duration(1<<63-1)/2 {
		v.reject(fmt.Errorf("%s.consumer.retry.max_backoff is too large to double safely", prefix))
	}
	return loaded
}

func validateKafkaResources(loaded Config) error {
	kafka := loaded.Storage.Kafka
	if !kafka.Enabled {
		if loaded.Mode == ModeWorker {
			return errors.New("worker requires storage.kafka.enabled")
		}
		return nil
	}
	if len(kafka.Brokers) == 0 || kafka.Topic.Name == "" {
		return errors.New("storage.kafka.brokers and storage.kafka.topic.name are required")
	}
	if loaded.Mode == ModeWorker && kafka.Consumer.GroupID == "" {
		return errors.New("storage.kafka.consumer.group_id is required in worker mode")
	}
	if kafka.DeadLetter.Topic == kafka.Topic.Name {
		return errors.New("storage.kafka.dead_letter.topic must differ from topic.name")
	}
	return nil
}
