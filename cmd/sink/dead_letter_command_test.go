package main

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/config"
	"github.com/liran/sink/internal/queue"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestDeadLetterReplayPreservesPublisherRouting(t *testing.T) {
	for _, driver := range []config.Driver{config.DriverMongoDB, config.DriverElasticsearch, config.DriverOpenSearch} {
		for _, kind := range []string{"write", "delete"} {
			t.Run(string(driver)+"/"+kind, func(t *testing.T) {
				cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(3, "source", "source.dlq"))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(cluster.Close)
				partitions := map[int32]kgo.Offset{0: kgo.NewOffset().AtStart(), 1: kgo.NewOffset().AtStart(), 2: kgo.NewOffset().AtStart()}
				topics := map[string]map[int32]kgo.Offset{"source": partitions}
				opts := []kgo.Opt{kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.ConsumePartitions(topics)}
				client, err := kgo.NewClient(opts...)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(client.Close)
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				defer cancel()
				keyValue := &sink.RecordKey_StringValue{StringValue: "same-record"}
				key := &sink.RecordKey{Kind: keyValue}
				address := &sink.RecordAddress{Store: "primary", Namespace: "tenant", Dataset: "documents", Key: key}
				mutation := queue.Mutation{}
				if kind == "write" {
					document := &sink.Document{Encoding: sink.DocumentEncoding_DOCUMENT_ENCODING_JSON, Payload: []byte(`{}`)}
					if driver == config.DriverMongoDB {
						document.Encoding = sink.DocumentEncoding_DOCUMENT_ENCODING_BSON
						document.Payload = []byte{5, 0, 0, 0, 0}
					}
					put := &sink.PutOperation{Mode: sink.WriteMode_WRITE_MODE_UPSERT, Document: document}
					action := &sink.WriteOperation_Put{Put: put}
					mutation.Write = &sink.WriteOperation{Address: address, Action: action}
				} else {
					mutation.Delete = &sink.DeleteOperation{Address: address}
				}
				partitionKey, err := queue.MutationKey(mutation)
				if driver != config.DriverMongoDB {
					partitionKey, err = queue.MutationKeyWithoutNamespace(mutation)
				}
				if err != nil {
					t.Fatal(err)
				}
				payload, err := queue.MarshalMutation(mutation)
				if err != nil {
					t.Fatal(err)
				}
				original := &kgo.Record{Topic: "source", Key: partitionKey, Value: payload}
				letter := &kgo.Record{Topic: "source.dlq", Key: partitionKey, Value: payload}
				if err := client.ProduceSync(ctx, original, letter).FirstErr(); err != nil {
					t.Fatal(err)
				}
				backend := "  search:\n    endpoints: [http://127.0.0.1:1]\n"
				if driver == config.DriverMongoDB {
					backend = "  mongodb:\n    uri: mongodb://127.0.0.1:1\n"
				}
				contents := fmt.Sprintf(`mode: engine
storage:
  name: primary
  database_id: test-database
  driver: %s
%s  kafka:
    enabled: true
    brokers: [%q]
    topic:
      name: source
      replication_factor: 1
      min_insync_replicas: 1
    dead_letter:
      topic: source.dlq
`, driver, backend, cluster.ListenAddrs()[0])
				configPath := writeConfig(t, contents)
				args := []string{"replay", "--config", configPath, "--store", "primary", "--partition", strconv.Itoa(int(letter.Partition)), "--offset", strconv.FormatInt(letter.Offset, 10)}
				var stdout, stderr bytes.Buffer
				if err := runDeadLetterCommand(args, &stdout, &stderr); err != nil {
					t.Fatalf("replay: %v stdout=%s stderr=%s", err, &stdout, &stderr)
				}
				seen := 0
				for seen < 2 {
					fetches := client.PollRecords(ctx, 2-seen)
					if err := fetches.Err(); err != nil {
						t.Fatal(err)
					}
					for _, record := range fetches.Records() {
						seen++
						if !bytes.Equal(record.Key, partitionKey) || record.Partition != original.Partition || !bytes.Equal(record.Value, payload) {
							t.Fatalf("replay changed routing or mutation: partition=%d want=%d key=%x want=%x", record.Partition, original.Partition, record.Key, partitionKey)
						}
					}
				}
			})
		}
	}
}
