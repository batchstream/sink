package kafka

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	sink "github.com/liran/sink/gen/sink"
	sinkmetrics "github.com/liran/sink/internal/metrics"
	"github.com/liran/sink/internal/queue"
	"github.com/liran/sink/internal/storage"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestPublisherPreservesMixedResultsAndRecordOrder(t *testing.T) {
	for _, closed := range []bool{false, true} {
		t.Run(fmt.Sprintf("closed=%t", closed), func(t *testing.T) {
			const topic = "publisher-results"
			cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(cluster.Close)
			observed, err := sinkmetrics.New("test", "primary")
			if err != nil {
				t.Fatal(err)
			}
			opts := PublisherOptions{Store: "primary", Brokers: cluster.ListenAddrs(), Topic: topic, Metrics: observed, MaxRecordBytes: 1024}
			publisher, err := NewPublisher(opts)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(publisher.Close)
			if closed {
				publisher.Close()
			}
			first := reliabilityPut(sink.WriteMode_WRITE_MODE_UPSERT, `{"value":1}`)
			second := reliabilityPut(sink.WriteMode_WRITE_MODE_UPSERT, `{"value":2}`)
			oversized := reliabilityPut(sink.WriteMode_WRITE_MODE_UPSERT, strings.Repeat("x", 1024))
			deletion := &sink.DeleteOperation{Address: reliabilityAddress()}
			last := queue.Mutation{Delete: deletion}
			invalid := queue.Mutation{}
			request := queue.PublishRequest{Mutations: []queue.Mutation{invalid, first, oversized, second, invalid, last}}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			response, err := publisher.Publish(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			if len(response.Results) != len(request.Mutations) {
				t.Fatalf("got %d results for %d mutations", len(response.Results), len(request.Mutations))
			}
			for _, index := range []int{0, 2, 4} {
				result := response.Results[index]
				code, retryable := storage.ErrorDetails(result.Err)
				if result.Status != queue.PublishStatusFailed || code != storage.ErrorCodeInvalidArgument || retryable {
					t.Fatalf("invalid mutation %d: %+v", index, result)
				}
			}
			for _, index := range []int{1, 3, 5} {
				result := response.Results[index]
				if closed {
					if result.Status != queue.PublishStatusFailed || !errors.Is(result.Err, kgo.ErrClientClosed) {
						t.Fatalf("closed publisher result %d: %+v", index, result)
					}
				} else if result.Status != queue.PublishStatusAccepted || result.Err != nil {
					t.Fatalf("accepted mutation %d: %+v", index, result)
				}
			}
			accepted, failed := 3, 3
			if closed {
				accepted, failed = 0, 6
			}
			recorder := httptest.NewRecorder()
			observed.Handler().ServeHTTP(recorder, httptest.NewRequest("GET", "/metrics", nil))
			metricLines := []string{
				fmt.Sprintf(`sink_kafka_publisher_records_total{status="failed",store="primary"} %d`, failed),
				`sink_kafka_publisher_duration_seconds_count{store="primary"} 1`,
			}
			if accepted > 0 {
				metricLines = append(metricLines, fmt.Sprintf(`sink_kafka_publisher_records_total{status="accepted",store="primary"} %d`, accepted))
			}
			for _, line := range metricLines {
				if !strings.Contains(recorder.Body.String(), line) {
					t.Errorf("missing metric: %s", line)
				}
			}
			if closed {
				return
			}
			consumer, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(consumer.Close)
			var records []*kgo.Record
			for len(records) < accepted {
				fetches := consumer.PollRecords(ctx, accepted-len(records))
				if err := fetches.Err(); err != nil {
					t.Fatal(err)
				}
				records = append(records, fetches.Records()...)
			}
			for index, mutation := range []queue.Mutation{first, second, last} {
				want, err := queue.MarshalMutation(mutation)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(records[index].Value, want) || string(records[index].Key) != reliabilityAddress().GetUri() {
					t.Fatalf("record %d changed payload, key, or order", index)
				}
			}
		})
	}
}

func BenchmarkPublisherBatch(b *testing.B) {
	for _, closed := range []bool{false, true} {
		for _, count := range []int{1, 128, 1024} {
			b.Run(fmt.Sprintf("closed=%t/operations=%d", closed, count), func(b *testing.B) {
				const topic = "publisher-benchmark"
				cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(cluster.Close)
				opts := PublisherOptions{Brokers: cluster.ListenAddrs(), Topic: topic}
				publisher, err := NewPublisher(opts)
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(publisher.Close)
				want := queue.PublishStatusAccepted
				if closed {
					publisher.Close()
					want = queue.PublishStatusFailed
				}
				request := queue.PublishRequest{Mutations: make([]queue.Mutation, count)}
				for index := range request.Mutations {
					request.Mutations[index] = reliabilityPut(sink.WriteMode_WRITE_MODE_UPSERT, `{"value":1}`)
				}
				ctx, cancel := context.WithTimeout(b.Context(), time.Minute)
				defer cancel()
				if _, err := publisher.Publish(ctx, request); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				for b.Loop() {
					response, err := publisher.Publish(ctx, request)
					if err != nil {
						b.Fatal(err)
					}
					for index, result := range response.Results {
						if result.Status != want {
							b.Fatalf("result %d: %+v", index, result)
						}
					}
				}
			})
		}
	}
}
