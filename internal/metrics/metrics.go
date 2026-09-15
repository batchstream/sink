// Package metrics exposes bounded-cardinality Prometheus metrics for Sink.
package metrics

import (
	"context"
	"fmt"
	"net/http"
	"time"

	sink "github.com/liran/sink/gen/sink"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

const namespace = "sink"

type Metrics struct {
	stores                 map[string]struct{}
	registry               *prometheus.Registry
	requests               *prometheus.CounterVec
	requestDuration        *prometheus.HistogramVec
	operationResults       *prometheus.CounterVec
	batcherBatches         *prometheus.CounterVec
	batcherOperations      *prometheus.HistogramVec
	batcherBytes           *prometheus.HistogramVec
	batcherQueueDuration   *prometheus.HistogramVec
	batcherExecution       *prometheus.HistogramVec
	batcherQueuedOps       *prometheus.GaugeVec
	batcherQueuedBytes     *prometheus.GaugeVec
	batcherRejected        *prometheus.CounterVec
	requestQueueDuration   *prometheus.HistogramVec
	writePhaseDuration     *prometheus.HistogramVec
	writeExecutionRounds   *prometheus.HistogramVec
	requestQueueExits      *prometheus.CounterVec
	writeSlowPhases        *prometheus.CounterVec
	mergeConflicts         *prometheus.CounterVec
	mergeExhausted         *prometheus.CounterVec
	mergeFoldedChains      *prometheus.CounterVec
	mergeFoldedOperations  *prometheus.CounterVec
	kafkaPublished         *prometheus.CounterVec
	kafkaPublishDuration   *prometheus.HistogramVec
	kafkaWorkerMutations   *prometheus.CounterVec
	kafkaWorkerRetries     *prometheus.CounterVec
	kafkaWorkerDeadLetters *prometheus.CounterVec
	admissionRequests      prometheus.Gauge
	admissionBytes         prometheus.Gauge
	admissionRejected      prometheus.Counter
	admissionPoolRequests  *prometheus.GaugeVec
	admissionPoolBytes     *prometheus.GaugeVec
	admissionPoolRejected  *prometheus.CounterVec
	scanQueuedRequests     *prometheus.GaugeVec
	scanQueuedBytes        *prometheus.GaugeVec
	scanAdmissionWait      *prometheus.HistogramVec
	storeExecutionBytes    *prometheus.GaugeVec
	workerLastPoll         *prometheus.GaugeVec
	workerLastCommit       *prometheus.GaugeVec
	workerOldest           *prometheus.GaugeVec
	workerPending          *prometheus.GaugeVec
	workerRecoveries       *prometheus.CounterVec
	workerFetchErrors      *prometheus.CounterVec
	workerDelivery         *prometheus.HistogramVec
	workerQuarantined      *prometheus.CounterVec
	workerOffsetGap        *prometheus.GaugeVec
}

type BatchObservation struct {
	Store             string
	Method            string
	Reason            string
	Operations        int
	Bytes             int
	QueueDuration     time.Duration
	ExecutionDuration time.Duration
}

func New(version string, storeNames ...string) (*Metrics, error) {
	stores := make(map[string]struct{}, len(storeNames))
	for _, store := range storeNames {
		if store != "" {
			stores[store] = struct{}{}
		}
	}
	requestOptions := prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "grpc_server",
		Name:      "requests_total",
		Help:      "Total number of completed Sink gRPC requests.",
	}
	requests := prometheus.NewCounterVec(requestOptions, []string{"store", "method", "code"})
	durationOptions := prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "grpc_server",
		Name:      "request_duration_seconds",
		Help:      "Duration of completed Sink gRPC requests in seconds.",
		Buckets:   prometheus.DefBuckets,
	}
	requestDuration := prometheus.NewHistogramVec(durationOptions, []string{"store", "method"})
	resultOptions := prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "grpc_server",
		Name:      "operation_results_total",
		Help:      "Total number of per-operation results returned by Sink gRPC requests.",
	}
	operationResults := prometheus.NewCounterVec(resultOptions, []string{"store", "method", "status"})
	batchOptions := prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "batcher",
		Name:      "batches_total",
		Help:      "Total number of synchronous batches by flush reason.",
	}
	batcherBatches := prometheus.NewCounterVec(batchOptions, []string{"store", "method", "reason"})
	batchOperationOptions := prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "batcher",
		Name:      "operations",
		Help:      "Number of operations in each synchronous batch.",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 11),
	}
	batcherOperations := prometheus.NewHistogramVec(batchOperationOptions, []string{"store", "method"})
	batchByteOptions := prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "batcher",
		Name:      "bytes",
		Help:      "Encoded request bytes represented by each synchronous batch.",
		Buckets:   prometheus.ExponentialBuckets(1024, 4, 9),
	}
	batcherBytes := prometheus.NewHistogramVec(batchByteOptions, []string{"store", "method"})
	batchQueueDurationOptions := prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "batcher",
		Name:      "queue_duration_seconds",
		Help:      "Oldest request queue duration before a synchronous batch starts.",
		Buckets:   prometheus.ExponentialBuckets(0.00025, 2, 12),
	}
	batcherQueueDuration := prometheus.NewHistogramVec(batchQueueDurationOptions, []string{"store", "method"})
	batchExecutionOptions := prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "batcher",
		Name:      "execution_duration_seconds",
		Help:      "Execution duration of synchronous batches.",
		Buckets:   prometheus.DefBuckets,
	}
	batcherExecution := prometheus.NewHistogramVec(batchExecutionOptions, []string{"store", "method"})
	queuedOperationOptions := prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: "batcher",
		Name:      "queued_operations",
		Help:      "Current number of synchronous operations waiting for execution.",
	}
	batcherQueuedOps := prometheus.NewGaugeVec(queuedOperationOptions, []string{"store", "method"})
	queuedByteOptions := prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: "batcher",
		Name:      "queued_bytes",
		Help:      "Current encoded request bytes waiting for synchronous execution.",
	}
	batcherQueuedBytes := prometheus.NewGaugeVec(queuedByteOptions, []string{"store", "method"})
	rejectedOptions := prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "batcher",
		Name:      "rejected_total",
		Help:      "Total number of synchronous requests rejected before batching.",
	}
	batcherRejected := prometheus.NewCounterVec(rejectedOptions, []string{"store", "method", "reason"})
	// Seven finite latency buckets retain the 5s/10s thresholds per store;
	// completion modes and queue exit outcomes do not multiply histogram buckets.
	latencyBuckets := []float64{0.001, 0.01, 0.1, 1, 5, 10, 30}
	requestQueueOptions := prometheus.HistogramOpts{
		Namespace: namespace, Subsystem: "batcher", Name: "request_queue_duration_seconds",
		Help:    "Queue residence of each RPC leaving the synchronous queue, including cancellation and shutdown.",
		Buckets: latencyBuckets,
	}
	requestQueueDuration := prometheus.NewHistogramVec(requestQueueOptions, []string{"store", "method"})
	queueExitOptions := prometheus.CounterOpts{
		Namespace: namespace, Subsystem: "batcher", Name: "request_queue_exits_total",
		Help: "RPCs leaving the synchronous queue by exit outcome.",
	}
	requestQueueExits := prometheus.NewCounterVec(queueExitOptions, []string{"store", "method", "outcome"})
	writePhaseOptions := prometheus.HistogramOpts{
		Namespace: namespace, Subsystem: "write", Name: "phase_duration_seconds",
		Help:    "Synchronous core write phase durations by configured store; storage_write_visible includes visibility waiting.",
		Buckets: latencyBuckets,
	}
	writePhaseDuration := prometheus.NewHistogramVec(writePhaseOptions, []string{"store", "phase"})
	slowPhaseOptions := prometheus.CounterOpts{
		Namespace: namespace, Subsystem: "write", Name: "slow_phases_total",
		Help: "Synchronous write phase observations exceeding 5 seconds, by configured store and phase.",
	}
	writeSlowPhases := prometheus.NewCounterVec(slowPhaseOptions, []string{"store", "phase"})
	roundOptions := prometheus.HistogramOpts{
		Namespace: namespace, Subsystem: "write", Name: "execution_rounds",
		Help:    "Storage read/write calls per synchronous core write execution, including conflict retries, by configured store.",
		Buckets: []float64{0, 1, 2, 4, 8, 16},
	}
	writeExecutionRounds := prometheus.NewHistogramVec(roundOptions, []string{"store", "phase"})
	mergeConflictOptions := prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "merge",
		Name:      "conflicts_total",
		Help:      "Total number of revision conflicts retried by Lua merges.",
	}
	mergeConflicts := prometheus.NewCounterVec(mergeConflictOptions, []string{"store"})
	mergeExhaustedOptions := prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "merge",
		Name:      "exhausted_total",
		Help:      "Total number of Lua merges that exhausted the configured revision-conflict attempts.",
	}
	mergeExhausted := prometheus.NewCounterVec(mergeExhaustedOptions, []string{"store"})
	foldedChainOptions := prometheus.CounterOpts{Namespace: namespace, Subsystem: "merge", Name: "folded_chains_total", Help: "Ordered merge runs with multiple operations planned for one conditional commit, excluding retries."}
	mergeFoldedChains := prometheus.NewCounterVec(foldedChainOptions, []string{"store"})
	foldedOperationOptions := prometheus.CounterOpts{Namespace: namespace, Subsystem: "merge", Name: "folded_operations_total", Help: "Logical operations in folded merge runs, excluding retries; not a commit success count."}
	mergeFoldedOperations := prometheus.NewCounterVec(foldedOperationOptions, []string{"store"})
	publishedOptions := prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "kafka_publisher",
		Name:      "records_total",
		Help:      "Total number of Kafka mutation records by publish result.",
	}
	kafkaPublished := prometheus.NewCounterVec(publishedOptions, []string{"store", "status"})
	publishDurationOptions := prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "kafka_publisher",
		Name:      "duration_seconds",
		Help:      "Duration of synchronous Kafka publish batches in seconds.",
		Buckets:   prometheus.DefBuckets,
	}
	kafkaPublishDuration := prometheus.NewHistogramVec(publishDurationOptions, []string{"store"})
	workerOptions := prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "kafka_worker",
		Name:      "mutations_total",
		Help:      "Total number of Kafka mutations by processing result.",
	}
	kafkaWorkerMutations := prometheus.NewCounterVec(workerOptions, []string{"store", "status"})
	retryOptions := prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "kafka_worker",
		Name:      "retries_total",
		Help:      "Total number of Kafka mutation retry attempts.",
	}
	kafkaWorkerRetries := prometheus.NewCounterVec(retryOptions, []string{"store"})
	deadLetterOptions := prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "kafka_worker",
		Name:      "dead_letters_total",
		Help:      "Total number of Kafka mutations published to the dead-letter topic.",
	}
	kafkaWorkerDeadLetters := prometheus.NewCounterVec(deadLetterOptions, []string{"store"})
	buildOptions := prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "build_info",
		Help:      "Sink build information.",
	}
	buildInfo := prometheus.NewGaugeVec(buildOptions, []string{"version"})
	buildInfo.WithLabelValues(version).Set(1)

	offsetGapOptions := prometheus.GaugeOpts{Namespace: namespace, Name: "kafka_worker_offset_gap", Help: "A committed source offset fell outside retention; explicit recovery is required."}
	workerOffsetGap := prometheus.NewGaugeVec(offsetGapOptions, []string{"store"})
	quarantineOptions := prometheus.CounterOpts{Namespace: namespace, Name: "kafka_worker_quarantined_total", Help: "Records durably quarantined by configured store."}
	workerQuarantined := prometheus.NewCounterVec(quarantineOptions, []string{"store"})
	registry := prometheus.NewRegistry()
	registry.MustRegister(workerQuarantined, workerOffsetGap)
	admissionRequestsOptions := prometheus.GaugeOpts{Namespace: namespace, Name: "in_flight_requests", Help: "Core requests currently executing, including cross-store and asynchronous requests."}
	admissionRequests := prometheus.NewGauge(admissionRequestsOptions)
	admissionBytesOptions := prometheus.GaugeOpts{Namespace: namespace, Name: "in_flight_bytes", Help: "Request and output bytes reserved by executing core requests."}
	admissionBytes := prometheus.NewGauge(admissionBytesOptions)
	admissionRejectedOptions := prometheus.CounterOpts{Namespace: namespace, Name: "admission_rejected_total", Help: "Requests rejected by global or configured-store execution limits."}
	admissionRejected := prometheus.NewCounter(admissionRejectedOptions)
	poolRequestsOptions := prometheus.GaugeOpts{Namespace: namespace, Name: "admission_pool_requests", Help: "Executing requests in each independently bounded admission pool."}
	admissionPoolRequests := prometheus.NewGaugeVec(poolRequestsOptions, []string{"store", "pool"})
	poolBytesOptions := prometheus.GaugeOpts{Namespace: namespace, Name: "admission_pool_bytes", Help: "Bytes reserved in each independently bounded admission pool."}
	admissionPoolBytes := prometheus.NewGaugeVec(poolBytesOptions, []string{"store", "pool"})
	poolRejectedOptions := prometheus.CounterOpts{Namespace: namespace, Name: "admission_pool_rejected_total", Help: "Admission rejections by pool and capacity reason."}
	admissionPoolRejected := prometheus.NewCounterVec(poolRejectedOptions, []string{"store", "pool", "reason"})
	scanQueueRequestsOptions := prometheus.GaugeOpts{Namespace: namespace, Name: "scan_queued_requests", Help: "Scan pages waiting for execution admission."}
	scanQueuedRequests := prometheus.NewGaugeVec(scanQueueRequestsOptions, []string{"store"})
	scanQueueBytesOptions := prometheus.GaugeOpts{Namespace: namespace, Name: "scan_queued_bytes", Help: "Conservative reservation bytes charged to the separate Scan waiting queue."}
	scanQueuedBytes := prometheus.NewGaugeVec(scanQueueBytesOptions, []string{"store"})
	scanWaitOptions := prometheus.HistogramOpts{Namespace: namespace, Name: "scan_admission_wait_duration_seconds", Help: "Time queued Scan pages waited before admission, rejection or cancellation.", Buckets: prometheus.DefBuckets}
	scanAdmissionWait := prometheus.NewHistogramVec(scanWaitOptions, []string{"store"})
	storeBytesOptions := prometheus.GaugeOpts{Namespace: namespace, Name: "execution_store_bytes", Help: "Execution reservation bytes charged to each store, including cross-store calls."}
	storeExecutionBytes := prometheus.NewGaugeVec(storeBytesOptions, []string{"store"})
	lastPollOptions := prometheus.GaugeOpts{Namespace: namespace, Name: "kafka_worker_last_poll_timestamp_seconds", Help: "Last completed Kafka poll by configured store."}
	workerLastPoll := prometheus.NewGaugeVec(lastPollOptions, []string{"store"})
	lastCommitOptions := prometheus.GaugeOpts{Namespace: namespace, Name: "kafka_worker_last_commit_timestamp_seconds", Help: "Last successful source offset commit by configured store."}
	workerLastCommit := prometheus.NewGaugeVec(lastCommitOptions, []string{"store"})
	oldestOptions := prometheus.GaugeOpts{Namespace: namespace, Name: "kafka_worker_oldest_pending_timestamp_seconds", Help: "Oldest timestamp in the current uncommitted fetch, zero after commit; use Kafka lag monitoring for unpolled backlog."}
	workerOldest := prometheus.NewGaugeVec(oldestOptions, []string{"store"})
	pendingOptions := prometheus.GaugeOpts{Namespace: namespace, Name: "kafka_worker_pending_records", Help: "Records in the current uncommitted fetch, including records retained for retry."}
	workerPending := prometheus.NewGaugeVec(pendingOptions, []string{"store"})
	recoveryOptions := prometheus.CounterOpts{Namespace: namespace, Name: "kafka_worker_recoveries_total", Help: "Batches retained for retry without advancing source offsets."}
	workerRecoveries := prometheus.NewCounterVec(recoveryOptions, []string{"store"})
	fetchErrorOptions := prometheus.CounterOpts{Namespace: namespace, Name: "kafka_worker_fetch_errors_total", Help: "Kafka fetch errors by configured store, including offset retention gaps."}
	workerFetchErrors := prometheus.NewCounterVec(fetchErrorOptions, []string{"store"})
	deliveryOptions := prometheus.HistogramOpts{Namespace: namespace, Name: "kafka_worker_delivery_seconds", Help: "Age of the oldest record when a batch is resolved and its source offsets commit.", Buckets: []float64{0.1, 1, 5, 10, 30, 60, 300, 1800, 3600, 86400}}
	workerDelivery := prometheus.NewHistogramVec(deliveryOptions, []string{"store"})
	processOptions := collectors.ProcessCollectorOpts{}
	registeredCollectors := []prometheus.Collector{
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(processOptions),
		buildInfo,
		requests,
		requestDuration,
		operationResults,
		batcherBatches,
		batcherOperations,
		batcherBytes,
		batcherQueueDuration,
		batcherExecution,
		batcherQueuedOps,
		batcherQueuedBytes,
		batcherRejected,
		requestQueueDuration, requestQueueExits, writePhaseDuration, writeSlowPhases, writeExecutionRounds,
		mergeConflicts,
		mergeExhausted,
		mergeFoldedChains,
		mergeFoldedOperations,
		kafkaPublished,
		kafkaPublishDuration,
		kafkaWorkerMutations,
		kafkaWorkerRetries,
		kafkaWorkerDeadLetters,
		admissionRequests,
		admissionBytes,
		admissionRejected,
		admissionPoolRequests, admissionPoolBytes, admissionPoolRejected,
		scanQueuedRequests, scanQueuedBytes, scanAdmissionWait,
		storeExecutionBytes,
		workerLastPoll, workerLastCommit, workerOldest, workerPending, workerRecoveries, workerFetchErrors, workerDelivery,
	}
	for _, collector := range registeredCollectors {
		if err := registry.Register(collector); err != nil {
			return nil, fmt.Errorf("register Prometheus collector: %w", err)
		}
	}
	metrics := &Metrics{
		admissionPoolRequests:  admissionPoolRequests,
		admissionPoolBytes:     admissionPoolBytes,
		admissionPoolRejected:  admissionPoolRejected,
		scanQueuedRequests:     scanQueuedRequests,
		scanQueuedBytes:        scanQueuedBytes,
		scanAdmissionWait:      scanAdmissionWait,
		storeExecutionBytes:    storeExecutionBytes,
		workerOffsetGap:        workerOffsetGap,
		workerQuarantined:      workerQuarantined,
		stores:                 stores,
		registry:               registry,
		requests:               requests,
		requestDuration:        requestDuration,
		operationResults:       operationResults,
		batcherBatches:         batcherBatches,
		batcherOperations:      batcherOperations,
		batcherBytes:           batcherBytes,
		batcherQueueDuration:   batcherQueueDuration,
		batcherExecution:       batcherExecution,
		batcherQueuedOps:       batcherQueuedOps,
		batcherQueuedBytes:     batcherQueuedBytes,
		batcherRejected:        batcherRejected,
		requestQueueDuration:   requestQueueDuration,
		writePhaseDuration:     writePhaseDuration,
		writeExecutionRounds:   writeExecutionRounds,
		requestQueueExits:      requestQueueExits,
		writeSlowPhases:        writeSlowPhases,
		mergeConflicts:         mergeConflicts,
		mergeExhausted:         mergeExhausted,
		mergeFoldedChains:      mergeFoldedChains,
		mergeFoldedOperations:  mergeFoldedOperations,
		kafkaPublished:         kafkaPublished,
		kafkaPublishDuration:   kafkaPublishDuration,
		kafkaWorkerMutations:   kafkaWorkerMutations,
		kafkaWorkerRetries:     kafkaWorkerRetries,
		kafkaWorkerDeadLetters: kafkaWorkerDeadLetters,
		admissionRequests:      admissionRequests,
		admissionBytes:         admissionBytes,
		admissionRejected:      admissionRejected,
		workerLastPoll:         workerLastPoll, workerLastCommit: workerLastCommit, workerOldest: workerOldest,
		workerPending: workerPending, workerRecoveries: workerRecoveries, workerFetchErrors: workerFetchErrors, workerDelivery: workerDelivery,
	}
	return metrics, nil
}

func (m *Metrics) AdjustAdmission(requests int, bytes int) {
	if m == nil {
		return
	}
	m.admissionRequests.Add(float64(requests))
	m.admissionBytes.Add(float64(bytes))
}

func (m *Metrics) ObserveAdmissionRejected() {
	if m == nil {
		return
	}
	m.admissionRejected.Inc()
}

func (m *Metrics) AdjustAdmissionPool(store string, pool string, requests int, bytes int) {
	if m == nil {
		return
	}
	m.AdjustAdmission(requests, bytes)
	m.admissionPoolRequests.WithLabelValues(m.storeLabel(store), pool).Add(float64(requests))
	m.admissionPoolBytes.WithLabelValues(m.storeLabel(store), pool).Add(float64(bytes))
}

func (m *Metrics) ObserveAdmissionPoolRejected(store string, pool string, reason string) {
	if m == nil {
		return
	}
	m.ObserveAdmissionRejected()
	m.admissionPoolRejected.WithLabelValues(m.storeLabel(store), pool, reason).Inc()
}

func (m *Metrics) AdjustScanQueue(store string, requests int, bytes int) {
	if m == nil {
		return
	}
	m.scanQueuedRequests.WithLabelValues(m.storeLabel(store)).Add(float64(requests))
	m.scanQueuedBytes.WithLabelValues(m.storeLabel(store)).Add(float64(bytes))
}

func (m *Metrics) ObserveScanAdmissionWait(store string, duration time.Duration) {
	if m == nil {
		return
	}
	m.scanAdmissionWait.WithLabelValues(m.storeLabel(store)).Observe(duration.Seconds())
}

func (m *Metrics) AdjustStoreExecutionBytes(store string, bytes int) {
	if m == nil {
		return
	}
	m.storeExecutionBytes.WithLabelValues(m.storeLabel(store)).Add(float64(bytes))
}

func (m *Metrics) ObserveWorkerPoll(store string, errors int) {
	if m == nil {
		return
	}
	m.workerLastPoll.WithLabelValues(m.storeLabel(store)).SetToCurrentTime()
	m.workerFetchErrors.WithLabelValues(m.storeLabel(store)).Add(float64(errors))
}

func (m *Metrics) SetWorkerPending(store string, oldest time.Time, count int) {
	if m == nil {
		return
	}
	timestamp := float64(0)
	if !oldest.IsZero() {
		timestamp = float64(oldest.UnixMilli()) / 1000
	}
	m.workerOldest.WithLabelValues(m.storeLabel(store)).Set(timestamp)
	m.workerPending.WithLabelValues(m.storeLabel(store)).Set(float64(count))
}

func (m *Metrics) ObserveWorkerRecovery(store string) {
	if m == nil {
		return
	}
	m.workerRecoveries.WithLabelValues(m.storeLabel(store)).Inc()
}

func (m *Metrics) ObserveWorkerCommitted(store string, oldest time.Time) {
	if m == nil {
		return
	}
	m.workerLastCommit.WithLabelValues(m.storeLabel(store)).SetToCurrentTime()
	if !oldest.IsZero() {
		m.workerDelivery.WithLabelValues(m.storeLabel(store)).Observe(max(0, time.Since(oldest).Seconds()))
	}
	var cleared time.Time
	m.SetWorkerPending(store, cleared, 0)
}

func (m *Metrics) ObserveMergeConflict(store string, count int) {
	if m == nil || count <= 0 {
		return
	}
	m.mergeConflicts.WithLabelValues(m.storeLabel(store)).Add(float64(count))
}

func (m *Metrics) ObserveMergeFold(store string, operations int) {
	if m == nil || operations < 2 {
		return
	}
	m.mergeFoldedChains.WithLabelValues(m.storeLabel(store)).Inc()
	m.mergeFoldedOperations.WithLabelValues(m.storeLabel(store)).Add(float64(operations))
}

func (m *Metrics) ObserveMergeExhausted(store string, count int) {
	if m == nil || count <= 0 {
		return
	}
	m.mergeExhausted.WithLabelValues(m.storeLabel(store)).Add(float64(count))
}

func (m *Metrics) AdjustBatchQueue(store string, method string, operations int, bytes int) {
	if m == nil {
		return
	}
	m.batcherQueuedOps.WithLabelValues(m.storeLabel(store), method).Add(float64(operations))
	m.batcherQueuedBytes.WithLabelValues(m.storeLabel(store), method).Add(float64(bytes))
}

func (m *Metrics) ObserveBatch(observation BatchObservation) {
	if m == nil {
		return
	}
	m.batcherBatches.WithLabelValues(m.storeLabel(observation.Store), observation.Method, observation.Reason).Inc()
	m.batcherOperations.WithLabelValues(m.storeLabel(observation.Store), observation.Method).Observe(float64(observation.Operations))
	m.batcherBytes.WithLabelValues(m.storeLabel(observation.Store), observation.Method).Observe(float64(observation.Bytes))
	m.batcherQueueDuration.WithLabelValues(m.storeLabel(observation.Store), observation.Method).Observe(observation.QueueDuration.Seconds())
	m.batcherExecution.WithLabelValues(m.storeLabel(observation.Store), observation.Method).Observe(observation.ExecutionDuration.Seconds())
}

func (m *Metrics) ObserveBatchRejected(store string, method string, reason string) {
	if m == nil {
		return
	}
	m.batcherRejected.WithLabelValues(m.storeLabel(store), method, reason).Inc()
}

func (m *Metrics) ObserveKafkaPublish(store string, duration time.Duration, accepted int, failed int) {
	if m == nil {
		return
	}
	m.kafkaPublishDuration.WithLabelValues(m.storeLabel(store)).Observe(duration.Seconds())
	if accepted > 0 {
		m.kafkaPublished.WithLabelValues(m.storeLabel(store), "accepted").Add(float64(accepted))
	}
	if failed > 0 {
		m.kafkaPublished.WithLabelValues(m.storeLabel(store), "failed").Add(float64(failed))
	}
}

func (m *Metrics) ObserveKafkaWorker(store string, status string, count int) {
	if m == nil || count <= 0 {
		return
	}
	m.kafkaWorkerMutations.WithLabelValues(m.storeLabel(store), status).Add(float64(count))
}

func (m *Metrics) ObserveKafkaRetry(store string, count int) {
	if m == nil || count <= 0 {
		return
	}
	m.kafkaWorkerRetries.WithLabelValues(m.storeLabel(store)).Add(float64(count))
}

func (m *Metrics) ObserveKafkaDeadLetter(store string, count int) {
	if m == nil || count <= 0 {
		return
	}
	m.kafkaWorkerDeadLetters.WithLabelValues(m.storeLabel(store)).Add(float64(count))
}

func (m *Metrics) Handler() http.Handler {
	handlerOptions := promhttp.HandlerOpts{EnableOpenMetrics: true}
	return promhttp.HandlerFor(m.registry, handlerOptions)
}

func (m *Metrics) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	interceptor := func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		method, observed := sinkMethod(info.FullMethod)
		if !observed {
			return handler(ctx, req)
		}

		store := m.RequestStore(req)
		started := time.Now()
		response, err := handler(ctx, req)
		code := status.Code(err).String()
		m.requests.WithLabelValues(store, method, code).Inc()
		m.requestDuration.WithLabelValues(m.storeLabel(store), method).Observe(time.Since(started).Seconds())
		if err == nil {
			m.observeOperationResults(method, req, response)
		}
		return response, err
	}
	return interceptor
}

func sinkMethod(fullMethod string) (string, bool) {
	switch fullMethod {
	case sink.Sink_Read_FullMethodName:
		return "Read", true
	case sink.Sink_Write_FullMethodName:
		return "Write", true
	case sink.Sink_Delete_FullMethodName:
		return "Delete", true
	case sink.Sink_Execute_FullMethodName:
		return "Execute", true
	case sink.Sink_Query_FullMethodName:
		return "Query", true
	case sink.Sink_Count_FullMethodName:
		return "Count", true
	case sink.Sink_Scan_FullMethodName:
		return "Scan", true
	default:
		return "", false
	}
}

func (m *Metrics) StreamServerInterceptor() grpc.StreamServerInterceptor {
	interceptor := func(server any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		method, observed := sinkMethod(info.FullMethod)
		if !observed {
			return handler(server, stream)
		}
		store := unconfiguredStore
		started := time.Now()
		err := handler(server, stream)
		m.requests.WithLabelValues(store, method, status.Code(err).String()).Inc()
		m.requestDuration.WithLabelValues(m.storeLabel(store), method).Observe(time.Since(started).Seconds())
		return err
	}
	return interceptor
}

func (m *Metrics) observeOperationResults(method string, request any, response any) {
	switch typed := response.(type) {
	case *sink.ReadResponse:
		req, _ := request.(*sink.ReadRequest)
		for index, result := range typed.GetResults() {
			m.operationResults.WithLabelValues(operationStore(m, req.GetOperations(), index), method, readStatus(result.GetStatus())).Inc()
		}
	case *sink.WriteResponse:
		req, _ := request.(*sink.WriteRequest)
		for index, result := range typed.GetResults() {
			m.operationResults.WithLabelValues(operationStore(m, req.GetOperations(), index), method, writeStatus(result.GetStatus())).Inc()
		}
	case *sink.DeleteResponse:
		req, _ := request.(*sink.DeleteRequest)
		for index, result := range typed.GetResults() {
			m.operationResults.WithLabelValues(operationStore(m, req.GetOperations(), index), method, deleteStatus(result.GetStatus())).Inc()
		}
	case *sink.ExecuteResponse:
		result := "failed"
		if typed.GetSuccess() {
			result = "succeeded"
		}
		m.operationResults.WithLabelValues(m.RequestStore(request), method, result).Inc()
	}
}

func readStatus(value sink.ReadStatus) string {
	switch value {
	case sink.ReadStatus_READ_STATUS_FOUND:
		return "found"
	case sink.ReadStatus_READ_STATUS_NOT_FOUND:
		return "not_found"
	case sink.ReadStatus_READ_STATUS_FAILED:
		return "failed"
	default:
		return "unspecified"
	}
}

func writeStatus(value sink.WriteStatus) string {
	switch value {
	case sink.WriteStatus_WRITE_STATUS_APPLIED:
		return "applied"
	case sink.WriteStatus_WRITE_STATUS_ACCEPTED:
		return "accepted"
	case sink.WriteStatus_WRITE_STATUS_PRECONDITION_FAILED:
		return "precondition_failed"
	case sink.WriteStatus_WRITE_STATUS_FAILED:
		return "failed"
	default:
		return "unspecified"
	}
}

func deleteStatus(value sink.DeleteStatus) string {
	switch value {
	case sink.DeleteStatus_DELETE_STATUS_APPLIED:
		return "applied"
	case sink.DeleteStatus_DELETE_STATUS_ACCEPTED:
		return "accepted"
	case sink.DeleteStatus_DELETE_STATUS_FAILED:
		return "failed"
	default:
		return "unspecified"
	}
}

func (m *Metrics) ObserveQuarantined(store string) {
	if m != nil {
		m.workerQuarantined.WithLabelValues(m.storeLabel(store)).Inc()
	}
}

func (m *Metrics) SetWorkerOffsetGap(store string, gap bool) {
	if m == nil {
		return
	}
	value := 0.0
	if gap {
		value = 1
	}
	m.workerOffsetGap.WithLabelValues(m.storeLabel(store)).Set(value)
}
