package service

import (
	"context"
	"slices"
	"sync"
	"time"

	"github.com/liran/sink/internal/protocol"

	sink "github.com/liran/sink/gen/sink"
	sinkmetrics "github.com/liran/sink/internal/metrics"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const defaultRequestTimeout = 30 * time.Second

// Publishing has its own bounded pool so storage latency and snapshot waiters
// cannot consume the capacity needed to durably enqueue asynchronous work.
type admissionPool struct {
	name                string
	metrics             *sinkmetrics.Metrics
	requestTimeout      time.Duration
	maxInFlightRequests int
	maxInFlightBytes    int
	admissionMu         sync.Mutex
	admissionWaiters    []*admissionRequest
	inFlightRequests    int
	inFlightBytes       int
	maxScanRequests     int
	maxScanBytes        int
	scanRequests        int
	scanBytes           int
	scanAdmissionWait   time.Duration
	maxQueuedRequests   int
	maxQueuedBytes      int
	admissionWait       time.Duration
	queuedRequests      int
	queuedBytes         int
}

type admissionRequest struct {
	encodedBytes int
	stores       []string
	wait         bool
	timeout      time.Duration
	scan         bool
	publish      bool
	reservation  *admissionReservation
	inputBytes   int
	direct       bool
	ready        chan struct{}
}

func (s *Server) admitRequest(ctx context.Context, request admissionRequest) (context.Context, context.CancelFunc, error) {
	if request.publish {
		return s.publishAdmission.admitRequest(ctx, request)
	}
	if !request.wait && !request.scan && request.inputBytes > 0 {
		request.wait = true
		request.direct = true
		// Charge decoded input and per-call bookkeeping, not hypothetical
		// response buffers that are only allocated after execution admission.
		request.inputBytes += 256
	}
	return s.admissionPool.admitRequest(ctx, request)
}

func (s *admissionPool) admitRequest(ctx context.Context, request admissionRequest) (context.Context, context.CancelFunc, error) {
	started := time.Now()
	store := s.metrics.RequestStores(request.stores)
	waitCtx := ctx
	if request.scan && request.wait {
		var cancel context.CancelFunc
		waitCtx, cancel = context.WithTimeout(ctx, s.scanAdmissionWait)
		defer cancel()
	}
	if request.direct {
		var cancel context.CancelFunc
		waitCtx, cancel = context.WithTimeout(ctx, s.admissionWait)
		defer cancel()
	}
	var queued *admissionRequest
	var scanQueuedAt time.Time
	var directQueuedAt time.Time
	defer func() {
		if queued != nil {
			s.admissionMu.Lock()
			s.removeAdmissionWaiter(queued)
			s.admissionMu.Unlock()
		}
		if !scanQueuedAt.IsZero() {
			s.metrics.ObserveScanAdmissionWait(store, time.Since(scanQueuedAt))
		}
		if !directQueuedAt.IsZero() {
			s.metrics.ObserveExecutionAdmissionWait(store, time.Since(directQueuedAt))
		}
	}()
	for {
		if err := contextError(ctx); err != nil {
			return ctx, nil, err
		}
		if queued != nil && waitCtx.Err() != nil {
			if err := contextError(ctx); err != nil {
				return ctx, nil, err
			}
			return ctx, nil, s.rejectAdmission(request, "wait_timeout")
		}
		s.admissionMu.Lock()
		full := s.admissionSlotsFull(request) || request.encodedBytes > s.maxInFlightBytes-s.inFlightBytes
		reason := "requests"
		if request.encodedBytes > s.maxInFlightBytes-s.inFlightBytes {
			reason = "bytes"
		}
		for _, earlier := range s.admissionWaiters {
			if earlier == queued {
				break
			}
			// A runnable older batch reserves the next available byte capacity.
			// Otherwise small arrivals can indefinitely starve returned-document
			// batches. A busy scan pool must still let ordinary requests run.
			if !s.admissionSlotsFull(*earlier) {
				if !full {
					reason = "fairness"
				}
				full = true
				break
			}
		}
		if full {
			canWait := request.wait && s.admissionCanFit(request)
			if canWait && queued == nil {
				if request.direct && s.directWaitQueueFull(request) {
					s.admissionMu.Unlock()
					return ctx, nil, s.rejectAdmission(request, "queue")
				}
				if request.scan && s.scanWaitQueueFull(request) {
					s.admissionMu.Unlock()
					return ctx, nil, s.rejectAdmission(request, "queue")
				}
				request.ready = make(chan struct{}, 1)
				queued = &request
				s.admissionWaiters = append(s.admissionWaiters, queued)
				if request.direct {
					directQueuedAt = time.Now()
					s.adjustDirectQueue(request, 1)
				}
				if request.scan {
					scanQueuedAt = time.Now()
					s.metrics.AdjustScanQueue(store, 1, request.encodedBytes)
				}
			}
			changed := request.ready
			s.admissionMu.Unlock()
			if canWait {
				select {
				case <-changed:
					continue
				case <-waitCtx.Done():
					if err := contextError(ctx); err != nil {
						return ctx, nil, err
					}
					return ctx, nil, s.rejectAdmission(request, "wait_timeout")
				}
			}
			return ctx, nil, s.rejectAdmission(request, reason)
		}
		s.inFlightRequests++
		s.inFlightBytes += request.encodedBytes
		s.adjustStoreBytes(request.stores, request.encodedBytes)
		if request.reservation != nil {
			request.reservation.pool = s
			request.reservation.stores = request.stores
			request.reservation.bytes = request.encodedBytes
			request.reservation.maximum = request.encodedBytes
		}
		if request.scan {
			s.scanRequests++
			s.scanBytes += request.encodedBytes
		}
		if queued != nil {
			s.removeAdmissionWaiter(queued)
			queued = nil
		}
		s.admissionMu.Unlock()
		break
	}
	s.metrics.AdjustAdmissionPool(store, s.name, 1, request.encodedBytes)
	timeout := request.timeout
	if timeout == 0 {
		timeout = s.requestTimeout
	}
	if request.direct {
		timeout -= time.Since(started)
	}
	execution, cancel := context.WithTimeout(ctx, timeout)
	release := func() {
		cancel()
		s.admissionMu.Lock()
		bytes := request.encodedBytes
		if request.reservation != nil {
			bytes = request.reservation.bytes
			request.reservation.released = true
		}
		s.inFlightRequests--
		s.inFlightBytes -= bytes
		s.adjustStoreBytes(request.stores, -bytes)
		if request.scan {
			s.scanRequests--
			s.scanBytes -= request.encodedBytes
		}
		s.wakeAdmissionWaiter()
		s.admissionMu.Unlock()
		s.metrics.AdjustAdmissionPool(store, s.name, -1, -bytes)
	}
	return execution, release, nil
}

func (s *admissionPool) admissionCanFit(request admissionRequest) bool {
	return request.encodedBytes <= s.maxInFlightBytes && (!request.scan || request.encodedBytes <= s.maxScanBytes)
}

// The caller holds admissionMu. The execution pool belongs to one Store.
func (s *admissionPool) adjustStoreBytes(stores []string, bytes int) {
	if s.name == "execution" {
		s.metrics.AdjustStoreExecutionBytes(s.metrics.RequestStores(stores), bytes)
	}
}

// The caller holds admissionMu. Queued scans have separate count and byte
// bounds equal to the execution scan sublimits. Charge the full
// reservation conservatively, including input, without taking execution slots.
func (s *admissionPool) scanWaitQueueFull(request admissionRequest) bool {
	requests, bytes := 0, 0
	for _, queued := range s.admissionWaiters {
		if !queued.scan {
			continue
		}
		requests++
		bytes += queued.encodedBytes
	}
	return requests >= s.maxScanRequests || request.encodedBytes > s.maxScanBytes-bytes
}

func (s *admissionPool) rejectAdmission(request admissionRequest, reason string) error {
	store := s.metrics.RequestStores(request.stores)
	s.metrics.ObserveAdmissionPoolRejected(store, s.name, reason)
	rejection := status.New(codes.ResourceExhausted, "Sink "+s.name+" capacity is full")
	if request.scan && s.admissionCanFit(request) {
		// This detail is emitted only before backend execution and only for
		// temporary pressure. Oversized requests and backend/page failures must
		// never acquire this retry signal.
		detail := &errdetails.ErrorInfo{Domain: "sink", Reason: "SCAN_ADMISSION_REJECTED",
			Metadata: map[string]string{"pool": s.name, "reason": reason}}
		if detailed, err := rejection.WithDetails(detail); err == nil {
			rejection = detailed
		}
	}
	return rejection.Err()
}

// The caller holds admissionMu. Global bytes are handled separately so a
// waiting large request can accumulate space without reserving a store slot.
func (s *admissionPool) admissionSlotsFull(request admissionRequest) bool {
	if s.inFlightRequests >= s.maxInFlightRequests {
		return true
	}
	if request.scan && (s.scanRequests >= s.maxScanRequests || request.encodedBytes > s.maxScanBytes-s.scanBytes) {
		return true
	}
	return false
}

func (s *admissionPool) removeAdmissionWaiter(request *admissionRequest) {
	index := slices.Index(s.admissionWaiters, request)
	if index < 0 {
		return
	}
	s.admissionWaiters = slices.Delete(s.admissionWaiters, index, index+1)
	if request.direct {
		s.adjustDirectQueue(*request, -1)
	}
	if request.scan {
		store := s.metrics.RequestStores(request.stores)
		s.metrics.AdjustScanQueue(store, -1, -request.encodedBytes)
	}
	s.wakeAdmissionWaiter()
}

// The caller holds admissionMu. Hand capacity to the oldest eligible waiter;
// admission or cancellation of that waiter wakes the next one. Broadcasting
// every release makes all queued RPCs contend for the same lock and CPU quota.
func (s *admissionPool) wakeAdmissionWaiter() {
	for _, request := range s.admissionWaiters {
		if s.admissionSlotsFull(*request) {
			continue
		}
		if request.encodedBytes <= s.maxInFlightBytes-s.inFlightBytes {
			select {
			case request.ready <- struct{}{}:
			default:
			}
		}
		// Reserve accumulating global bytes for this older eligible request.
		return
	}
}

func operationStores[T interface{ GetAddress() *sink.RecordAddress }](operations []T) []string {
	seen := make(map[string]struct{})
	for _, operation := range operations {
		seen[protocol.RecordStore(operation.GetAddress())] = struct{}{}
	}
	stores := make([]string, 0, len(seen))
	for name := range seen {
		stores = append(stores, name)
	}
	return stores
}

// Include retained output and expanded source copies, before parsing or cloning.
// This bounds admitted payload bytes; VM/driver overhead is sized separately.
type writeExecutionEstimate struct {
	bytes        int
	workingBytes int
}

func (s *Server) estimateWriteExecution(req *sink.WriteRequest, callers int, returningCallers int) writeExecutionEstimate {
	estimate := writeExecutionEstimate{}
	bytes := req.SizeVT() + failureResponseBytes(len(req.GetOperations()))
	bytes += s.estimateWriteReturns(req, returningCallers)
	largestSource := 0
	for _, program := range req.GetLuaPrograms() {
		largestSource = max(largestSource, len(program.GetSource()))
	}
	hasSnapshot := false
	hasConditionalPut := false
	if len(req.GetOperations()) > 1 {
		for _, operation := range req.GetOperations() {
			if operation.GetPut() != nil && operation.GetPut().GetMode() != sink.WriteMode_WRITE_MODE_UPSERT {
				hasConditionalPut = true
				break
			}
		}
	}
	type putRun struct {
		count       int
		conditional bool
	}
	puts := make(map[recordIdentity]putRun)
	for _, operation := range req.GetOperations() {
		if operation.GetMerge() != nil {
			hasSnapshot = true
			bytes += max(largestSource, len(operation.GetMerge().GetLuaProgram().GetSource()))
		}
		if hasConditionalPut && !hasSnapshot && operation.GetPut() != nil {
			address, err := protocol.ParseAddress(operation.GetAddress())
			if err == nil {
				key := identityOf(address)
				run := puts[key]
				run.count++
				run.conditional = run.conditional || operation.GetPut().GetMode() != sink.WriteMode_WRITE_MODE_UPSERT
				puts[key] = run
				hasSnapshot = run.count > 1 && run.conditional
			}
		}
	}
	if hasSnapshot && req.GetCompletionMode() != sink.CompletionMode_COMPLETION_MODE_RETURN_AFTER_ACCEPTED {
		// Shared records retain one snapshot and final document, even when
		// several RPCs contribute to the chain. Preserve hot-key folding while
		// reserving separate space for independent callers/records.
		retained := callers
		if callers > 1 {
			records := make(map[recordIdentity]bool)
			for _, operation := range req.GetOperations() {
				address, err := protocol.ParseAddress(operation.GetAddress())
				if err == nil {
					records[identityOf(address)] = true
				}
			}
			retained = min(callers, max(1, len(records)))
		}
		// A micro-batch streams independent records through one bounded read
		// chunk and one output batch. One additional candidate can coexist with
		// the output batch while it is committed. Caller quotas remain separate.
		workingSets := min(2*retained, 3)
		estimate.workingBytes = s.maxReadBytes * workingSets
		bytes += estimate.workingBytes
	}
	estimate.bytes = bytes
	return estimate
}

func (s *Server) estimateWriteReturns(req *sink.WriteRequest, callers int) int {
	maximum := s.maxReadBytes * callers
	bytes := 0
	for _, operation := range req.GetOperations() {
		if !operation.GetReturnDocument() {
			continue
		}
		if operation.GetMerge() != nil {
			// Lua output is unknown until execution; retain the original RPC
			// allowance for every returning caller in a mixed batch.
			return maximum
		}
		// Returned Put documents are known before execution. Every returned
		// operation owns a copy, including repeated writes of the same key.
		bytes += len(operation.GetPut().GetDocument().GetPayload()) + 128
	}
	return min(bytes, maximum)
}

func returningCallerCount(req *sink.WriteRequest, budgets *requestBudgets) int {
	owners := make(map[int]bool)
	for index, operation := range req.GetOperations() {
		if operation.GetReturnDocument() {
			owners[budgets.owner(index)] = true
		}
	}
	return len(owners)
}
