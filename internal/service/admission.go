package service

import (
	"context"
	"slices"
	"sync"
	"time"

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
	name                 string
	metrics              *sinkmetrics.Metrics
	requestTimeout       time.Duration
	maxInFlightRequests  int
	maxInFlightBytes     int
	maxStoreRequests     int
	admissionMu          sync.Mutex
	admissionChanged     chan struct{}
	admissionWaiters     []*admissionRequest
	inFlightRequests     int
	inFlightBytes        int
	storeRequests        map[string]int
	maxScanRequests      int
	maxScanBytes         int
	maxStoreScanRequests int
	scanRequests         int
	scanBytes            int
	storeScanRequests    map[string]int
	scanAdmissionWait    time.Duration
	storeBytes           map[string]int
	maxStoreBytes        map[string]int
}

type admissionRequest struct {
	encodedBytes int
	stores       []string
	wait         bool
	timeout      time.Duration
	scan         bool
	publish      bool
	reservation  *admissionReservation
}

func (s *Server) admitRequest(ctx context.Context, request admissionRequest) (context.Context, context.CancelFunc, error) {
	if request.publish {
		return s.publishAdmission.admitRequest(ctx, request)
	}
	return s.admissionPool.admitRequest(ctx, request)
}

func (s *admissionPool) admitRequest(ctx context.Context, request admissionRequest) (context.Context, context.CancelFunc, error) {
	store := s.metrics.RequestStores(request.stores)
	waitCtx := ctx
	if request.scan && request.wait {
		var cancel context.CancelFunc
		waitCtx, cancel = context.WithTimeout(ctx, s.scanAdmissionWait)
		defer cancel()
	}
	var queued *admissionRequest
	var scanQueuedAt time.Time
	defer func() {
		if queued != nil {
			s.admissionMu.Lock()
			s.removeAdmissionWaiter(queued)
			s.admissionMu.Unlock()
		}
		if !scanQueuedAt.IsZero() {
			s.metrics.ObserveScanAdmissionWait(store, time.Since(scanQueuedAt))
		}
	}()
	for {
		if err := contextError(ctx); err != nil {
			return ctx, nil, err
		}
		if queued != nil && waitCtx.Err() != nil {
			return ctx, nil, s.rejectAdmission(request, "wait_timeout")
		}
		s.admissionMu.Lock()
		full := s.admissionSlotsFull(request) || request.encodedBytes > s.maxInFlightBytes-s.inFlightBytes
		reason := "requests"
		for _, name := range request.stores {
			if limit := s.maxStoreBytes[name]; limit > 0 && request.encodedBytes > limit-s.storeBytes[name] {
				reason = "store_bytes"
			}
		}
		if request.encodedBytes > s.maxInFlightBytes-s.inFlightBytes {
			reason = "bytes"
		}
		for _, earlier := range s.admissionWaiters {
			if earlier == queued {
				break
			}
			// A runnable older batch reserves the next available byte capacity.
			// Otherwise small arrivals can indefinitely starve returned-document
			// batches. A busy store or scan pool must still let other stores run.
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
				if request.scan && s.scanWaitQueueFull(request) {
					s.admissionMu.Unlock()
					return ctx, nil, s.rejectAdmission(request, "queue")
				}
				queued = &request
				s.admissionWaiters = append(s.admissionWaiters, queued)
				if request.scan {
					scanQueuedAt = time.Now()
					s.metrics.AdjustScanQueue(store, 1, request.encodedBytes)
				}
			}
			changed := s.admissionChanged
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
		for _, name := range request.stores {
			if request.scan {
				s.storeScanRequests[name]++
			}
			if _, configured := s.storeRequests[name]; configured {
				s.storeRequests[name]++
			}
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
		for _, name := range request.stores {
			if request.scan {
				s.storeScanRequests[name]--
				if s.storeScanRequests[name] == 0 {
					delete(s.storeScanRequests, name)
				}
			}
			if _, configured := s.storeRequests[name]; configured {
				s.storeRequests[name]--
			}
		}
		close(s.admissionChanged)
		s.admissionChanged = make(chan struct{})
		s.admissionMu.Unlock()
		s.metrics.AdjustAdmissionPool(store, s.name, -1, -bytes)
	}
	return execution, release, nil
}

func (s *admissionPool) admissionCanFit(request admissionRequest) bool {
	return request.encodedBytes <= s.executionByteLimit(request.stores) && (!request.scan || request.encodedBytes <= s.maxScanBytes)
}

func (s *admissionPool) executionByteLimit(stores []string) int {
	limit := s.maxInFlightBytes
	for _, name := range stores {
		if storeLimit := s.maxStoreBytes[name]; storeLimit > 0 {
			limit = min(limit, storeLimit)
		}
	}
	return limit
}

// The caller holds admissionMu. Cross-store calls conservatively charge their
// entire reservation to each touched store, and only once to the global pool.
func (s *admissionPool) adjustStoreBytes(stores []string, bytes int) {
	for _, name := range stores {
		if _, configured := s.storeBytes[name]; configured {
			s.storeBytes[name] += bytes
			s.metrics.AdjustStoreExecutionBytes(name, bytes)
		}
	}
}

// The caller holds admissionMu. Queued scans have separate count, byte and
// per-store bounds equal to the execution scan sublimits. Charge the full
// reservation conservatively, including input, without taking execution slots.
func (s *admissionPool) scanWaitQueueFull(request admissionRequest) bool {
	requests, bytes, storeRequests := 0, 0, 0
	for _, queued := range s.admissionWaiters {
		if !queued.scan {
			continue
		}
		requests++
		bytes += queued.encodedBytes
		if len(request.stores) > 0 && slices.Contains(queued.stores, request.stores[0]) {
			storeRequests++
		}
	}
	return requests >= s.maxScanRequests || request.encodedBytes > s.maxScanBytes-bytes || storeRequests >= s.maxStoreScanRequests
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
	for _, name := range request.stores {
		if request.scan && s.storeScanRequests[name] >= s.maxStoreScanRequests {
			return true
		}
		if limit := s.maxStoreBytes[name]; limit > 0 && request.encodedBytes > limit-s.storeBytes[name] {
			return true
		}
		if count, configured := s.storeRequests[name]; configured && count >= s.maxStoreRequests {
			return true
		}
	}
	return false
}

func (s *admissionPool) removeAdmissionWaiter(request *admissionRequest) {
	index := slices.Index(s.admissionWaiters, request)
	if index < 0 {
		return
	}
	s.admissionWaiters = slices.Delete(s.admissionWaiters, index, index+1)
	if request.scan {
		store := s.metrics.RequestStores(request.stores)
		s.metrics.AdjustScanQueue(store, -1, -request.encodedBytes)
	}
	close(s.admissionChanged)
	s.admissionChanged = make(chan struct{})
}

func operationStores[T interface{ GetAddress() *sink.RecordAddress }](operations []T) []string {
	seen := make(map[string]struct{})
	for _, operation := range operations {
		seen[operation.GetAddress().GetStore()] = struct{}{}
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
	bytes += s.maxReadBytes * returningCallers
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
			address, err := convertAddress(operation.GetAddress())
			if err == nil {
				key := s.identityOf(address)
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
				address, err := convertAddress(operation.GetAddress())
				if err == nil {
					records[s.identityOf(address)] = true
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

func returningCallerCount(req *sink.WriteRequest, budgets *requestBudgets) int {
	owners := make(map[int]bool)
	for index, operation := range req.GetOperations() {
		if operation.GetReturnDocument() {
			owners[budgets.owner(index)] = true
		}
	}
	return len(owners)
}
