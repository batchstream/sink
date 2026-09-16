package gateway

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liran/sink-go/uri"
	"github.com/liran/sink/internal/testuri"

	forward "github.com/liran/sink/gen/forward"
	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/config"
	"github.com/liran/sink/internal/engine"
	"github.com/liran/sink/internal/forwarding"
	"github.com/liran/sink/internal/merge"
	"github.com/liran/sink/internal/service"
	"github.com/liran/sink/internal/storage/memory"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fixtureEngine struct {
	store  string
	target string
	core   *service.Server
}

func testEngine(t testing.TB, store string, maximum int) fixtureEngine {
	t.Helper()
	luaOpts := merge.LuaOptions{}
	lua, err := merge.NewLuaEngine(luaOpts)
	if err != nil {
		t.Fatal(err)
	}
	opts := service.Options{Storage: memory.New(), Lua: lua, BoundStore: store, MaxReadBytes: maximum}
	core, err := service.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	batchOpts := service.BatchingOptions{MaxWait: time.Millisecond}
	batched, err := service.NewBatchingServer(core, batchOpts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(batched.Close)
	engineOpts := engine.Options{Service: batched, Store: store, MaxReadBytes: maximum}
	backend, err := engine.New(engineOpts)
	if err != nil {
		t.Fatal(err)
	}
	target := serveEngine(t, backend, batched)
	fixture := fixtureEngine{store: store, target: target, core: core}
	return fixture
}
func serveEngine(t testing.TB, backend forward.EngineServer, public ...sink.SinkServer) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	forward.RegisterEngineServer(server, backend)
	if len(public) > 0 {
		sink.RegisterSinkServer(server, public[0])
	}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	return listener.Addr().String()
}
func routeText(engines ...fixtureEngine) string {
	text := "routes:\n"
	for _, e := range engines {
		text += fmt.Sprintf("  - store: %s\n    target: %s\n    tls: {insecure: true}\n", e.store, e.target)
	}
	return text
}
func testGateway(t testing.TB, maximum int, engines ...fixtureEngine) *Server {
	t.Helper()
	path := filepath.Join(t.TempDir(), "routes.yaml")
	if err := os.WriteFile(path, []byte(routeText(engines...)), 0600); err != nil {
		t.Fatal(err)
	}
	settings := config.Gateway{RoutesFile: path, ReloadInterval: time.Second, DNSRefreshInterval: time.Second, IdleTimeout: time.Minute, MaxConnections: 10, MaxRequests: 32, MaxRequestsPerStore: 32, MaxBytes: 256 << 20, MaxFanout: 2}
	request := config.Request{Timeout: 3 * time.Second, MaxOperations: 1000, MaxReadBytes: maximum}
	opts := Options{Gateway: settings, Request: request, MaxMessageBytes: 64 << 20}
	server, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(server.Close)
	return server
}
func address(store, key string) *sink.RecordAddress {
	kind := uri.StringKey(key)
	recordKey := kind
	result := &sink.RecordAddress{Uri: testuri.Record(store, []string{"db", "items"}, recordKey)}
	return result
}
func put(store, key string, returnDocument bool) *sink.WriteOperation {
	document := &sink.Document{Encoding: sink.DocumentEncoding_DOCUMENT_ENCODING_JSON, Payload: []byte(`{"value":1}`)}
	operation := &sink.PutOperation{Document: document, Mode: sink.WriteMode_WRITE_MODE_UPSERT}
	action := &sink.WriteOperation_Put{Put: operation}
	result := &sink.WriteOperation{Address: address(store, key), Action: action, ReturnDocument: returnDocument}
	return result
}
func TestCrossStorePublicRecordsAndPartialFailure(t *testing.T) {
	a := testEngine(t, "a", 4096)
	b := testEngine(t, "b", 4096)
	gateway := testGateway(t, 4096, a, b)
	write := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, Operations: []*sink.WriteOperation{put("a", "one", false), put("b", "two", false), put("missing", "three", false), put("a", "one", false)}}
	response, err := gateway.Write(t.Context(), write)
	if err != nil {
		t.Fatal(err)
	}
	for i, result := range response.Results {
		if int(result.OperationIndex) != i {
			t.Fatal(response)
		}
		if i == 2 {
			if result.GetFailure().GetCode() != sink.FailureCode_FAILURE_CODE_INVALID_ARGUMENT {
				t.Fatal(response)
			}
		} else if result.Status != sink.WriteStatus_WRITE_STATUS_APPLIED {
			t.Fatal(response)
		}
	}
	readA := &sink.ReadOperation{Address: address("a", "one")}
	readB := &sink.ReadOperation{Address: address("b", "two")}
	read := &sink.ReadRequest{Operations: []*sink.ReadOperation{readB, readA, readB}}
	found, err := gateway.Read(t.Context(), read)
	if err != nil {
		t.Fatal(err)
	}
	for i, result := range found.Results {
		if result.Status != sink.ReadStatus_READ_STATUS_FOUND || int(result.OperationIndex) != i {
			t.Fatal(found)
		}
	}
	deleteA := &sink.DeleteOperation{Address: readA.Address}
	deleteB := &sink.DeleteOperation{Address: readB.Address}
	deletion := &sink.DeleteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, Operations: []*sink.DeleteOperation{deleteA, deleteB}}
	deleted, err := gateway.Delete(t.Context(), deletion)
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range deleted.Results {
		if result.Status != sink.DeleteStatus_DELETE_STATUS_APPLIED {
			t.Fatal(deleted)
		}
	}
}
func TestCrossStoreReturnBudgetCheckedBeforeCommit(t *testing.T) {
	a := testEngine(t, "a", 200)
	b := testEngine(t, "b", 200)
	gateway := testGateway(t, 200, a, b)
	write := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, Operations: []*sink.WriteOperation{put("a", "one", true), put("b", "two", true)}}
	response, err := gateway.Write(t.Context(), write)
	if err != nil {
		t.Fatal(err)
	}
	if response.Results[0].Status != sink.WriteStatus_WRITE_STATUS_APPLIED || response.Results[1].GetFailure().GetCode() != sink.FailureCode_FAILURE_CODE_RESOURCE_EXHAUSTED {
		t.Fatal(response)
	}
	op := &sink.ReadOperation{Address: address("b", "two")}
	req := &sink.ReadRequest{Operations: []*sink.ReadOperation{op}}
	check, err := b.core.Read(t.Context(), req)
	if err != nil || check.Results[0].Status != sink.ReadStatus_READ_STATUS_NOT_FOUND {
		t.Fatalf("over-budget mutation committed: %v %v", check, err)
	}
}
func TestCrossStoreReadBudgetAndRepeatedKeys(t *testing.T) {
	a := testEngine(t, "a", 300)
	b := testEngine(t, "b", 300)
	gateway := testGateway(t, 300, a, b)
	write := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, Operations: []*sink.WriteOperation{put("a", "one", false), put("b", "two", false)}}
	if _, err := gateway.Write(t.Context(), write); err != nil {
		t.Fatal(err)
	}
	one := &sink.ReadOperation{Address: address("a", "one")}
	two := &sink.ReadOperation{Address: address("b", "two")}
	request := &sink.ReadRequest{Operations: []*sink.ReadOperation{one, two, one}}
	response, err := gateway.Read(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	// A's repeated copies consume 278 bytes; B cannot allocate another document.
	if response.Results[0].Status != sink.ReadStatus_READ_STATUS_FOUND || response.Results[2].Status != sink.ReadStatus_READ_STATUS_FOUND || response.Results[1].GetFailure().GetCode() != sink.FailureCode_FAILURE_CODE_RESOURCE_EXHAUSTED {
		t.Fatal(response)
	}
}
func TestLuaDeclarationsValidatedBeforeAnyStoreWrites(t *testing.T) {
	a := testEngine(t, "a", 4096)
	b := testEngine(t, "b", 4096)
	gateway := testGateway(t, 4096, a, b)
	bad := &sink.LuaProgram{Source: []byte("return 1"), Sha256: make([]byte, 32)}
	write := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, Operations: []*sink.WriteOperation{put("a", "one", false), put("b", "two", false)}, LuaPrograms: []*sink.LuaProgram{bad}}
	if _, err := gateway.Write(t.Context(), write); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid declaration accepted: %v", err)
	}
	one := &sink.ReadOperation{Address: address("a", "one")}
	read := &sink.ReadRequest{Operations: []*sink.ReadOperation{one}}
	result, err := gateway.Read(t.Context(), read)
	if err != nil || result.Results[0].Status != sink.ReadStatus_READ_STATUS_NOT_FOUND {
		t.Fatalf("partial write before validation: %v %v", result, err)
	}
	source := []byte("return function(current, incoming) return incoming end")
	digest := sha256.Sum256(source)
	declaration := &sink.LuaProgram{Source: source, Sha256: digest[:]}
	write.LuaPrograms = []*sink.LuaProgram{declaration}
	for _, op := range write.Operations {
		reference := &sink.LuaProgram{Sha256: digest[:]}
		merged := &sink.MergeOperation{IncomingDocument: op.GetPut().GetDocument(), LuaProgram: reference}
		op.Action = &sink.WriteOperation_Merge{Merge: merged}
	}
	response, err := gateway.Write(t.Context(), write)
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range response.Results {
		if result.Status != sink.WriteStatus_WRITE_STATUS_APPLIED {
			t.Fatal(response)
		}
	}
}
func TestMisroutedEngineCannotWrite(t *testing.T) {
	a := testEngine(t, "a", 4096)
	wrong := fixtureEngine{store: "b", target: a.target}
	gateway := testGateway(t, 4096, wrong)
	req := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, Operations: []*sink.WriteOperation{put("b", "one", false)}}
	response, err := gateway.Write(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	if response.Results[0].Status != sink.WriteStatus_WRITE_STATUS_FAILED || response.Results[0].GetFailure().GetRetryable() {
		t.Fatal(response)
	}
	// A core bound to one Store rejects a mixed batch before touching its storage.
	mixed := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, Operations: []*sink.WriteOperation{put("a", "one", false), put("b", "two", false)}}
	if _, err := a.core.Write(t.Context(), mixed); status.Code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}
	one := &sink.ReadOperation{Address: address("a", "one")}
	read := &sink.ReadRequest{Operations: []*sink.ReadOperation{one}}
	result, err := a.core.Read(t.Context(), read)
	if err != nil || result.Results[0].Status != sink.ReadStatus_READ_STATUS_NOT_FOUND {
		t.Fatalf("misrouted batch wrote a record: %v %v", result, err)
	}
}

type controlledEngine struct {
	forward.UnimplementedEngineServer
	store   string
	calls   atomic.Int64
	entered chan struct{}
	release chan struct{}
	lost    bool
}

func (e *controlledEngine) Forward(ctx context.Context, req *forward.ForwardRequest) (*forward.ForwardResponse, error) {
	e.calls.Add(1)
	if e.entered != nil {
		select {
		case e.entered <- struct{}{}:
		default:
		}
	}
	if e.release != nil {
		select {
		case <-e.release:
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		}
	}
	if e.lost {
		return nil, status.Error(codes.Unavailable, "reply lost after mutation")
	}
	response := emptyResponse(req, len(req.GetWrite().GetOperations()))
	response.Version = forwarding.Version
	response.Store = e.store
	response.Used = &forward.Budget{}
	for i := range req.GetWrite().GetOperations() {
		result := &sink.WriteResult{OperationIndex: uint32(i), Status: sink.WriteStatus_WRITE_STATUS_APPLIED}
		response.GetWrite().Results[i] = result
	}
	return response, nil
}
func TestLostMutationReplyIsNotReplayedAndKeepsOtherSuccess(t *testing.T) {
	lost := &controlledEngine{store: "a", lost: true}
	a := fixtureEngine{store: "a", target: serveEngine(t, lost)}
	b := testEngine(t, "b", 4096)
	gateway := testGateway(t, 4096, a, b)
	request := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, Operations: []*sink.WriteOperation{put("a", "one", false), put("b", "two", false)}}
	response, err := gateway.Write(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if lost.calls.Load() != 1 || !strings.Contains(response.Results[0].GetFailure().GetMessage(), "outcome is unknown") || response.Results[0].GetFailure().GetRetryable() || response.Results[1].Status != sink.WriteStatus_WRITE_STATUS_APPLIED {
		t.Fatalf("unsafe replay or lost partial success: %v", response)
	}
}
func TestReloadRetainsSnapshotForWholeRequest(t *testing.T) {
	held := &controlledEngine{store: "a", entered: make(chan struct{}, 1), release: make(chan struct{})}
	a := fixtureEngine{store: "a", target: serveEngine(t, held)}
	old := &controlledEngine{store: "b"}
	next := &controlledEngine{store: "b"}
	b := fixtureEngine{store: "b", target: serveEngine(t, old)}
	replacement := fixtureEngine{store: "b", target: serveEngine(t, next)}
	gateway := testGateway(t, 4096, a, b)
	request := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, Operations: []*sink.WriteOperation{put("a", "one", true), put("b", "two", true)}}
	done := make(chan error, 1)
	go func() { _, err := gateway.Write(t.Context(), request); done <- err }()
	select {
	case <-held.entered:
	case <-time.After(time.Second):
		t.Fatal("first Store did not start")
	}
	if err := os.WriteFile(gateway.config.RoutesFile, []byte(routeText(a, replacement)), 0600); err != nil {
		t.Fatal(err)
	}
	if err := gateway.Reload(); err != nil {
		t.Fatal(err)
	}
	close(held.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if old.calls.Load() != 1 || next.calls.Load() != 0 {
		t.Fatal("request mixed route snapshots")
	}
	request.Operations = request.Operations[1:]
	if _, err := gateway.Write(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if next.calls.Load() != 1 {
		t.Fatal("new request did not use new snapshot")
	}
	hash := gateway.current.Load().hash
	if err := os.WriteFile(gateway.config.RoutesFile, []byte("routes: [invalid]"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := gateway.Reload(); err == nil || gateway.current.Load().hash != hash {
		t.Fatal("invalid route file replaced last valid snapshot")
	}
}
func TestConnectionsAreLazyBoundedAndExpire(t *testing.T) {
	a := testEngine(t, "a", 4096)
	b := testEngine(t, "b", 4096)
	gateway := testGateway(t, 4096, a, b)
	if len(gateway.pool.entries) != 0 {
		t.Fatal("connections were eagerly opened")
	}
	gateway.pool.maximum = 1
	routeA := gateway.current.Load().routes["a"]
	routeB := gateway.current.Load().routes["b"]
	first, err := gateway.pool.acquire(routeA)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := gateway.pool.acquire(routeB); status.Code(err) != codes.ResourceExhausted {
		t.Fatal("active connection evicted", err)
	}
	gateway.pool.release(first)
	second, err := gateway.pool.acquire(routeB)
	if err != nil {
		t.Fatal(err)
	}
	gateway.pool.release(second)
	gateway.pool.mu.Lock()
	second.idleSince = time.Now().Add(-time.Hour)
	gateway.pool.mu.Unlock()
	gateway.pool.expire()
	if len(gateway.pool.entries) != 0 {
		t.Fatal("idle connection not expired")
	}
}
func TestGatewayAdmissionAndConcurrentBudgets(t *testing.T) {
	a := testEngine(t, "a", 200)
	b := testEngine(t, "b", 200)
	gateway := testGateway(t, 200, a, b)
	var work sync.WaitGroup
	for i := range 24 {
		work.Go(func() {
			request := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, Operations: []*sink.WriteOperation{put("a", fmt.Sprint(i), true), put("b", fmt.Sprint(i), true)}}
			response, err := gateway.Write(t.Context(), request)
			if err != nil {
				t.Error(err)
				return
			}
			if response.Results[0].Status != sink.WriteStatus_WRITE_STATUS_APPLIED || response.Results[1].GetFailure().GetCode() != sink.FailureCode_FAILURE_CODE_RESOURCE_EXHAUSTED {
				t.Error(response)
			}
		})
	}
	work.Wait()
	gateway.config.MaxBytes = 1
	request := &sink.ReadRequest{}
	op := &sink.ReadOperation{Address: address("a", "one")}
	request.Operations = []*sink.ReadOperation{op}
	if _, err := gateway.Read(t.Context(), request); status.Code(err) != codes.ResourceExhausted {
		t.Fatal(err)
	}
}
func TestRoutesRejectDuplicateStoreNames(t *testing.T) {
	a := fixtureEngine{store: "a", target: "127.0.0.1:1"}
	gateway := testGateway(t, 4096, a)
	previous := gateway.current.Load()
	duplicate := a
	duplicate.target = "127.0.0.1:2"
	if err := os.WriteFile(gateway.config.RoutesFile, []byte(routeText(a, duplicate)), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := readRoutes(gateway.config.RoutesFile); err == nil || !strings.Contains(err.Error(), "duplicate Store") {
		t.Fatalf("duplicate name accepted at startup: %v", err)
	}
	if err := gateway.Reload(); err == nil || gateway.current.Load() != previous {
		t.Fatal("duplicate name replaced the valid route snapshot")
	}
}

type nativeFixture struct {
	sink.UnimplementedSinkServer
	cancelled chan struct{}
}

func (n *nativeFixture) Execute(_ context.Context, request *sink.ExecuteRequest) (*sink.ExecuteResponse, error) {
	response := &sink.ExecuteResponse{Success: true, Payload: request.GetCommand().GetPayload(), ContentType: "application/json"}
	return response, nil
}
func (n *nativeFixture) Query(_ context.Context, request *sink.QueryRequest) (*sink.QueryResponse, error) {
	document := &sink.Document{Encoding: sink.DocumentEncoding_DOCUMENT_ENCODING_JSON, Payload: []byte(`{"ok":true}`)}
	response := &sink.QueryResponse{Documents: []*sink.Document{document}, HasMore: request.GetPage() == 2}
	return response, nil
}
func (n *nativeFixture) Count(ctx context.Context, request *sink.CountRequest) (*sink.CountResponse, error) {
	if string(request.GetCommand().GetPayload()) == "wait" {
		<-ctx.Done()
		close(n.cancelled)
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	response := &sink.CountResponse{Count: 42}
	return response, nil
}
func (n *nativeFixture) Scan(_ context.Context, _ *sink.ScanRequest) (*sink.ScanResponse, error) {
	detail := &errdetails.ErrorInfo{Reason: "admission-marker", Domain: "sink"}
	annotated, err := status.New(codes.ResourceExhausted, "admission full").WithDetails(detail)
	if err != nil {
		return nil, err
	}
	return nil, annotated.Err()
}
func TestNativeForwardingPreservesDetailsAndCancellation(t *testing.T) {
	native := &nativeFixture{cancelled: make(chan struct{})}
	opts := engine.Options{Service: native, Store: "a", MaxReadBytes: 4096}
	backend, err := engine.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	fixture := fixtureEngine{store: "a", target: serveEngine(t, backend)}
	gateway := testGateway(t, 4096, fixture)
	command := &sink.Command{Uri: "sink://a/custom/tenant/partition", Payload: []byte(`{"ok":true}`)}
	execute := &sink.ExecuteRequest{Command: command}
	executed, err := gateway.Execute(t.Context(), execute)
	if err != nil || !executed.GetSuccess() || string(executed.GetPayload()) != string(command.Payload) {
		t.Fatalf("Execute: %v %v", executed, err)
	}
	query := &sink.QueryRequest{Command: command, Page: 2}
	queried, err := gateway.Query(t.Context(), query)
	if err != nil || !queried.GetHasMore() || len(queried.GetDocuments()) != 1 {
		t.Fatalf("Query: %v %v", queried, err)
	}
	count := &sink.CountRequest{Command: command}
	counted, err := gateway.Count(t.Context(), count)
	if err != nil || counted.GetCount() != 42 {
		t.Fatalf("Count: %v %v", counted, err)
	}
	scan := &sink.ScanRequest{Command: command}
	_, err = gateway.Scan(t.Context(), scan)
	details := status.Convert(err).Details()
	if status.Code(err) != codes.ResourceExhausted || len(details) != 1 || details[0].(*errdetails.ErrorInfo).Reason != "admission-marker" {
		t.Fatalf("lost status detail: %v", err)
	}
	waiting := &sink.Command{Uri: "sink://a", Payload: []byte("wait")}
	count.Command = waiting
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	if _, err := gateway.Count(ctx, count); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("deadline not propagated: %v", err)
	}
	select {
	case <-native.cancelled:
	case <-time.After(time.Second):
		t.Fatal("Engine retained cancelled work")
	}
}
func TestStoreForwardingLimitDoesNotBlockHealthyStore(t *testing.T) {
	held := &controlledEngine{store: "a", entered: make(chan struct{}, 1), release: make(chan struct{})}
	a := fixtureEngine{store: "a", target: serveEngine(t, held)}
	b := testEngine(t, "b", 4096)
	gateway := testGateway(t, 4096, a, b)
	gateway.config.MaxRequestsPerStore = 1
	request := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, Operations: []*sink.WriteOperation{put("a", "held", false)}}
	done := make(chan error, 1)
	go func() { _, err := gateway.Write(t.Context(), request); done <- err }()
	select {
	case <-held.entered:
	case <-time.After(time.Second):
		t.Fatal("slow Store did not start")
	}
	mixed := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, Operations: []*sink.WriteOperation{put("a", "rejected", false), put("b", "healthy", false)}}
	response, err := gateway.Write(t.Context(), mixed)
	close(held.release)
	if firstErr := <-done; firstErr != nil {
		t.Fatal(firstErr)
	}
	if err != nil || response.Results[0].GetFailure().GetCode() != sink.FailureCode_FAILURE_CODE_RESOURCE_EXHAUSTED || !response.Results[0].GetFailure().GetRetryable() || response.Results[1].Status != sink.WriteStatus_WRITE_STATUS_APPLIED {
		t.Fatalf("Store saturation leaked: %v %v", response, err)
	}
}
func TestFanoutIsBounded(t *testing.T) {
	entered := make(chan struct{}, 3)
	release := make(chan struct{})
	var fixtures []fixtureEngine
	for _, store := range []string{"a", "b", "c"} {
		backend := &controlledEngine{store: store, entered: entered, release: release}
		fixture := fixtureEngine{store: store, target: serveEngine(t, backend)}
		fixtures = append(fixtures, fixture)
	}
	gateway := testGateway(t, 4096, fixtures...)
	request := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, Operations: []*sink.WriteOperation{put("a", "one", false), put("b", "two", false), put("c", "three", false)}}
	done := make(chan error, 1)
	go func() { _, err := gateway.Write(t.Context(), request); done <- err }()
	for range 2 {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("fanout failed to start")
		}
	}
	select {
	case <-entered:
		t.Error("fanout exceeded configured limit")
	default:
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
func TestReloadReaddsStoreWithNewEngineAddress(t *testing.T) {
	a := fixtureEngine{store: "a", target: "127.0.0.1:1"}
	b := fixtureEngine{store: "b", target: "127.0.0.1:2"}
	gateway := testGateway(t, 4096, a, b)
	if err := os.WriteFile(gateway.config.RoutesFile, []byte(routeText(b)), 0600); err != nil {
		t.Fatal(err)
	}
	if err := gateway.Reload(); err != nil {
		t.Fatal(err)
	}
	a.target = "127.0.0.1:3"
	if err := os.WriteFile(gateway.config.RoutesFile, []byte(routeText(a, b)), 0600); err != nil {
		t.Fatal(err)
	}
	if err := gateway.Reload(); err != nil {
		t.Fatal(err)
	}
	if routes := gateway.current.Load().routes; len(routes) != 2 || routes[a.store].Target != a.target {
		t.Fatal("re-added Store did not use its new Engine address")
	}
}

func TestInvalidEngineResultsCannotOverwriteKnownSuccess(t *testing.T) {
	good := &sink.WriteResult{OperationIndex: 0, Status: sink.WriteStatus_WRITE_STATUS_APPLIED}
	cases := [][]*sink.WriteResult{
		{nil},
		{{OperationIndex: 0, Status: sink.WriteStatus_WRITE_STATUS_UNSPECIFIED}},
		{{OperationIndex: 1, Status: sink.WriteStatus_WRITE_STATUS_APPLIED}},
		{good, good},
	}
	for _, source := range cases {
		destination := []*sink.WriteResult{good, nil}
		if err := mergeResults(destination, source, []int{1}); err == nil {
			t.Fatal("invalid response accepted")
		}
		if destination[0] != good || destination[1] != nil {
			t.Fatal("validation changed a known result")
		}
	}
}

func TestEngineRejectsStaleProtocolAndMismatchedStoreBeforeWrites(t *testing.T) {
	a := testEngine(t, "a", 4096)
	opts := engine.Options{Service: a.core, Store: "a", MaxReadBytes: 4096}
	backend, err := engine.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name    string
		version uint32
		store   string
		body    string
		code    codes.Code
	}{
		{name: "previous protocol", version: 1, store: "a", body: "a", code: codes.FailedPrecondition},
		{name: "wrong envelope Store", version: forwarding.Version, store: "b", body: "a", code: codes.FailedPrecondition},
		{name: "wrong operation Store", version: forwarding.Version, store: "a", body: "b", code: codes.InvalidArgument},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			write := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, Operations: []*sink.WriteOperation{put(test.body, test.name, false)}}
			body := &forward.ForwardRequest_Write{Write: write}
			grant := &forward.Budget{Snapshots: 4096, Inputs: 4096, Outputs: 4096, Returns: 4096}
			request := &forward.ForwardRequest{Version: test.version, Store: test.store, Grant: grant, Request: body}
			response, err := backend.Forward(t.Context(), request)
			if err != nil || response.GetCode() != uint32(test.code) || !response.GetNotStarted() {
				t.Fatalf("request was not rejected before execution: %v, %v", response, err)
			}
			op := &sink.ReadOperation{Address: address("a", test.name)}
			read := &sink.ReadRequest{Operations: []*sink.ReadOperation{op}}
			result, err := a.core.Read(t.Context(), read)
			if err != nil || result.Results[0].Status != sink.ReadStatus_READ_STATUS_NOT_FOUND {
				t.Fatalf("rejected request wrote a record: %v, %v", result, err)
			}
		})
	}
}
