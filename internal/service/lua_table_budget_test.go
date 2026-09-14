package service_test

import (
	"bytes"
	"strings"
	"testing"
	"time"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/merge"
	"github.com/liran/sink/internal/service"
	"github.com/liran/sink/internal/storage"
	"github.com/liran/sink/internal/storage/memory"
)

func TestLuaNativeBudgetFailureDoesNotCommitPartialMutation(t *testing.T) {
	for _, body := range []string{
		`table.insert(current.values, 1, "inserted")`,
		`table.remove(current.values, 1)`,
		`table.sort(current.values)`,
		`table.unpack(current.values)`,
		`string.unpack("` + strings.Repeat(" ", 100) + `", "")`,
		`string.pack("` + strings.Repeat(" ", 100) + `")`,
		`string.packsize("` + strings.Repeat(" ", 100) + `")`,
	} {
		t.Run(body, func(t *testing.T) {
			backend := memory.New()
			document := storageJSONDocument(`{"values":[` + strings.Repeat(`"",`, 99) + `""],"value":0}`)
			seed := memory.SeedRequest{Address: storageAddress("limited"), Document: document}
			backend.Seed(seed)
			luaOptions := merge.LuaOptions{MaxInstructions: 50}
			engine, err := merge.NewLuaEngine(luaOptions)
			if err != nil {
				t.Fatal(err)
			}
			options := service.Options{Storage: backend, Lua: engine}
			server, err := service.New(options)
			if err != nil {
				t.Fatal(err)
			}
			source := "return function(current, incoming) current.value=1; " + body + "; return current end"
			limited := foldingMerge("limited", source, `{}`)
			healthy := foldingPut("healthy", sink.WriteMode_WRITE_MODE_UPSERT, 2)
			request := foldingRequest(limited, healthy)
			response, err := server.Write(t.Context(), request)
			if err != nil {
				t.Fatal(err)
			}
			result := response.Results[0]
			if result.Status != sink.WriteStatus_WRITE_STATUS_FAILED || result.GetFailure().GetCode() != sink.FailureCode_FAILURE_CODE_RESOURCE_EXHAUSTED {
				t.Fatalf("native work escaped its budget: %v", result)
			}
			if response.Results[1].Status != sink.WriteStatus_WRITE_STATUS_APPLIED {
				t.Fatalf("budget failure affected a sibling record: %v", response.Results[1])
			}
			read := storage.ReadOperation{Address: seed.Address}
			readRequest := storage.ReadRequest{Operations: []storage.ReadOperation{read}}
			stored, err := backend.Read(t.Context(), readRequest)
			if err != nil || !bytes.Equal(stored.Results[0].Document.Payload, document.Payload) {
				t.Fatalf("failed merge persisted its partial mutation: response=%v error=%v", stored, err)
			}
		})
	}
}

func TestLuaConversionDeadlineDoesNotCommitMutation(t *testing.T) {
	cases := []struct {
		name     string
		body     string
		incoming string
	}{
		{name: "decode input", incoming: `{}` + strings.Repeat(" ", 8<<20)},
		{name: "encode result", body: `current.padding=string.pack("c2097152", "");`, incoming: `{}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			backend := memory.New()
			document := storageJSONDocument(`{"value":0}`)
			seed := memory.SeedRequest{Address: storageAddress("limited"), Document: document}
			backend.Seed(seed)
			luaOptions := merge.LuaOptions{Timeout: 5 * time.Millisecond}
			engine, err := merge.NewLuaEngine(luaOptions)
			if err != nil {
				t.Fatal(err)
			}
			options := service.Options{Storage: backend, Lua: engine}
			server, err := service.New(options)
			if err != nil {
				t.Fatal(err)
			}
			source := "return function(current, incoming) current.value=1; " + tc.body + "return current end"
			limited := foldingMerge("limited", source, tc.incoming)
			healthy := foldingPut("healthy", sink.WriteMode_WRITE_MODE_UPSERT, 2)
			request := foldingRequest(limited, healthy)
			response, err := server.Write(t.Context(), request)
			if err != nil {
				t.Fatal(err)
			}
			result := response.Results[0]
			if result.Status != sink.WriteStatus_WRITE_STATUS_FAILED || result.GetFailure().GetCode() != sink.FailureCode_FAILURE_CODE_DEADLINE_EXCEEDED {
				t.Fatalf("conversion escaped deadline: status=%v failure=%v", result.Status, result.Failure)
			}
			if response.Results[1].Status != sink.WriteStatus_WRITE_STATUS_APPLIED {
				t.Fatalf("deadline failure affected a sibling record: %v", response.Results[1])
			}
			read := storage.ReadOperation{Address: seed.Address}
			readRequest := storage.ReadRequest{Operations: []storage.ReadOperation{read}}
			stored, err := backend.Read(t.Context(), readRequest)
			if err != nil || !bytes.Equal(stored.Results[0].Document.Payload, document.Payload) {
				t.Fatalf("timed-out merge persisted its mutation: error=%v", err)
			}
		})
	}
}

func TestSinkV1CumulativeBudgetFailureDoesNotCommitMutation(t *testing.T) {
	for _, call := range []string{
		`array.append_all(json.array(), incoming.values)`,
		`array.deduplicate(incoming.values, tostring)`,
		`array.keep_tail(incoming.values, 40)`,
		`array.union_strings(incoming.values, nil)`,
		`object.replace_nonempty_array(json.object(), incoming, "values")`,
	} {
		t.Run(call, func(t *testing.T) {
			backend := memory.New()
			document := storageJSONDocument(`{"value":0}`)
			seed := memory.SeedRequest{Address: storageAddress("limited"), Document: document}
			backend.Seed(seed)
			luaOptions := merge.LuaOptions{MaxInstructions: 200}
			engine, err := merge.NewLuaEngine(luaOptions)
			if err != nil {
				t.Fatal(err)
			}
			options := service.Options{Storage: backend, Lua: engine}
			server, err := service.New(options)
			if err != nil {
				t.Fatal(err)
			}
			source := "return function(current, incoming) current.value=1; local array, object = sink.v1.array, sink.v1.object; for i=1,8 do " + call + " end; return current end"
			incoming := `{"values":[` + strings.Repeat(`"",`, 39) + `""]}`
			limited := foldingMerge("limited", source, incoming)
			healthy := foldingPut("healthy", sink.WriteMode_WRITE_MODE_UPSERT, 2)
			request := foldingRequest(limited, healthy)
			response, err := server.Write(t.Context(), request)
			if err != nil {
				t.Fatal(err)
			}
			result := response.Results[0]
			if result.Status != sink.WriteStatus_WRITE_STATUS_FAILED || result.GetFailure().GetCode() != sink.FailureCode_FAILURE_CODE_RESOURCE_EXHAUSTED {
				t.Fatalf("repeated helper calls escaped the budget: %v", result)
			}
			if response.Results[1].Status != sink.WriteStatus_WRITE_STATUS_APPLIED {
				t.Fatalf("budget failure affected a sibling record: %v", response.Results[1])
			}
			read := storage.ReadOperation{Address: seed.Address}
			readRequest := storage.ReadRequest{Operations: []storage.ReadOperation{read}}
			stored, err := backend.Read(t.Context(), readRequest)
			if err != nil || !bytes.Equal(stored.Results[0].Document.Payload, document.Payload) {
				t.Fatalf("failed merge persisted its mutation: error=%v", err)
			}
		})
	}
}
