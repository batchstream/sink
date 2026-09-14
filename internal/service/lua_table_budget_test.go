package service_test

import (
	"bytes"
	"strings"
	"testing"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/merge"
	"github.com/liran/sink/internal/service"
	"github.com/liran/sink/internal/storage"
	"github.com/liran/sink/internal/storage/memory"
)

func TestLuaTableBudgetFailureDoesNotCommitPartialMutation(t *testing.T) {
	for _, body := range []string{
		`table.insert(current.values, 1, "inserted")`,
		`table.remove(current.values, 1)`,
		`table.sort(current.values)`,
		`table.unpack(current.values)`,
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
				t.Fatalf("table work escaped its budget: %v", result)
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
