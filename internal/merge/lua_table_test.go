package merge

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/iceisfun/golua/compiler"
	"github.com/iceisfun/golua/parser"
	"github.com/iceisfun/golua/stdlib"
	"github.com/iceisfun/golua/vm"
)

func TestLuaTableLoopsHonorInstructionLimit(t *testing.T) {
	for _, name := range []string{"move", "concat"} {
		t.Run(name, func(t *testing.T) {
			limits := vm.Limits{MaxInstructions: 20}
			state := vm.New(vm.WithLimits(limits))
			defer state.Close(context.Background())
			stdlib.Open(state)
			boundLuaAllocations(state, 1024)
			table := vm.NewEmptyTable()
			for index := 1; index <= 100; index++ {
				table.SetInt(index, vm.NewString(""))
			}
			arguments := []vm.Value{vm.NewTable(table)}
			if name == "move" {
				arguments = append(arguments, vm.NewInt(1), vm.NewInt(100), vm.NewInt(2))
			}
			function := state.GetGlobal("table").AsTable().(*vm.Table).GetString(name)
			_, err := state.ProtectedCall(function, arguments)
			if err == nil || !strings.Contains(err.Error(), "instruction limit exceeded") {
				t.Fatalf("table.%s ignored instruction budget: %v", name, err)
			}
		})
	}
}

func TestBoundedTableOperationsMatchRuntime(t *testing.T) {
	plain := vm.New()
	defer plain.Close(context.Background())
	stdlib.Open(plain)
	bounded := vm.New()
	defer bounded.Close(context.Background())
	stdlib.Open(bounded)
	boundLuaAllocations(bounded, 4096)
	cases := []string{
		`return table.concat({"a", "b", "c"})`,
		`return table.concat({[0]="a", "b", 2, 3.5}, ":", "0", "3")`,
		`return table.concat({"abc"}, "separator", 1, 1)`,
		`return table.concat({}, "", 2, 1)`,
		`return table.concat({[math.maxinteger]="z"}, "", math.maxinteger, math.maxinteger)`,
		`return table.concat({true})`,
		`return table.concat({"a"}, {})`,
		`return table.concat({"a"}, "", 1.5)`,
		`return table.concat({"a"}, "", 1, 2)`,
		`return table.concat(nil)`,
		`local t={1,2,3,4}; assert(table.move(t,1,3,2)==t); return table.concat(t,",")`,
		`local t={1,2,3,4}; table.move(t,2,4,1); return table.concat(t,",")`,
		`local t={1,2,3}; local out={}; assert(table.move(t,"1","3","0",out)==out); return table.concat(out,",",0,2)`,
		`local t={1,2,3}; table.move(t,1,3,-2); return table.concat(t,",",-2,3)`,
		`local t={1,2,3}; table.move({},1,2,2,t); return t[1], t[2], t[3]`,
		`local t={1,2,3}; table.move(t,3,1,1); return table.concat(t,",")`,
		`local t={[math.maxinteger]="z"}; table.move(t,math.maxinteger,math.maxinteger,math.mininteger); return t[math.mininteger]`,
		`return table.move({},1,2,math.maxinteger)`,
		`return table.move({},0,math.maxinteger,1)`,
		`return table.move({},1.5,2,1)`,
		`return table.move({},1,2,1,false)`,
		`return table.move(nil,1,0,1)`,
		`local t=setmetatable({}, {__len=function() return 2 end, __index=function(_,i) return i end}); return table.concat(t,":")`,
		`local t=setmetatable({}, {__index=function(_,i) return i end}); local out={}; table.move(t,1,3,1,out); return table.concat(out,":")`,
	}
	for _, source := range cases {
		block, err := parser.Parse("table-parity", source)
		if err != nil {
			t.Fatal(err)
		}
		program, err := compiler.Compile("table-parity", block)
		if err != nil {
			t.Fatal(err)
		}
		want, wantErr := plain.Run(program)
		got, gotErr := bounded.Run(program)
		if (wantErr == nil) != (gotErr == nil) {
			t.Fatalf("%s: runtime=%v bounded=%v", source, wantErr, gotErr)
		}
		if wantErr != nil {
			continue
		}
		if len(want) != len(got) {
			t.Fatalf("%s: got=%v want=%v", source, got, want)
		}
		for index := range want {
			if got[index].Type() != want[index].Type() || got[index].String() != want[index].String() {
				t.Fatalf("%s: got=%v want=%v", source, got, want)
			}
		}
	}
}

func TestLuaTableLoopsHonorCancellation(t *testing.T) {
	for _, name := range []string{"move", "concat"} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			state := vm.New(vm.WithContext(ctx))
			defer state.Close(context.Background())
			stdlib.Open(state)
			boundLuaAllocations(state, 1024)
			reads := 0
			meta := vm.NewEmptyTable()
			meta.SetString(vm.MetaIndex, vm.NewNativeFunc(func(state *vm.VM) int {
				reads++
				cancel()
				state.Set(0, vm.NewString("a"))
				return 1
			}))
			source := vm.NewEmptyTable()
			source.SetMetatable(meta)
			arguments := []vm.Value{vm.NewTable(source), vm.NewString(""), vm.NewInt(1), vm.NewInt(100)}
			if name == "move" {
				arguments = []vm.Value{vm.NewTable(source), vm.NewInt(1), vm.NewInt(100), vm.NewInt(1), vm.NewTable(vm.NewEmptyTable())}
			}
			function := state.GetGlobal("table").AsTable().(*vm.Table).GetString(name)
			_, err := state.ProtectedCall(function, arguments)
			if err == nil || !strings.Contains(err.Error(), "context canceled") || reads != 1 {
				t.Fatalf("table.%s continued after cancellation: reads=%d error=%v", name, reads, err)
			}
		})
	}
}

func TestLuaMoveRejectsOverflowBeforeMutating(t *testing.T) {
	state := vm.New()
	defer state.Close(context.Background())
	stdlib.Open(state)
	boundLuaAllocations(state, 1024)
	for _, bounds := range []string{"math.mininteger, math.maxinteger, 0", "0, math.maxinteger, 1", "1, 2, math.maxinteger"} {
		source := fmt.Sprintf(`local source={"a","b"}; local target={"unchanged"}; assert(not pcall(table.move,source,%s,target)); return table.concat(target)`, bounds)
		block, err := parser.Parse("move-overflow", source)
		if err != nil {
			t.Fatal(err)
		}
		program, err := compiler.Compile("move-overflow", block)
		if err != nil {
			t.Fatal(err)
		}
		result, err := state.Run(program)
		if err != nil || len(result) != 1 || result[0].AsString() != "unchanged" {
			t.Fatalf("invalid move mutated its destination: %v %v", result, err)
		}
	}
}
