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
	for _, name := range []string{"move", "concat", "insert", "remove", "sort", "unpack", "pack"} {
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
			switch name {
			case "insert":
				arguments = append(arguments, vm.NewInt(1), vm.NewString("inserted"))
			case "remove":
				arguments = append(arguments, vm.NewInt(1))
			case "pack":
				arguments = make([]vm.Value, 100)
				for index := range arguments {
					arguments[index] = vm.NewInt(int64(index))
				}
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
		`local t={1,2,3}; local n=select("#",table.insert(t,4)); return n,table.concat(t,",")`,
		`local t={1,2,3}; table.insert(t,"1",0); table.insert(t,3,7); return table.concat(t,",")`,
		`local t={1,2,3}; table.insert(t,2,nil); return #t,t[1],t[2],t[3],t[4]`,
		`return table.insert({},0,1)`,
		`return table.insert({},2,1)`,
		`return table.insert({},math.mininteger,1)`,
		`return table.insert({},math.maxinteger,1)`,
		`return table.insert({},1.5,1)`,
		`return table.insert({})`,
		`return table.insert({},1,2,3)`,
		`return table.insert(nil,1)`,
		`local t={1,2,3}; local a=table.remove(t,"1"); local b=table.remove(t); return a,b,table.concat(t,",")`,
		`local t={1,2,3}; return table.remove(t,4),table.concat(t,",")`,
		`local t={[0]="zero"}; return table.remove(t),t[0]`,
		`return table.remove({})`,
		`return table.remove({},1)`,
		`return table.remove({1,2},0)`,
		`return table.remove({},-1)`,
		`return table.remove({},math.maxinteger)`,
		`return table.remove({},math.mininteger)`,
		`return table.remove({1,2},1.5)`,
		`return table.remove(nil)`,
		`local t={4,1,3,2,1}; local n=select("#",table.sort(t)); return n,table.concat(t,",")`,
		`local t={"c","a","b"}; table.sort(t); return table.concat(t,",")`,
		`local t={4,1,3,2,1}; table.sort(t,function(a,b) return a>b end); return table.concat(t,",")`,
		`local t={{n=2},{n=1},{n=3}}; table.sort(t,function(a,b) return a.n<b.n end); return t[1].n,t[2].n,t[3].n`,
		`local t={}; table.sort(t,false); table.sort({1},false); return #t`,
		`return table.sort({1,2},false)`,
		`return table.sort({1,"a"})`,
		`return table.sort(nil)`,
		`local err={}; local ok,value=pcall(table.sort,{2,1},function() error(err) end); return ok,value==err`,
		`local t=table.pack(1,nil,3,nil); return t.n,#t,table.unpack(t,1,t.n)`,
		`local t=table.pack(nil,nil); return t.n,#t,table.unpack(t,1,t.n)`,
		`local t=table.pack(); return t.n,#t,table.unpack(t)`,
		`return table.unpack({1,2,3},"2","3")`,
		`return table.unpack({[0]="zero",1,2},0,2)`,
		`return table.unpack({1,nil,3},1,4)`,
		`return table.unpack({},2,1)`,
		`return table.unpack(nil,2,1)`,
		`return table.unpack(nil)`,
		`return table.unpack({},1.5,2)`,
		`return table.unpack({},math.mininteger,math.maxinteger)`,
		`return table.unpack({[math.maxinteger]="last"},math.maxinteger,math.maxinteger)`,
		`local t=setmetatable({}, {__len=function() return 3 end,__index=function(_,i) return i*2 end}); return table.unpack(t)`,
		`local t=setmetatable({}, {__len=function() return 2 end,__index=function(_,i) return i end}); table.insert(t,1,9); return t[1],t[2],t[3]`,
		`local t=setmetatable({}, {__len=function() return 2 end,__index=function(_,i) return i end}); return table.remove(t,1),t[1]`,
		`local meta={__lt=function(a,b) return a.n<b.n end}; local t={setmetatable({n=2},meta),setmetatable({n=1},meta)}; table.sort(t); return t[1].n,t[2].n`,
		`return table.sort({1,2,3,4,5,6,7,8},function() return true end)`,
		`local t={1,2,3,4,5,6,7,8}; table.sort(t,function() return false end); return table.concat(t,",")`,
	}
	for _, size := range []int{0, 1, 2, 3, 4, 8, 17, 64, 257} {
		for seed := range 8 {
			var values strings.Builder
			for index := range size {
				fmt.Fprintf(&values, "%d,", (index*index+seed*index+seed)%31)
			}
			cases = append(cases, fmt.Sprintf(`local t={%s}; table.sort(t); return table.concat(t,",")`, values.String()))
			// Callback order is observable even for equal values. Preserve the
			// original sort's comparison trace as well as its final ordering.
			source := fmt.Sprintf(`local t={%s}; local count,trace=0,0
table.sort(t,function(a,b) count=count+1; trace=(trace*31+a*7+b)%%1000003; return a>b end)
return count,trace,table.concat(t,",")`, values.String())
			cases = append(cases, source)
		}
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

func TestLuaTableMutationsHonorInFlightCancellation(t *testing.T) {
	for _, name := range []string{"insert", "remove", "sort", "unpack"} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			state := vm.New(vm.WithContext(ctx))
			defer state.Close(context.Background())
			stdlib.Open(state)
			boundLuaAllocations(state, 4096)
			reads := 0
			meta := vm.NewEmptyTable()
			meta.SetString(vm.MetaLen, vm.NewNativeFunc(func(state *vm.VM) int {
				state.Set(0, vm.NewInt(100))
				return 1
			}))
			meta.SetString(vm.MetaIndex, vm.NewNativeFunc(func(state *vm.VM) int {
				reads++
				cancel()
				state.Set(0, vm.NewInt(1))
				return 1
			}))
			table := vm.NewEmptyTable()
			table.SetMetatable(meta)
			arguments := []vm.Value{vm.NewTable(table)}
			switch name {
			case "insert":
				arguments = append(arguments, vm.NewInt(1), vm.NewInt(9))
			case "remove":
				arguments = append(arguments, vm.NewInt(1))
			}
			function := state.GetGlobal("table").AsTable().(*vm.Table).GetString(name)
			_, err := state.ProtectedCall(function, arguments)
			if err == nil || !strings.Contains(err.Error(), "context canceled") || reads > 2 {
				t.Fatalf("table.%s continued after cancellation: reads=%d error=%v", name, reads, err)
			}
		})
	}
}

func BenchmarkLuaTableSort(b *testing.B) {
	for _, bounded := range []bool{false, true} {
		b.Run(fmt.Sprintf("bounded=%t", bounded), func(b *testing.B) {
			state := vm.New(vm.WithContext(b.Context()))
			defer state.Close(context.Background())
			stdlib.Open(state)
			if bounded {
				boundLuaAllocations(state, 16384)
			}
			function := state.GetGlobal("table").AsTable().(*vm.Table).GetString("sort")
			table := vm.NewTableWithSize(1000, 0)
			table.EnsureArraySize(1000)
			arguments := []vm.Value{vm.NewTable(table)}
			b.ReportAllocs()
			for b.Loop() {
				for index := 1; index <= 1000; index++ {
					table.RawSetArray(index, vm.NewInt(int64(1001-index)))
				}
				if _, err := state.ProtectedCall(function, arguments); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
