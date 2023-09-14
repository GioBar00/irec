package wasm

import (
	"unsafe"

	"github.com/bytecodealliance/wasmtime-go/v12"

	"github.com/scionproto/scion/pkg/log"
	"github.com/scionproto/scion/rac"
	"github.com/scionproto/scion/rac/config"
)

type WasmEnv struct {
	Config *wasmtime.Config
	Engine *wasmtime.Engine
	//ModuleStore map[string]*wasmtime.Module
	Writer   rac.EgressWriter
	staticVM *wasmtime.Module
}

func (w *WasmEnv) Initialize() {
	w.Config = wasmtime.NewConfig()
	//w.Config.SetWasmMemory64(true)
	w.Engine = wasmtime.NewEngineWithConfig(w.Config)
}
func (w *WasmEnv) InitStaticAlgo(alg config.RACAlgorithm) {
	// Load algorithms into some cache.
	module, err := wasmtime.NewModuleFromFile(w.Engine, alg.FilePath)
	if err != nil {
		log.Error("Algorithm not loaded due to error", alg.FilePath, err)
		return
	}
	w.staticVM = module
}

func guestWriteBytes(instance *wasmtime.Instance, store *wasmtime.Store, bytes []byte) (int32, error) {
	mem := instance.GetExport(store, "memory").Memory()
	_, err := mem.Grow(store, 4092)
	if err != nil {
		return 0, err
	}
	// Use alloc in the WASM module to allocate memory for the data.
	ptr, err := instance.GetExport(store, "__alloc").Func().Call(store, len(bytes), 0)

	if err != nil {
		return 0, err
	}
	// This yields a pointer, which is where the data will be placed.
	dst := unsafe.Pointer(uintptr(mem.Data(store)) + uintptr(ptr.(int32)))
	// Write the data to the linear memory at this pointer.
	copy(unsafe.Slice((*byte)(dst), len(bytes)), bytes)
	return ptr.(int32), nil
}

// AdditionalInfo is given when called
type AdditionalInfo struct {
	PropagationInterfaces []uint32
}
