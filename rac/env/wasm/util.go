package wasm

import (
	"encoding/json"
	wasmtime "github.com/bytecodealliance/wasmtime-go/v12"
	"github.com/scionproto/scion/pkg/addr"
	"github.com/scionproto/scion/pkg/log"
	"github.com/scionproto/scion/private/topology"
	"io/ioutil"
	"unsafe"
)

// Some utility functions and others for future usage.
type InterfaceStaticInfo struct {
	ID       uint16
	IA       addr.IA
	LinkType topology.LinkType // this is going to be consistently child or core?
	RemoteID uint16
	MTU      uint16
}
type InterfaceDynamicInfo struct {
	//	Things like bw
}
type InterfaceInfo struct {
	Dynamic InterfaceDynamicInfo
	Static  InterfaceStaticInfo
}

// RegisterInfo is given after registering and saved locally. This is constant data.
type RegisterInfo struct {
	IntfInfo []InterfaceStaticInfo
}

func readFromKv(key string) json.RawMessage {

	var data []byte
	data, _ = ioutil.ReadFile("kv.json")
	c := make(map[string]json.RawMessage)
	_ = json.Unmarshal(data, &c)
	return c[key]
}
func hookKvFns(linker *wasmtime.Linker, store *wasmtime.Store) {

	linker.DefineFunc(store, "env", "__variable_size", func(caller *wasmtime.Caller, offset int32, length int32, value int32) {
		if offset < 0 {
			log.Error("Invalid offset")
		}
		if length < 0 {
			log.Error("Invalid length")
		}
		mem := caller.GetExport("memory").Memory()
		kv := readFromKv(string(mem.UnsafeData(store)[offset : offset+length]))
		bytes, _ := kv.MarshalJSON()
		mem.UnsafeData(store)[value] = byte(len(bytes))
	})

	linker.DefineFunc(store, "env", "__get_v_ptr", func(caller *wasmtime.Caller, keyptr int32, keylen int32, valptr int32, vallen int32) {
		if keyptr < 0 || keylen < 0 || valptr < 0 || vallen < 0 {
			log.Error("Invalid args")
		}
		mem := caller.GetExport("memory").Memory()

		kv := readFromKv(string(mem.UnsafeData(store)[keyptr : keyptr+keylen]))
		bytes, err := kv.MarshalJSON()
		if err != nil {
			log.Error("error at get_v_ptr", "err", err)
		}
		copy(unsafe.Slice((*byte)(unsafe.Pointer(uintptr(mem.Data(store))+uintptr(valptr))), vallen), bytes)
	})
}

//
//func (w *WasmEnv) guestPrintMemRegion(instance *wasmtime.Instance, offset, length int32) {
//	mem := instance.GetExport(w.store, "memory").Memory()
//	log.Info("", "memregion", mem.UnsafeData(w.store)[offset : offset+length])
//}
//
