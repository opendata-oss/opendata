package opendata_ingest_bindings

// #include <opendata_ingest_bindings.h>
import "C"

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"runtime"
	"sync/atomic"
	"unsafe"
)

// This is needed, because as of go 1.24
// type RustBuffer C.RustBuffer cannot have methods,
// RustBuffer is treated as non-local type
type GoRustBuffer struct {
	inner C.RustBuffer
}

type RustBufferI interface {
	AsReader() *bytes.Reader
	Free()
	ToGoBytes() []byte
	Data() unsafe.Pointer
	Len() uint64
	Capacity() uint64
}

// C.RustBuffer fields exposed as an interface so they can be accessed in different Go packages.
// See https://github.com/golang/go/issues/13467
type ExternalCRustBuffer interface {
	Data() unsafe.Pointer
	Len() uint64
	Capacity() uint64
}

func RustBufferFromC(b C.RustBuffer) ExternalCRustBuffer {
	return GoRustBuffer{
		inner: b,
	}
}

func CFromRustBuffer(b ExternalCRustBuffer) C.RustBuffer {
	return C.RustBuffer{
		capacity: C.uint64_t(b.Capacity()),
		len:      C.uint64_t(b.Len()),
		data:     (*C.uchar)(b.Data()),
	}
}

func RustBufferFromExternal(b ExternalCRustBuffer) GoRustBuffer {
	return GoRustBuffer{
		inner: C.RustBuffer{
			capacity: C.uint64_t(b.Capacity()),
			len:      C.uint64_t(b.Len()),
			data:     (*C.uchar)(b.Data()),
		},
	}
}

func (cb GoRustBuffer) Capacity() uint64 {
	return uint64(cb.inner.capacity)
}

func (cb GoRustBuffer) Len() uint64 {
	return uint64(cb.inner.len)
}

func (cb GoRustBuffer) Data() unsafe.Pointer {
	return unsafe.Pointer(cb.inner.data)
}

func (cb GoRustBuffer) AsReader() *bytes.Reader {
	b := unsafe.Slice((*byte)(cb.inner.data), C.uint64_t(cb.inner.len))
	return bytes.NewReader(b)
}

func (cb GoRustBuffer) Free() {
	rustCall(func(status *C.RustCallStatus) bool {
		C.ffi_opendata_ingest_bindings_rustbuffer_free(cb.inner, status)
		return false
	})
}

func (cb GoRustBuffer) ToGoBytes() []byte {
	return C.GoBytes(unsafe.Pointer(cb.inner.data), C.int(cb.inner.len))
}

func stringToRustBuffer(str string) C.RustBuffer {
	return bytesToRustBuffer([]byte(str))
}

func bytesToRustBuffer(b []byte) C.RustBuffer {
	if len(b) == 0 {
		return C.RustBuffer{}
	}
	// We can pass the pointer along here, as it is pinned
	// for the duration of this call
	foreign := C.ForeignBytes{
		len:  C.int(len(b)),
		data: (*C.uchar)(unsafe.Pointer(&b[0])),
	}

	return rustCall(func(status *C.RustCallStatus) C.RustBuffer {
		return C.ffi_opendata_ingest_bindings_rustbuffer_from_bytes(foreign, status)
	})
}

type BufLifter[GoType any] interface {
	Lift(value RustBufferI) GoType
}

type BufLowerer[GoType any] interface {
	Lower(value GoType) C.RustBuffer
}

type BufReader[GoType any] interface {
	Read(reader io.Reader) GoType
}

type BufWriter[GoType any] interface {
	Write(writer io.Writer, value GoType)
}

func LowerIntoRustBuffer[GoType any](bufWriter BufWriter[GoType], value GoType) C.RustBuffer {
	// This might be not the most efficient way but it does not require knowing allocation size
	// beforehand
	var buffer bytes.Buffer
	bufWriter.Write(&buffer, value)

	bytes, err := io.ReadAll(&buffer)
	if err != nil {
		panic(fmt.Errorf("reading written data: %w", err))
	}
	return bytesToRustBuffer(bytes)
}

func LiftFromRustBuffer[GoType any](bufReader BufReader[GoType], rbuf RustBufferI) GoType {
	defer rbuf.Free()
	reader := rbuf.AsReader()
	item := bufReader.Read(reader)
	if reader.Len() > 0 {
		// TODO: Remove this
		leftover, _ := io.ReadAll(reader)
		panic(fmt.Errorf("Junk remaining in buffer after lifting: %s", string(leftover)))
	}
	return item
}

func rustCallWithError[E any, U any](converter BufReader[*E], callback func(*C.RustCallStatus) U) (U, *E) {
	var status C.RustCallStatus
	returnValue := callback(&status)
	err := checkCallStatus(converter, status)
	return returnValue, err
}

func checkCallStatus[E any](converter BufReader[*E], status C.RustCallStatus) *E {
	switch status.code {
	case 0:
		return nil
	case 1:
		return LiftFromRustBuffer(converter, GoRustBuffer{inner: status.errorBuf})
	case 2:
		// when the rust code sees a panic, it tries to construct a rustBuffer
		// with the message.  but if that code panics, then it just sends back
		// an empty buffer.
		if status.errorBuf.len > 0 {
			panic(fmt.Errorf("%s", FfiConverterStringINSTANCE.Lift(GoRustBuffer{inner: status.errorBuf})))
		} else {
			panic(fmt.Errorf("Rust panicked while handling Rust panic"))
		}
	default:
		panic(fmt.Errorf("unknown status code: %d", status.code))
	}
}

func checkCallStatusUnknown(status C.RustCallStatus) error {
	switch status.code {
	case 0:
		return nil
	case 1:
		panic(fmt.Errorf("function not returning an error returned an error"))
	case 2:
		// when the rust code sees a panic, it tries to construct a C.RustBuffer
		// with the message.  but if that code panics, then it just sends back
		// an empty buffer.
		if status.errorBuf.len > 0 {
			panic(fmt.Errorf("%s", FfiConverterStringINSTANCE.Lift(GoRustBuffer{
				inner: status.errorBuf,
			})))
		} else {
			panic(fmt.Errorf("Rust panicked while handling Rust panic"))
		}
	default:
		return fmt.Errorf("unknown status code: %d", status.code)
	}
}

func rustCall[U any](callback func(*C.RustCallStatus) U) U {
	returnValue, err := rustCallWithError[error](nil, callback)
	if err != nil {
		panic(err)
	}
	return returnValue
}

type NativeError interface {
	AsError() error
}

func writeInt8(writer io.Writer, value int8) {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		panic(err)
	}
}

func writeUint8(writer io.Writer, value uint8) {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		panic(err)
	}
}

func writeInt16(writer io.Writer, value int16) {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		panic(err)
	}
}

func writeUint16(writer io.Writer, value uint16) {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		panic(err)
	}
}

func writeInt32(writer io.Writer, value int32) {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		panic(err)
	}
}

func writeUint32(writer io.Writer, value uint32) {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		panic(err)
	}
}

func writeInt64(writer io.Writer, value int64) {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		panic(err)
	}
}

func writeUint64(writer io.Writer, value uint64) {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		panic(err)
	}
}

func writeFloat32(writer io.Writer, value float32) {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		panic(err)
	}
}

func writeFloat64(writer io.Writer, value float64) {
	if err := binary.Write(writer, binary.BigEndian, value); err != nil {
		panic(err)
	}
}

func readInt8(reader io.Reader) int8 {
	var result int8
	if err := binary.Read(reader, binary.BigEndian, &result); err != nil {
		panic(err)
	}
	return result
}

func readUint8(reader io.Reader) uint8 {
	var result uint8
	if err := binary.Read(reader, binary.BigEndian, &result); err != nil {
		panic(err)
	}
	return result
}

func readInt16(reader io.Reader) int16 {
	var result int16
	if err := binary.Read(reader, binary.BigEndian, &result); err != nil {
		panic(err)
	}
	return result
}

func readUint16(reader io.Reader) uint16 {
	var result uint16
	if err := binary.Read(reader, binary.BigEndian, &result); err != nil {
		panic(err)
	}
	return result
}

func readInt32(reader io.Reader) int32 {
	var result int32
	if err := binary.Read(reader, binary.BigEndian, &result); err != nil {
		panic(err)
	}
	return result
}

func readUint32(reader io.Reader) uint32 {
	var result uint32
	if err := binary.Read(reader, binary.BigEndian, &result); err != nil {
		panic(err)
	}
	return result
}

func readInt64(reader io.Reader) int64 {
	var result int64
	if err := binary.Read(reader, binary.BigEndian, &result); err != nil {
		panic(err)
	}
	return result
}

func readUint64(reader io.Reader) uint64 {
	var result uint64
	if err := binary.Read(reader, binary.BigEndian, &result); err != nil {
		panic(err)
	}
	return result
}

func readFloat32(reader io.Reader) float32 {
	var result float32
	if err := binary.Read(reader, binary.BigEndian, &result); err != nil {
		panic(err)
	}
	return result
}

func readFloat64(reader io.Reader) float64 {
	var result float64
	if err := binary.Read(reader, binary.BigEndian, &result); err != nil {
		panic(err)
	}
	return result
}

func init() {

	uniffiCheckChecksums()
}

func uniffiCheckChecksums() {
	// Get the bindings contract version from our ComponentInterface
	bindingsContractVersion := 29
	// Get the scaffolding contract version by calling the into the dylib
	scaffoldingContractVersion := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint32_t {
		return C.ffi_opendata_ingest_bindings_uniffi_contract_version()
	})
	if bindingsContractVersion != int(scaffoldingContractVersion) {
		// If this happens try cleaning and rebuilding your project
		panic("opendata_ingest_bindings: UniFFI contract version mismatch")
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_opendata_ingest_bindings_checksum_method_ingestor_close()
		})
		if checksum != 24458 {
			// If this happens try cleaning and rebuilding your project
			panic("opendata_ingest_bindings: uniffi_opendata_ingest_bindings_checksum_method_ingestor_close: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_opendata_ingest_bindings_checksum_method_ingestor_flush()
		})
		if checksum != 20161 {
			// If this happens try cleaning and rebuilding your project
			panic("opendata_ingest_bindings: uniffi_opendata_ingest_bindings_checksum_method_ingestor_flush: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_opendata_ingest_bindings_checksum_method_ingestor_ingest()
		})
		if checksum != 19862 {
			// If this happens try cleaning and rebuilding your project
			panic("opendata_ingest_bindings: uniffi_opendata_ingest_bindings_checksum_method_ingestor_ingest: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_opendata_ingest_bindings_checksum_method_ingestor_ingest_many()
		})
		if checksum != 57019 {
			// If this happens try cleaning and rebuilding your project
			panic("opendata_ingest_bindings: uniffi_opendata_ingest_bindings_checksum_method_ingestor_ingest_many: UniFFI API checksum mismatch")
		}
	}
	{
		checksum := rustCall(func(_uniffiStatus *C.RustCallStatus) C.uint16_t {
			return C.uniffi_opendata_ingest_bindings_checksum_constructor_ingestor_new()
		})
		if checksum != 1766 {
			// If this happens try cleaning and rebuilding your project
			panic("opendata_ingest_bindings: uniffi_opendata_ingest_bindings_checksum_constructor_ingestor_new: UniFFI API checksum mismatch")
		}
	}
}

type FfiConverterUint64 struct{}

var FfiConverterUint64INSTANCE = FfiConverterUint64{}

func (FfiConverterUint64) Lower(value uint64) C.uint64_t {
	return C.uint64_t(value)
}

func (FfiConverterUint64) Write(writer io.Writer, value uint64) {
	writeUint64(writer, value)
}

func (FfiConverterUint64) Lift(value C.uint64_t) uint64 {
	return uint64(value)
}

func (FfiConverterUint64) Read(reader io.Reader) uint64 {
	return readUint64(reader)
}

type FfiDestroyerUint64 struct{}

func (FfiDestroyerUint64) Destroy(_ uint64) {}

type FfiConverterString struct{}

var FfiConverterStringINSTANCE = FfiConverterString{}

func (FfiConverterString) Lift(rb RustBufferI) string {
	defer rb.Free()
	reader := rb.AsReader()
	b, err := io.ReadAll(reader)
	if err != nil {
		panic(fmt.Errorf("reading reader: %w", err))
	}
	return string(b)
}

func (FfiConverterString) Read(reader io.Reader) string {
	length := readInt32(reader)
	buffer := make([]byte, length)
	read_length, err := reader.Read(buffer)
	if err != nil && err != io.EOF {
		panic(err)
	}
	if read_length != int(length) {
		panic(fmt.Errorf("bad read length when reading string, expected %d, read %d", length, read_length))
	}
	return string(buffer)
}

func (FfiConverterString) Lower(value string) C.RustBuffer {
	return stringToRustBuffer(value)
}

func (c FfiConverterString) LowerExternal(value string) ExternalCRustBuffer {
	return RustBufferFromC(stringToRustBuffer(value))
}

func (FfiConverterString) Write(writer io.Writer, value string) {
	if len(value) > math.MaxInt32 {
		panic("String is too large to fit into Int32")
	}

	writeInt32(writer, int32(len(value)))
	write_length, err := io.WriteString(writer, value)
	if err != nil {
		panic(err)
	}
	if write_length != len(value) {
		panic(fmt.Errorf("bad write length when writing string, expected %d, written %d", len(value), write_length))
	}
}

type FfiDestroyerString struct{}

func (FfiDestroyerString) Destroy(_ string) {}

type FfiConverterBytes struct{}

var FfiConverterBytesINSTANCE = FfiConverterBytes{}

func (c FfiConverterBytes) Lower(value []byte) C.RustBuffer {
	return LowerIntoRustBuffer[[]byte](c, value)
}

func (c FfiConverterBytes) LowerExternal(value []byte) ExternalCRustBuffer {
	return RustBufferFromC(c.Lower(value))
}

func (c FfiConverterBytes) Write(writer io.Writer, value []byte) {
	if len(value) > math.MaxInt32 {
		panic("[]byte is too large to fit into Int32")
	}

	writeInt32(writer, int32(len(value)))
	write_length, err := writer.Write(value)
	if err != nil {
		panic(err)
	}
	if write_length != len(value) {
		panic(fmt.Errorf("bad write length when writing []byte, expected %d, written %d", len(value), write_length))
	}
}

func (c FfiConverterBytes) Lift(rb RustBufferI) []byte {
	return LiftFromRustBuffer[[]byte](c, rb)
}

func (c FfiConverterBytes) Read(reader io.Reader) []byte {
	length := readInt32(reader)
	buffer := make([]byte, length)
	read_length, err := reader.Read(buffer)
	if err != nil && err != io.EOF {
		panic(err)
	}
	if read_length != int(length) {
		panic(fmt.Errorf("bad read length when reading []byte, expected %d, read %d", length, read_length))
	}
	return buffer
}

type FfiDestroyerBytes struct{}

func (FfiDestroyerBytes) Destroy(_ []byte) {}

// Below is an implementation of synchronization requirements outlined in the link.
// https://github.com/mozilla/uniffi-rs/blob/0dc031132d9493ca812c3af6e7dd60ad2ea95bf0/uniffi_bindgen/src/bindings/kotlin/templates/ObjectRuntime.kt#L31

type FfiObject struct {
	pointer       unsafe.Pointer
	callCounter   atomic.Int64
	cloneFunction func(unsafe.Pointer, *C.RustCallStatus) unsafe.Pointer
	freeFunction  func(unsafe.Pointer, *C.RustCallStatus)
	destroyed     atomic.Bool
}

func newFfiObject(
	pointer unsafe.Pointer,
	cloneFunction func(unsafe.Pointer, *C.RustCallStatus) unsafe.Pointer,
	freeFunction func(unsafe.Pointer, *C.RustCallStatus),
) FfiObject {
	return FfiObject{
		pointer:       pointer,
		cloneFunction: cloneFunction,
		freeFunction:  freeFunction,
	}
}

func (ffiObject *FfiObject) incrementPointer(debugName string) unsafe.Pointer {
	for {
		counter := ffiObject.callCounter.Load()
		if counter <= -1 {
			panic(fmt.Errorf("%v object has already been destroyed", debugName))
		}
		if counter == math.MaxInt64 {
			panic(fmt.Errorf("%v object call counter would overflow", debugName))
		}
		if ffiObject.callCounter.CompareAndSwap(counter, counter+1) {
			break
		}
	}

	return rustCall(func(status *C.RustCallStatus) unsafe.Pointer {
		return ffiObject.cloneFunction(ffiObject.pointer, status)
	})
}

func (ffiObject *FfiObject) decrementPointer() {
	if ffiObject.callCounter.Add(-1) == -1 {
		ffiObject.freeRustArcPtr()
	}
}

func (ffiObject *FfiObject) destroy() {
	if ffiObject.destroyed.CompareAndSwap(false, true) {
		if ffiObject.callCounter.Add(-1) == -1 {
			ffiObject.freeRustArcPtr()
		}
	}
}

func (ffiObject *FfiObject) freeRustArcPtr() {
	rustCall(func(status *C.RustCallStatus) int32 {
		ffiObject.freeFunction(ffiObject.pointer, status)
		return 0
	})
}

type IngestorInterface interface {
	Close() error
	Flush() error
	Ingest(payload []byte, metadata []byte) error
	IngestMany(payloads [][]byte, metadata []byte) error
}
type Ingestor struct {
	ffiObject FfiObject
}

func NewIngestor(config IngestorConfig) (*Ingestor, error) {
	_uniffiRV, _uniffiErr := rustCallWithError[IngestBindingError](FfiConverterIngestBindingError{}, func(_uniffiStatus *C.RustCallStatus) unsafe.Pointer {
		return C.uniffi_opendata_ingest_bindings_fn_constructor_ingestor_new(FfiConverterIngestorConfigINSTANCE.Lower(config), _uniffiStatus)
	})
	if _uniffiErr != nil {
		var _uniffiDefaultValue *Ingestor
		return _uniffiDefaultValue, _uniffiErr
	} else {
		return FfiConverterIngestorINSTANCE.Lift(_uniffiRV), nil
	}
}

func (_self *Ingestor) Close() error {
	_pointer := _self.ffiObject.incrementPointer("*Ingestor")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[IngestBindingError](FfiConverterIngestBindingError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_opendata_ingest_bindings_fn_method_ingestor_close(
			_pointer, _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}

func (_self *Ingestor) Flush() error {
	_pointer := _self.ffiObject.incrementPointer("*Ingestor")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[IngestBindingError](FfiConverterIngestBindingError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_opendata_ingest_bindings_fn_method_ingestor_flush(
			_pointer, _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}

func (_self *Ingestor) Ingest(payload []byte, metadata []byte) error {
	_pointer := _self.ffiObject.incrementPointer("*Ingestor")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[IngestBindingError](FfiConverterIngestBindingError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_opendata_ingest_bindings_fn_method_ingestor_ingest(
			_pointer, FfiConverterBytesINSTANCE.Lower(payload), FfiConverterBytesINSTANCE.Lower(metadata), _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}

func (_self *Ingestor) IngestMany(payloads [][]byte, metadata []byte) error {
	_pointer := _self.ffiObject.incrementPointer("*Ingestor")
	defer _self.ffiObject.decrementPointer()
	_, _uniffiErr := rustCallWithError[IngestBindingError](FfiConverterIngestBindingError{}, func(_uniffiStatus *C.RustCallStatus) bool {
		C.uniffi_opendata_ingest_bindings_fn_method_ingestor_ingest_many(
			_pointer, FfiConverterSequenceBytesINSTANCE.Lower(payloads), FfiConverterBytesINSTANCE.Lower(metadata), _uniffiStatus)
		return false
	})
	return _uniffiErr.AsError()
}
func (object *Ingestor) Destroy() {
	runtime.SetFinalizer(object, nil)
	object.ffiObject.destroy()
}

type FfiConverterIngestor struct{}

var FfiConverterIngestorINSTANCE = FfiConverterIngestor{}

func (c FfiConverterIngestor) Lift(pointer unsafe.Pointer) *Ingestor {
	result := &Ingestor{
		newFfiObject(
			pointer,
			func(pointer unsafe.Pointer, status *C.RustCallStatus) unsafe.Pointer {
				return C.uniffi_opendata_ingest_bindings_fn_clone_ingestor(pointer, status)
			},
			func(pointer unsafe.Pointer, status *C.RustCallStatus) {
				C.uniffi_opendata_ingest_bindings_fn_free_ingestor(pointer, status)
			},
		),
	}
	runtime.SetFinalizer(result, (*Ingestor).Destroy)
	return result
}

func (c FfiConverterIngestor) Read(reader io.Reader) *Ingestor {
	return c.Lift(unsafe.Pointer(uintptr(readUint64(reader))))
}

func (c FfiConverterIngestor) Lower(value *Ingestor) unsafe.Pointer {
	// TODO: this is bad - all synchronization from ObjectRuntime.go is discarded here,
	// because the pointer will be decremented immediately after this function returns,
	// and someone will be left holding onto a non-locked pointer.
	pointer := value.ffiObject.incrementPointer("*Ingestor")
	defer value.ffiObject.decrementPointer()
	return pointer

}

func (c FfiConverterIngestor) Write(writer io.Writer, value *Ingestor) {
	writeUint64(writer, uint64(uintptr(c.Lower(value))))
}

type FfiDestroyerIngestor struct{}

func (_ FfiDestroyerIngestor) Destroy(value *Ingestor) {
	value.Destroy()
}

type IngestorConfig struct {
	StorageBackend    StorageBackend
	DataPathPrefix    string
	ManifestPath      string
	FlushIntervalMs   uint64
	FlushSizeBytes    uint64
	MaxBufferedInputs uint64
	Compression       Compression
}

func (r *IngestorConfig) Destroy() {
	FfiDestroyerStorageBackend{}.Destroy(r.StorageBackend)
	FfiDestroyerString{}.Destroy(r.DataPathPrefix)
	FfiDestroyerString{}.Destroy(r.ManifestPath)
	FfiDestroyerUint64{}.Destroy(r.FlushIntervalMs)
	FfiDestroyerUint64{}.Destroy(r.FlushSizeBytes)
	FfiDestroyerUint64{}.Destroy(r.MaxBufferedInputs)
	FfiDestroyerCompression{}.Destroy(r.Compression)
}

type FfiConverterIngestorConfig struct{}

var FfiConverterIngestorConfigINSTANCE = FfiConverterIngestorConfig{}

func (c FfiConverterIngestorConfig) Lift(rb RustBufferI) IngestorConfig {
	return LiftFromRustBuffer[IngestorConfig](c, rb)
}

func (c FfiConverterIngestorConfig) Read(reader io.Reader) IngestorConfig {
	return IngestorConfig{
		FfiConverterStorageBackendINSTANCE.Read(reader),
		FfiConverterStringINSTANCE.Read(reader),
		FfiConverterStringINSTANCE.Read(reader),
		FfiConverterUint64INSTANCE.Read(reader),
		FfiConverterUint64INSTANCE.Read(reader),
		FfiConverterUint64INSTANCE.Read(reader),
		FfiConverterCompressionINSTANCE.Read(reader),
	}
}

func (c FfiConverterIngestorConfig) Lower(value IngestorConfig) C.RustBuffer {
	return LowerIntoRustBuffer[IngestorConfig](c, value)
}

func (c FfiConverterIngestorConfig) LowerExternal(value IngestorConfig) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[IngestorConfig](c, value))
}

func (c FfiConverterIngestorConfig) Write(writer io.Writer, value IngestorConfig) {
	FfiConverterStorageBackendINSTANCE.Write(writer, value.StorageBackend)
	FfiConverterStringINSTANCE.Write(writer, value.DataPathPrefix)
	FfiConverterStringINSTANCE.Write(writer, value.ManifestPath)
	FfiConverterUint64INSTANCE.Write(writer, value.FlushIntervalMs)
	FfiConverterUint64INSTANCE.Write(writer, value.FlushSizeBytes)
	FfiConverterUint64INSTANCE.Write(writer, value.MaxBufferedInputs)
	FfiConverterCompressionINSTANCE.Write(writer, value.Compression)
}

type FfiDestroyerIngestorConfig struct{}

func (_ FfiDestroyerIngestorConfig) Destroy(value IngestorConfig) {
	value.Destroy()
}

type Compression uint

const (
	CompressionNone Compression = 1
	CompressionZstd Compression = 2
)

type FfiConverterCompression struct{}

var FfiConverterCompressionINSTANCE = FfiConverterCompression{}

func (c FfiConverterCompression) Lift(rb RustBufferI) Compression {
	return LiftFromRustBuffer[Compression](c, rb)
}

func (c FfiConverterCompression) Lower(value Compression) C.RustBuffer {
	return LowerIntoRustBuffer[Compression](c, value)
}

func (c FfiConverterCompression) LowerExternal(value Compression) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[Compression](c, value))
}
func (FfiConverterCompression) Read(reader io.Reader) Compression {
	id := readInt32(reader)
	return Compression(id)
}

func (FfiConverterCompression) Write(writer io.Writer, value Compression) {
	writeInt32(writer, int32(value))
}

type FfiDestroyerCompression struct{}

func (_ FfiDestroyerCompression) Destroy(value Compression) {
}

type IngestBindingError struct {
	err error
}

// Convience method to turn *IngestBindingError into error
// Avoiding treating nil pointer as non nil error interface
func (err *IngestBindingError) AsError() error {
	if err == nil {
		return nil
	} else {
		return err
	}
}

func (err IngestBindingError) Error() string {
	return fmt.Sprintf("IngestBindingError: %s", err.err.Error())
}

func (err IngestBindingError) Unwrap() error {
	return err.err
}

// Err* are used for checking error type with `errors.Is`
var ErrIngestBindingErrorStorage = fmt.Errorf("IngestBindingErrorStorage")
var ErrIngestBindingErrorInvalidInput = fmt.Errorf("IngestBindingErrorInvalidInput")

// Variant structs
type IngestBindingErrorStorage struct {
	Message string
}

func NewIngestBindingErrorStorage(
	message string,
) *IngestBindingError {
	return &IngestBindingError{err: &IngestBindingErrorStorage{
		Message: message}}
}

func (e IngestBindingErrorStorage) destroy() {
	FfiDestroyerString{}.Destroy(e.Message)
}

func (err IngestBindingErrorStorage) Error() string {
	return fmt.Sprint("Storage",
		": ",

		"Message=",
		err.Message,
	)
}

func (self IngestBindingErrorStorage) Is(target error) bool {
	return target == ErrIngestBindingErrorStorage
}

type IngestBindingErrorInvalidInput struct {
	Message string
}

func NewIngestBindingErrorInvalidInput(
	message string,
) *IngestBindingError {
	return &IngestBindingError{err: &IngestBindingErrorInvalidInput{
		Message: message}}
}

func (e IngestBindingErrorInvalidInput) destroy() {
	FfiDestroyerString{}.Destroy(e.Message)
}

func (err IngestBindingErrorInvalidInput) Error() string {
	return fmt.Sprint("InvalidInput",
		": ",

		"Message=",
		err.Message,
	)
}

func (self IngestBindingErrorInvalidInput) Is(target error) bool {
	return target == ErrIngestBindingErrorInvalidInput
}

type FfiConverterIngestBindingError struct{}

var FfiConverterIngestBindingErrorINSTANCE = FfiConverterIngestBindingError{}

func (c FfiConverterIngestBindingError) Lift(eb RustBufferI) *IngestBindingError {
	return LiftFromRustBuffer[*IngestBindingError](c, eb)
}

func (c FfiConverterIngestBindingError) Lower(value *IngestBindingError) C.RustBuffer {
	return LowerIntoRustBuffer[*IngestBindingError](c, value)
}

func (c FfiConverterIngestBindingError) LowerExternal(value *IngestBindingError) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[*IngestBindingError](c, value))
}

func (c FfiConverterIngestBindingError) Read(reader io.Reader) *IngestBindingError {
	errorID := readUint32(reader)

	switch errorID {
	case 1:
		return &IngestBindingError{&IngestBindingErrorStorage{
			Message: FfiConverterStringINSTANCE.Read(reader),
		}}
	case 2:
		return &IngestBindingError{&IngestBindingErrorInvalidInput{
			Message: FfiConverterStringINSTANCE.Read(reader),
		}}
	default:
		panic(fmt.Sprintf("Unknown error code %d in FfiConverterIngestBindingError.Read()", errorID))
	}
}

func (c FfiConverterIngestBindingError) Write(writer io.Writer, value *IngestBindingError) {
	switch variantValue := value.err.(type) {
	case *IngestBindingErrorStorage:
		writeInt32(writer, 1)
		FfiConverterStringINSTANCE.Write(writer, variantValue.Message)
	case *IngestBindingErrorInvalidInput:
		writeInt32(writer, 2)
		FfiConverterStringINSTANCE.Write(writer, variantValue.Message)
	default:
		_ = variantValue
		panic(fmt.Sprintf("invalid error value `%v` in FfiConverterIngestBindingError.Write", value))
	}
}

type FfiDestroyerIngestBindingError struct{}

func (_ FfiDestroyerIngestBindingError) Destroy(value *IngestBindingError) {
	switch variantValue := value.err.(type) {
	case IngestBindingErrorStorage:
		variantValue.destroy()
	case IngestBindingErrorInvalidInput:
		variantValue.destroy()
	default:
		_ = variantValue
		panic(fmt.Sprintf("invalid error value `%v` in FfiDestroyerIngestBindingError.Destroy", value))
	}
}

type StorageBackend interface {
	Destroy()
}
type StorageBackendInMemory struct {
}

func (e StorageBackendInMemory) Destroy() {
}

type StorageBackendLocal struct {
	Path string
}

func (e StorageBackendLocal) Destroy() {
	FfiDestroyerString{}.Destroy(e.Path)
}

type StorageBackendAws struct {
	Bucket string
	Region string
}

func (e StorageBackendAws) Destroy() {
	FfiDestroyerString{}.Destroy(e.Bucket)
	FfiDestroyerString{}.Destroy(e.Region)
}

type FfiConverterStorageBackend struct{}

var FfiConverterStorageBackendINSTANCE = FfiConverterStorageBackend{}

func (c FfiConverterStorageBackend) Lift(rb RustBufferI) StorageBackend {
	return LiftFromRustBuffer[StorageBackend](c, rb)
}

func (c FfiConverterStorageBackend) Lower(value StorageBackend) C.RustBuffer {
	return LowerIntoRustBuffer[StorageBackend](c, value)
}

func (c FfiConverterStorageBackend) LowerExternal(value StorageBackend) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[StorageBackend](c, value))
}
func (FfiConverterStorageBackend) Read(reader io.Reader) StorageBackend {
	id := readInt32(reader)
	switch id {
	case 1:
		return StorageBackendInMemory{}
	case 2:
		return StorageBackendLocal{
			FfiConverterStringINSTANCE.Read(reader),
		}
	case 3:
		return StorageBackendAws{
			FfiConverterStringINSTANCE.Read(reader),
			FfiConverterStringINSTANCE.Read(reader),
		}
	default:
		panic(fmt.Sprintf("invalid enum value %v in FfiConverterStorageBackend.Read()", id))
	}
}

func (FfiConverterStorageBackend) Write(writer io.Writer, value StorageBackend) {
	switch variant_value := value.(type) {
	case StorageBackendInMemory:
		writeInt32(writer, 1)
	case StorageBackendLocal:
		writeInt32(writer, 2)
		FfiConverterStringINSTANCE.Write(writer, variant_value.Path)
	case StorageBackendAws:
		writeInt32(writer, 3)
		FfiConverterStringINSTANCE.Write(writer, variant_value.Bucket)
		FfiConverterStringINSTANCE.Write(writer, variant_value.Region)
	default:
		_ = variant_value
		panic(fmt.Sprintf("invalid enum value `%v` in FfiConverterStorageBackend.Write", value))
	}
}

type FfiDestroyerStorageBackend struct{}

func (_ FfiDestroyerStorageBackend) Destroy(value StorageBackend) {
	value.Destroy()
}

type FfiConverterSequenceBytes struct{}

var FfiConverterSequenceBytesINSTANCE = FfiConverterSequenceBytes{}

func (c FfiConverterSequenceBytes) Lift(rb RustBufferI) [][]byte {
	return LiftFromRustBuffer[[][]byte](c, rb)
}

func (c FfiConverterSequenceBytes) Read(reader io.Reader) [][]byte {
	length := readInt32(reader)
	if length == 0 {
		return nil
	}
	result := make([][]byte, 0, length)
	for i := int32(0); i < length; i++ {
		result = append(result, FfiConverterBytesINSTANCE.Read(reader))
	}
	return result
}

func (c FfiConverterSequenceBytes) Lower(value [][]byte) C.RustBuffer {
	return LowerIntoRustBuffer[[][]byte](c, value)
}

func (c FfiConverterSequenceBytes) LowerExternal(value [][]byte) ExternalCRustBuffer {
	return RustBufferFromC(LowerIntoRustBuffer[[][]byte](c, value))
}

func (c FfiConverterSequenceBytes) Write(writer io.Writer, value [][]byte) {
	if len(value) > math.MaxInt32 {
		panic("[][]byte is too large to fit into Int32")
	}

	writeInt32(writer, int32(len(value)))
	for _, item := range value {
		FfiConverterBytesINSTANCE.Write(writer, item)
	}
}

type FfiDestroyerSequenceBytes struct{}

func (FfiDestroyerSequenceBytes) Destroy(sequence [][]byte) {
	for _, value := range sequence {
		FfiDestroyerBytes{}.Destroy(value)
	}
}
