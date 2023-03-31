//go:build arrow

package btrdb

import (
	"github.com/apache/arrow/go/v11/arrow"
	"github.com/apache/arrow/go/v11/arrow/memory"

	"reflect"
	"unsafe"
)

var (
	arrowTimeValueSchema *arrow.Schema = arrow.NewSchema(
		[]arrow.Field{
			{Name: "time", Type: arrow.FixedWidthTypes.Timestamp_ns},
			{Name: "value", Type: arrow.PrimitiveTypes.Float64},
		},
		nil,
	)

	arrowWindowsSchema *arrow.Schema = arrow.NewSchema(
		[]arrow.Field{
			{Name: "time", Type: arrow.FixedWidthTypes.Timestamp_ns},
			{Name: "mean", Type: arrow.PrimitiveTypes.Float64},
			{Name: "min", Type: arrow.PrimitiveTypes.Float64},
			{Name: "max", Type: arrow.PrimitiveTypes.Float64},
			{Name: "count", Type: arrow.PrimitiveTypes.Uint64},
			{Name: "stddev", Type: arrow.PrimitiveTypes.Float64},
		},
		nil,
	)

	theArrowAllocator = &arrowAllocator{}
)

type arrowAllocator struct {
	a memory.GoAllocator
}

func (a *arrowAllocator) Allocate(size int) []byte {
	return a.a.Allocate(size)
}

func (a *arrowAllocator) Reallocate(size int, b []byte) []byte {
	if cap(b) >= size {
		// XXX work around performance bug in the default arrow allocator
		// where it doesn't take advantage of the buffer capacity. Remove
		// once this is fixed upstream.
		return b[:size]
	}
	return a.a.Reallocate(size, b)
}

func (a *arrowAllocator) Free(b []byte) {
	a.a.Free(b)
}

func unsafeCastInt64SliceToTimestampSlice(int64Slice []int64) []arrow.Timestamp {
	int64Header := (*reflect.SliceHeader)(unsafe.Pointer(&int64Slice))
	var timestampSlice []arrow.Timestamp
	timestampHeader := (*reflect.SliceHeader)(unsafe.Pointer(&timestampSlice))
	timestampHeader.Data = int64Header.Data
	timestampHeader.Len = int64Header.Len
	timestampHeader.Cap = int64Header.Cap
	return timestampSlice
}
