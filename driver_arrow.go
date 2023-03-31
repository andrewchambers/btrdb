//go:build arrow

package btrdb

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"runtime"

	pb "github.com/BTrDB/btrdb/v5/v5api"
	"github.com/pborman/uuid"

	"github.com/apache/arrow/go/v11/arrow"
	"github.com/apache/arrow/go/v11/arrow/array"
	"github.com/apache/arrow/go/v11/arrow/ipc"
)

func (b *Endpoint) insertArrow(ctx context.Context, uu uuid.UUID, arrowBytes []byte, p *InsertParams) error {
	policy := pb.MergePolicy_NEVER
	rounding := (*pb.RoundSpec)(nil)
	if p != nil {
		if p.RoundBits != nil {
			rounding = &pb.RoundSpec{
				Spec: &pb.RoundSpec_Bits{Bits: int32(*p.RoundBits)},
			}
		}
		switch p.MergePolicy {
		case MPNever:
			policy = pb.MergePolicy_NEVER
		case MPEqual:
			policy = pb.MergePolicy_EQUAL
		case MPRetain:
			policy = pb.MergePolicy_RETAIN
		case MPReplace:
			policy = pb.MergePolicy_REPLACE
		}
	}
	rv, err := b.g.ArrowInsert(ctx, &pb.ArrowInsertParams{
		Uuid:        uu,
		Sync:        false,
		MergePolicy: policy,
		Rounding:    rounding,
		ArrowBytes:  arrowBytes,
	})
	if err != nil {
		return err
	}
	if rv.GetStat() != nil {
		return &CodedError{rv.GetStat()}
	}
	return nil
}

// InsertTV is a low level function, rather use Stream.InsertTV()
func (b *Endpoint) InsertTV(ctx context.Context, uu uuid.UUID, times []int64, values []float64, p *InsertParams) error {
	recordBuilder := array.NewRecordBuilder(theArrowAllocator, arrowTimeValueSchema)
	recordBuilder.Reserve(len(times))
	recordBuilder.Field(0).(*array.TimestampBuilder).AppendValues(unsafeCastInt64SliceToTimestampSlice(times), nil)
	recordBuilder.Field(1).(*array.Float64Builder).AppendValues(values, nil)
	rec := recordBuilder.NewRecord()
	defer rec.Release()
	buf := bytes.Buffer{}
	buf.Grow(len(times) * 16)
	w := ipc.NewWriter(
		&buf, ipc.WithAllocator(theArrowAllocator), ipc.WithSchema(arrowTimeValueSchema),
	)
	err := w.Write(rec)
	if err != nil {
		return err
	}
	err = w.Close()
	if err != nil {
		return err
	}
	runtime.KeepAlive(times) // Keep times alive for our unsafe cast.
	return b.insertArrow(ctx, uu, buf.Bytes(), p)
}

// InsertPoints is a low level function, rather use Stream.InsertPoints()
func (b *Endpoint) InsertPoints(ctx context.Context, uu uuid.UUID, points []RawPoint, p *InsertParams) error {
	recordBuilder := array.NewRecordBuilder(theArrowAllocator, arrowTimeValueSchema)
	recordBuilder.Reserve(len(points))
	timeBuilder := recordBuilder.Field(0).(*array.TimestampBuilder)
	valueBuilder := recordBuilder.Field(1).(*array.Float64Builder)
	for _, p := range points {
		timeBuilder.Append(arrow.Timestamp(p.Time))
		valueBuilder.Append(p.Value)
	}
	rec := recordBuilder.NewRecord()
	defer rec.Release()
	buf := &bytes.Buffer{}
	buf.Grow(len(points) * 16)
	w := ipc.NewWriter(
		buf, ipc.WithAllocator(theArrowAllocator), ipc.WithSchema(arrowTimeValueSchema),
	)
	err := w.Write(rec)
	if err != nil {
		return err
	}
	err = w.Close()
	if err != nil {
		return err
	}
	return b.insertArrow(ctx, uu, buf.Bytes(), p)
}

// ForEachRawValue is a low level function, rather use Stream.ForEachRawValue()
func (b *Endpoint) ForEachRawValue(ctx context.Context, uu uuid.UUID, start int64, end int64, version uint64, f func(RawPoint)) (uint64, error) {
	rv, err := b.g.ArrowRawValues(ctx, &pb.RawValuesParams{
		Uuid:         uu,
		Start:        start,
		End:          end,
		VersionMajor: version,
	})
	if err != nil {
		return 0, err
	}
	ver := uint64(0)
	for {
		rawv, err := rv.Recv()
		if err == io.EOF {
			return ver, nil
		}
		if err != nil {
			return 0, err
		}
		if rawv.Stat != nil {
			return 0, &CodedError{rawv.Stat}
		}
		if ver == 0 {
			ver = rawv.VersionMajor
		}
		reader, err := ipc.NewReader(
			bytes.NewReader(rawv.ArrowBytes),
			ipc.WithAllocator(theArrowAllocator),
			ipc.WithSchema(arrowTimeValueSchema),
		)
		if err != nil {
			return 0, fmt.Errorf("error opening arrow buffer: %w", err)
		}
		for reader.Next() {
			record := reader.Record()
			times := record.Column(0).(*array.Timestamp)
			values := record.Column(1).(*array.Float64)
			len := times.Len()
			for i := 0; i < len; i++ {
				f(RawPoint{int64(times.Value(i)), values.Value(i)})
			}
			record.Release()
		}
		if err := reader.Err(); err != nil {
			return 0, fmt.Errorf("error decoding arrow buffer: %w", err)
		}
	}
}

// RawValues is a low level function, rather use Stream.RawValues()
func (b *Endpoint) RawValues(ctx context.Context, uu uuid.UUID, start int64, end int64, version uint64) (chan RawPoint, chan uint64, chan error) {
	rv, err := b.g.ArrowRawValues(ctx, &pb.RawValuesParams{
		Uuid:         uu,
		Start:        start,
		End:          end,
		VersionMajor: version,
	})
	rvc := make(chan RawPoint, 100)
	rvv := make(chan uint64, 1)
	rve := make(chan error, 1)
	wroteVer := false

	sendErr := func(err error) {
		if err != nil {
			rve <- err
		}
		close(rvc)
		close(rvv)
		close(rve)
	}

	if err != nil {
		sendErr(err)
		return rvc, rvv, rve
	}

	go func() {
		for {
			rawv, err := rv.Recv()
			if err == io.EOF {
				sendErr(nil)
				return
			}
			if err != nil {
				sendErr(err)
				return
			}
			if rawv.Stat != nil {
				sendErr(&CodedError{rawv.Stat})
				return
			}
			if !wroteVer {
				rvv <- rawv.VersionMajor
				wroteVer = true
			}

			reader, err := ipc.NewReader(
				bytes.NewReader(rawv.ArrowBytes),
				ipc.WithAllocator(theArrowAllocator),
				ipc.WithSchema(arrowTimeValueSchema),
			)
			if err != nil {
				sendErr(fmt.Errorf("error opening arrow buffer: %w", err))
				return
			}

			for reader.Next() {
				record := reader.Record()
				times := record.Column(0).(*array.Timestamp)
				values := record.Column(1).(*array.Float64)
				len := times.Len()
				for i := 0; i < len; i++ {
					rvc <- RawPoint{int64(times.Value(i)), values.Value(i)}
				}
				record.Release()
			}
			if err := reader.Err(); err != nil {
				sendErr(fmt.Errorf("error decoding arrow buffer: %w", err))
				return
			}
		}
	}()
	return rvc, rvv, rve
}

// AlignedWindows is a low level function, rather use Stream.AlignedWindows()
func (b *Endpoint) AlignedWindows(ctx context.Context, uu uuid.UUID, start int64, end int64, pointwidth uint8, version uint64) (chan StatPoint, chan uint64, chan error) {
	rv, err := b.g.ArrowAlignedWindows(ctx, &pb.AlignedWindowsParams{
		Uuid:         uu,
		Start:        start,
		End:          end,
		PointWidth:   uint32(pointwidth),
		VersionMajor: version,
	})
	rvc := make(chan StatPoint, 100)
	rvv := make(chan uint64, 1)
	rve := make(chan error, 1)
	wroteVer := false

	sendErr := func(err error) {
		if err != nil {
			rve <- err
		}
		close(rvc)
		close(rvv)
		close(rve)
	}

	if err != nil {
		sendErr(err)
		return rvc, rvv, rve
	}

	go func() {
		for {
			rawv, err := rv.Recv()
			if err == io.EOF {
				sendErr(nil)
				return
			}
			if err != nil {
				sendErr(err)
				return
			}
			if rawv.Stat != nil {
				sendErr(&CodedError{rawv.Stat})
				return
			}
			if !wroteVer {
				rvv <- rawv.VersionMajor
				wroteVer = true
			}

			reader, err := ipc.NewReader(
				bytes.NewReader(rawv.ArrowBytes),
				ipc.WithAllocator(theArrowAllocator),
				ipc.WithSchema(arrowWindowsSchema),
			)
			if err != nil {
				sendErr(fmt.Errorf("error opening arrow buffer: %w", err))
				return
			}

			for reader.Next() {
				record := reader.Record()
				times := record.Column(0).(*array.Timestamp)
				means := record.Column(1).(*array.Float64)
				mins := record.Column(2).(*array.Float64)
				maxs := record.Column(3).(*array.Float64)
				counts := record.Column(4).(*array.Uint64)
				stddevs := record.Column(5).(*array.Float64)
				len := times.Len()
				for i := 0; i < len; i++ {
					rvc <- StatPoint{
						Time:   int64(times.Value(i)),
						Min:    mins.Value(i),
						Mean:   means.Value(i),
						Max:    maxs.Value(i),
						Count:  counts.Value(i),
						StdDev: stddevs.Value(i),
					}
				}
				record.Release()
			}
			if err := reader.Err(); err != nil {
				sendErr(fmt.Errorf("error decoding arrow buffer: %w", err))
				return
			}
		}
	}()
	return rvc, rvv, rve
}

// Windows is a low level function, rather use Stream.Windows()
func (b *Endpoint) Windows(ctx context.Context, uu uuid.UUID, start int64, end int64, width uint64, depth uint8, version uint64) (chan StatPoint, chan uint64, chan error) {
	rv, err := b.g.ArrowWindows(ctx, &pb.WindowsParams{
		Uuid:         uu,
		Start:        start,
		End:          end,
		Width:        width,
		Depth:        uint32(depth),
		VersionMajor: version,
	})
	rvc := make(chan StatPoint, 100)
	rvv := make(chan uint64, 1)
	rve := make(chan error, 1)
	wroteVer := false

	sendErr := func(err error) {
		if err != nil {
			rve <- err
		}
		close(rvc)
		close(rvv)
		close(rve)
	}

	if err != nil {
		sendErr(err)
		return rvc, rvv, rve
	}

	go func() {
		for {
			rawv, err := rv.Recv()
			if err == io.EOF {
				sendErr(nil)
				return
			}
			if err != nil {
				sendErr(err)
				return
			}
			if rawv.Stat != nil {
				sendErr(&CodedError{rawv.Stat})
				return
			}
			if !wroteVer {
				rvv <- rawv.VersionMajor
				wroteVer = true
			}

			reader, err := ipc.NewReader(
				bytes.NewReader(rawv.ArrowBytes),
				ipc.WithAllocator(theArrowAllocator),
				ipc.WithSchema(arrowWindowsSchema),
			)
			if err != nil {
				sendErr(fmt.Errorf("error opening arrow buffer: %w", err))
				return
			}

			for reader.Next() {
				record := reader.Record()
				times := record.Column(0).(*array.Timestamp)
				means := record.Column(1).(*array.Float64)
				mins := record.Column(2).(*array.Float64)
				maxs := record.Column(3).(*array.Float64)
				counts := record.Column(4).(*array.Uint64)
				stddevs := record.Column(5).(*array.Float64)
				len := times.Len()
				for i := 0; i < len; i++ {
					rvc <- StatPoint{
						Time:   int64(times.Value(i)),
						Min:    mins.Value(i),
						Mean:   means.Value(i),
						Max:    maxs.Value(i),
						Count:  counts.Value(i),
						StdDev: stddevs.Value(i),
					}
				}
				record.Release()
			}
			if err := reader.Err(); err != nil {
				sendErr(fmt.Errorf("error decoding arrow buffer: %w", err))
				return
			}
		}
	}()
	return rvc, rvv, rve
}
