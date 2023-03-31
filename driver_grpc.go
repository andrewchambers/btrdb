//go:build !arrow

package btrdb

import (
	"context"
	"io"

	pb "github.com/BTrDB/btrdb/v5/v5api"
	"github.com/pborman/uuid"
)

func (b *Endpoint) InsertGeneric(ctx context.Context, uu uuid.UUID, values []*pb.RawPoint, p *InsertParams) error {
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
	rv, err := b.g.Insert(ctx, &pb.InsertParams{
		Uuid:        uu,
		Sync:        false,
		Values:      values,
		MergePolicy: policy,
		Rounding:    rounding,
	})
	if err != nil {
		return err
	}
	if rv.GetStat() != nil {
		return &CodedError{rv.GetStat()}
	}
	return nil
}

func (b *Endpoint) InsertUnique(ctx context.Context, uu uuid.UUID, values []*pb.RawPoint, mp MergePolicy) error {
	return b.InsertGeneric(ctx, uu, values, &InsertParams{MergePolicy: mp})
}

// Insert is a low level function, rather use Stream.Insert()
func (b *Endpoint) Insert(ctx context.Context, uu uuid.UUID, values []*pb.RawPoint, p *InsertParams) error {
	return b.InsertGeneric(ctx, uu, values, p)
}

// ForEachRawValue is a low level function, rather use Stream.ForEachRawValue()
func (b *Endpoint) ForEachRawValue(ctx context.Context, uu uuid.UUID, start int64, end int64, version uint64, f func(RawPoint)) (uint64, error) {
	rv, err := b.g.RawValues(ctx, &pb.RawValuesParams{
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
		for _, x := range rawv.Values {
			f(RawPoint{x.Time, x.Value})
		}
	}
}

// RawValues is a low level function, rather use Stream.RawValues()
func (b *Endpoint) RawValues(ctx context.Context, uu uuid.UUID, start int64, end int64, version uint64) (chan RawPoint, chan uint64, chan error) {
	rv, err := b.g.RawValues(ctx, &pb.RawValuesParams{
		Uuid:         uu,
		Start:        start,
		End:          end,
		VersionMajor: version,
	})
	rvc := make(chan RawPoint, 100)
	rvv := make(chan uint64, 1)
	rve := make(chan error, 1)
	wroteVer := false
	if err != nil {
		close(rvv)
		close(rvc)
		rve <- err
		close(rve)
		return rvc, rvv, rve
	}
	go func() {
		for {
			rawv, err := rv.Recv()
			if err == io.EOF {
				close(rvc)
				close(rvv)
				close(rve)
				return
			}
			if err != nil {
				close(rvc)
				close(rvv)
				rve <- err
				close(rve)
				return
			}
			if rawv.Stat != nil {
				close(rvc)
				close(rvv)
				rve <- &CodedError{rawv.Stat}
				close(rve)
				return
			}
			if !wroteVer {
				rvv <- rawv.VersionMajor
				wroteVer = true
			}
			for _, x := range rawv.Values {
				rvc <- RawPoint{x.Time, x.Value}
			}
		}
	}()
	return rvc, rvv, rve
}

// AlignedWindows is a low level function, rather use Stream.AlignedWindows()
func (b *Endpoint) AlignedWindows(ctx context.Context, uu uuid.UUID, start int64, end int64, pointwidth uint8, version uint64) (chan StatPoint, chan uint64, chan error) {
	rv, err := b.g.AlignedWindows(ctx, &pb.AlignedWindowsParams{
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
	if err != nil {
		close(rvc)
		close(rvv)
		rve <- err
		close(rve)
		return rvc, rvv, rve
	}
	go func() {
		for {
			rawv, err := rv.Recv()
			if err == io.EOF {
				close(rvc)
				close(rvv)
				close(rve)
				return
			}
			if err != nil {
				close(rvc)
				close(rvv)
				rve <- err
				close(rve)
				return
			}
			if rawv.Stat != nil {
				close(rvc)
				close(rvv)
				rve <- &CodedError{rawv.Stat}
				close(rve)
				return
			}
			if !wroteVer {
				rvv <- rawv.VersionMajor
				wroteVer = true
			}
			for _, x := range rawv.Values {
				rvc <- StatPoint{
					Time:   x.Time,
					Min:    x.Min,
					Mean:   x.Mean,
					Max:    x.Max,
					Count:  x.Count,
					StdDev: x.Stddev,
				}
			}
		}
	}()
	return rvc, rvv, rve
}

// Windows is a low level function, rather use Stream.Windows()
func (b *Endpoint) Windows(ctx context.Context, uu uuid.UUID, start int64, end int64, width uint64, depth uint8, version uint64) (chan StatPoint, chan uint64, chan error) {
	rv, err := b.g.Windows(ctx, &pb.WindowsParams{
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
	if err != nil {
		close(rvc)
		close(rvv)
		rve <- err
		close(rve)
		return rvc, rvv, rve
	}
	go func() {
		for {
			rawv, err := rv.Recv()
			if err == io.EOF {
				close(rvc)
				close(rvv)
				close(rve)
				return
			}
			if err != nil {
				close(rvc)
				close(rvv)
				rve <- err
				close(rve)
				return
			}
			if rawv.Stat != nil {
				close(rvc)
				close(rvv)
				rve <- &CodedError{rawv.Stat}
				close(rve)
				return
			}
			if !wroteVer {
				rvv <- rawv.VersionMajor
				wroteVer = true
			}
			for _, x := range rawv.Values {
				rvc <- StatPoint{
					Time:   x.Time,
					Min:    x.Min,
					Mean:   x.Mean,
					Max:    x.Max,
					Count:  x.Count,
					StdDev: x.Stddev,
				}
			}
		}
	}()
	return rvc, rvv, rve
}
