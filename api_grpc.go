//go:build !arrow

package btrdb

import (
	"context"

	pb "github.com/BTrDB/btrdb/v5/v5api"
)

// InsertTV allows insertion of two equal length arrays, one containing times and
// the other containing values. The arrays need not be sorted, but they must correspond
// (i.e the first element of times is the time for the firt element of values). If the
// arrays are larger than appropriate, this function will automatically chunk the inserts.
// As a consequence, the insert is not necessarily atomic, but can be used with
// very large arrays.
func (s *Stream) InsertTV(ctx context.Context, times []int64, values []float64, p *InsertParams) error {
	if len(times) != len(values) {
		return ErrorWrongArgs
	}
	var ep *Endpoint
	var err error
	batchsize := 50000
	for len(times) > 0 {
		err = forceEp
		end := len(times)
		if end > batchsize {
			end = batchsize
		}
		thisBatchT := times[:end]
		thisBatchV := values[:end]
		//TODO pool or reuse
		pbraws := make([]*pb.RawPoint, len(thisBatchT))
		for i := 0; i < len(thisBatchT); i++ {
			pbraws[i] = &pb.RawPoint{
				Time:  thisBatchT[i],
				Value: thisBatchV[i],
			}
		}
		for s.b.TestEpError(ep, err) {
			ep, err = s.b.EndpointFor(ctx, s.uuid)
			if err != nil {
				continue
			}
			err = ep.Insert(ctx, s.uuid, pbraws, p)
		}
		if err != nil {
			return err
		}
		times = times[end:]
		values = values[end:]
	}
	return nil
}

func (s *Stream) InsertGeneric(ctx context.Context, vals []RawPoint, p *InsertParams) error {
	var ep *Endpoint
	var err error
	batchsize := 50000
	for len(vals) > 0 {
		err = forceEp
		end := len(vals)
		if end > batchsize {
			end = batchsize
		}
		thisBatch := vals[:end]
		//TODO pool or reuse
		pbraws := make([]*pb.RawPoint, len(thisBatch))
		for idx, p := range thisBatch {
			pbraws[idx] = &pb.RawPoint{
				Time:  p.Time,
				Value: p.Value,
			}
		}
		for s.b.TestEpError(ep, err) {
			ep, err = s.b.EndpointFor(ctx, s.uuid)
			if err != nil {
				continue
			}
			err = ep.InsertGeneric(ctx, s.uuid, pbraws, p)
		}
		if err != nil {
			return err
		}
		vals = vals[end:]
	}
	return nil

}

// InsertF will call the given time and val functions to get each value of the
// insertion. It is similar to InsertTV but may require less allocations if
// your data is already in a different data structure. If the
// size is larger than appropriate, this function will automatically chunk the inserts.
// As a consequence, the insert is not necessarily atomic, but can be used with
// very large size.
func (s *Stream) InsertF(ctx context.Context, length int, time func(int) int64, val func(int) float64, p *InsertParams) error {
	var ep *Endpoint
	var err error
	batchsize := 50000
	fidx := 0
	for fidx < length {
		err = forceEp
		tsize := length - fidx
		if tsize > batchsize {
			tsize = batchsize
		}
		//TODO pool or reuse
		pbraws := make([]*pb.RawPoint, tsize)
		for i := 0; i < tsize; i++ {
			pbraws[i] = &pb.RawPoint{
				Time:  time(fidx),
				Value: val(fidx),
			}
			fidx++
		}
		for s.b.TestEpError(ep, err) {
			ep, err = s.b.EndpointFor(ctx, s.uuid)
			if err != nil {
				continue
			}
			err = ep.Insert(ctx, s.uuid, pbraws, p)
		}
		if err != nil {
			return err
		}
	}
	return nil
}
