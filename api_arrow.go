//go:build arrow

package btrdb

import (
	"context"
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
		for s.b.TestEpError(ep, err) {
			ep, err = s.b.EndpointFor(ctx, s.uuid)
			if err != nil {
				continue
			}
			err = ep.InsertTV(ctx, s.uuid, thisBatchT, thisBatchV, p)
		}
		if err != nil {
			return err
		}
		times = times[end:]
		values = values[end:]
	}
	return nil
}

func min(x, y int) int {
	if x < y {
		return x
	} else {
		return y
	}
}

func (s *Stream) InsertGeneric(ctx context.Context, points []RawPoint, p *InsertParams) error {
	var ep *Endpoint
	var err error
	batchsize := 50000
	for len(points) > 0 {
		err = forceEp
		end := len(points)
		if end > batchsize {
			end = batchsize
		}
		thisBatch := points[:end]
		for s.b.TestEpError(ep, err) {
			ep, err = s.b.EndpointFor(ctx, s.uuid)
			if err != nil {
				continue
			}
			err = ep.InsertPoints(ctx, s.uuid, thisBatch, p)
		}
		if err != nil {
			return err
		}
		points = points[end:]
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
	times := make([]int64, 0, min(batchsize, length))
	values := make([]float64, 0, min(batchsize, length))
	fidx := 0
	for fidx < length {
		err = forceEp
		tsize := length - fidx
		if tsize > batchsize {
			tsize = batchsize
		}
		times = times[:0]
		values = values[:0]
		for i := 0; i < tsize; i++ {
			times = append(times, time(fidx))
			values = append(values, val(fidx))
			fidx++
		}
		for s.b.TestEpError(ep, err) {
			ep, err = s.b.EndpointFor(ctx, s.uuid)
			if err != nil {
				continue
			}
			err = ep.InsertTV(ctx, s.uuid, times, values, p)
		}
		if err != nil {
			return err
		}
	}
	return nil
}
