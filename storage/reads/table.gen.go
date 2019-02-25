// Generated by tmpl
// https://github.com/benbjohnson/tmpl
//
// DO NOT EDIT!
// Source: table.gen.go.tmpl

package reads

import (
	"sync"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/arrow"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/memory"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb/cursors"
	"github.com/pkg/errors"
)

//
// *********** Float ***********
//

type floatTable struct {
	table
	valBuf []float64
	mu     sync.Mutex
	cur    cursors.FloatArrayCursor
}

func newFloatTable(
	done chan struct{},
	cur cursors.FloatArrayCursor,
	bounds execute.Bounds,
	key flux.GroupKey,
	cols []flux.ColMeta,
	tags models.Tags,
	defs [][]byte,
) *floatTable {
	t := &floatTable{
		table: newTable(done, bounds, key, cols, defs),
		cur:   cur,
	}
	t.readTags(tags)
	t.advance()

	return t
}

func (t *floatTable) Close() {
	t.mu.Lock()
	if t.cur != nil {
		t.cur.Close()
		t.cur = nil
	}
	t.mu.Unlock()
}

func (t *floatTable) Statistics() cursors.CursorStats {
	t.mu.Lock()
	defer t.mu.Unlock()
	cur := t.cur
	if cur == nil {
		return cursors.CursorStats{}
	}
	cs := cur.Stats()
	return cursors.CursorStats{
		ScannedValues: cs.ScannedValues,
		ScannedBytes:  cs.ScannedBytes,
	}
}

func (t *floatTable) Do(f func(flux.ColReader) error) error {
	t.mu.Lock()
	defer func() {
		t.closeDone()
		t.mu.Unlock()
	}()

	if !t.Empty() {
		t.err = f(t)
		for !t.isCancelled() && t.err == nil && t.advance() {
			t.err = f(t)
		}
	}

	return t.err
}

func (t *floatTable) advance() bool {
	a := t.cur.Next()
	t.l = a.Len()
	if t.l == 0 {
		return false
	}

	if cap(t.timeBuf) < t.l {
		t.timeBuf = make([]int64, t.l)
	} else {
		t.timeBuf = t.timeBuf[:t.l]
	}
	copy(t.timeBuf, a.Timestamps)

	if cap(t.valBuf) < t.l {
		t.valBuf = make([]float64, t.l)
	} else {
		t.valBuf = t.valBuf[:t.l]
	}
	copy(t.valBuf, a.Values)

	t.colBufs[timeColIdx] = arrow.NewInt(t.timeBuf, &memory.Allocator{})
	t.colBufs[valueColIdx] = t.toArrowBuffer(t.valBuf)
	t.appendTags()
	t.appendBounds()
	return true
}

// group table

type floatGroupTable struct {
	table
	valBuf []float64
	mu     sync.Mutex
	gc     GroupCursor
	cur    cursors.FloatArrayCursor
}

func newFloatGroupTable(
	done chan struct{},
	gc GroupCursor,
	cur cursors.FloatArrayCursor,
	bounds execute.Bounds,
	key flux.GroupKey,
	cols []flux.ColMeta,
	tags models.Tags,
	defs [][]byte,
) *floatGroupTable {
	t := &floatGroupTable{
		table: newTable(done, bounds, key, cols, defs),
		gc:    gc,
		cur:   cur,
	}
	t.readTags(tags)
	t.advance()

	return t
}

func (t *floatGroupTable) Close() {
	t.mu.Lock()
	if t.cur != nil {
		t.cur.Close()
		t.cur = nil
	}
	if t.gc != nil {
		t.gc.Close()
		t.gc = nil
	}
	t.mu.Unlock()
}

func (t *floatGroupTable) Do(f func(flux.ColReader) error) error {
	t.mu.Lock()
	defer func() {
		t.closeDone()
		t.mu.Unlock()
	}()

	if !t.Empty() {
		t.err = f(t)
		for !t.isCancelled() && t.err == nil && t.advance() {
			t.err = f(t)
		}
	}

	return t.err
}

func (t *floatGroupTable) advance() bool {
RETRY:
	a := t.cur.Next()
	t.l = a.Len()
	if t.l == 0 {
		if t.advanceCursor() {
			goto RETRY
		}

		return false
	}

	if cap(t.timeBuf) < t.l {
		t.timeBuf = make([]int64, t.l)
	} else {
		t.timeBuf = t.timeBuf[:t.l]
	}
	copy(t.timeBuf, a.Timestamps)

	if cap(t.valBuf) < t.l {
		t.valBuf = make([]float64, t.l)
	} else {
		t.valBuf = t.valBuf[:t.l]
	}
	copy(t.valBuf, a.Values)

	t.colBufs[timeColIdx] = arrow.NewInt(t.timeBuf, &memory.Allocator{})
	t.colBufs[valueColIdx] = t.toArrowBuffer(t.valBuf)
	t.appendTags()
	t.appendBounds()
	return true
}

func (t *floatGroupTable) advanceCursor() bool {
	t.cur.Close()
	t.cur = nil
	for t.gc.Next() {
		cur := t.gc.Cursor()
		if cur == nil {
			continue
		}

		if typedCur, ok := cur.(cursors.FloatArrayCursor); !ok {
			// TODO(sgc): error or skip?
			cur.Close()
			t.err = errors.Errorf("expected float cursor type, got %T", cur)
			return false
		} else {
			t.readTags(t.gc.Tags())
			t.cur = typedCur
			return true
		}
	}
	return false
}

func (t *floatGroupTable) Statistics() cursors.CursorStats {
	if t.cur == nil {
		return cursors.CursorStats{}
	}
	cs := t.cur.Stats()
	return cursors.CursorStats{
		ScannedValues: cs.ScannedValues,
		ScannedBytes:  cs.ScannedBytes,
	}
}

//
// *********** Integer ***********
//

type integerTable struct {
	table
	valBuf []int64
	mu     sync.Mutex
	cur    cursors.IntegerArrayCursor
}

func newIntegerTable(
	done chan struct{},
	cur cursors.IntegerArrayCursor,
	bounds execute.Bounds,
	key flux.GroupKey,
	cols []flux.ColMeta,
	tags models.Tags,
	defs [][]byte,
) *integerTable {
	t := &integerTable{
		table: newTable(done, bounds, key, cols, defs),
		cur:   cur,
	}
	t.readTags(tags)
	t.advance()

	return t
}

func (t *integerTable) Close() {
	t.mu.Lock()
	if t.cur != nil {
		t.cur.Close()
		t.cur = nil
	}
	t.mu.Unlock()
}

func (t *integerTable) Statistics() cursors.CursorStats {
	t.mu.Lock()
	defer t.mu.Unlock()
	cur := t.cur
	if cur == nil {
		return cursors.CursorStats{}
	}
	cs := cur.Stats()
	return cursors.CursorStats{
		ScannedValues: cs.ScannedValues,
		ScannedBytes:  cs.ScannedBytes,
	}
}

func (t *integerTable) Do(f func(flux.ColReader) error) error {
	t.mu.Lock()
	defer func() {
		t.closeDone()
		t.mu.Unlock()
	}()

	if !t.Empty() {
		t.err = f(t)
		for !t.isCancelled() && t.err == nil && t.advance() {
			t.err = f(t)
		}
	}

	return t.err
}

func (t *integerTable) advance() bool {
	a := t.cur.Next()
	t.l = a.Len()
	if t.l == 0 {
		return false
	}

	if cap(t.timeBuf) < t.l {
		t.timeBuf = make([]int64, t.l)
	} else {
		t.timeBuf = t.timeBuf[:t.l]
	}
	copy(t.timeBuf, a.Timestamps)

	if cap(t.valBuf) < t.l {
		t.valBuf = make([]int64, t.l)
	} else {
		t.valBuf = t.valBuf[:t.l]
	}
	copy(t.valBuf, a.Values)

	t.colBufs[timeColIdx] = arrow.NewInt(t.timeBuf, &memory.Allocator{})
	t.colBufs[valueColIdx] = t.toArrowBuffer(t.valBuf)
	t.appendTags()
	t.appendBounds()
	return true
}

// group table

type integerGroupTable struct {
	table
	valBuf []int64
	mu     sync.Mutex
	gc     GroupCursor
	cur    cursors.IntegerArrayCursor
}

func newIntegerGroupTable(
	done chan struct{},
	gc GroupCursor,
	cur cursors.IntegerArrayCursor,
	bounds execute.Bounds,
	key flux.GroupKey,
	cols []flux.ColMeta,
	tags models.Tags,
	defs [][]byte,
) *integerGroupTable {
	t := &integerGroupTable{
		table: newTable(done, bounds, key, cols, defs),
		gc:    gc,
		cur:   cur,
	}
	t.readTags(tags)
	t.advance()

	return t
}

func (t *integerGroupTable) Close() {
	t.mu.Lock()
	if t.cur != nil {
		t.cur.Close()
		t.cur = nil
	}
	if t.gc != nil {
		t.gc.Close()
		t.gc = nil
	}
	t.mu.Unlock()
}

func (t *integerGroupTable) Do(f func(flux.ColReader) error) error {
	t.mu.Lock()
	defer func() {
		t.closeDone()
		t.mu.Unlock()
	}()

	if !t.Empty() {
		t.err = f(t)
		for !t.isCancelled() && t.err == nil && t.advance() {
			t.err = f(t)
		}
	}

	return t.err
}

func (t *integerGroupTable) advance() bool {
RETRY:
	a := t.cur.Next()
	t.l = a.Len()
	if t.l == 0 {
		if t.advanceCursor() {
			goto RETRY
		}

		return false
	}

	if cap(t.timeBuf) < t.l {
		t.timeBuf = make([]int64, t.l)
	} else {
		t.timeBuf = t.timeBuf[:t.l]
	}
	copy(t.timeBuf, a.Timestamps)

	if cap(t.valBuf) < t.l {
		t.valBuf = make([]int64, t.l)
	} else {
		t.valBuf = t.valBuf[:t.l]
	}
	copy(t.valBuf, a.Values)

	t.colBufs[timeColIdx] = arrow.NewInt(t.timeBuf, &memory.Allocator{})
	t.colBufs[valueColIdx] = t.toArrowBuffer(t.valBuf)
	t.appendTags()
	t.appendBounds()
	return true
}

func (t *integerGroupTable) advanceCursor() bool {
	t.cur.Close()
	t.cur = nil
	for t.gc.Next() {
		cur := t.gc.Cursor()
		if cur == nil {
			continue
		}

		if typedCur, ok := cur.(cursors.IntegerArrayCursor); !ok {
			// TODO(sgc): error or skip?
			cur.Close()
			t.err = errors.Errorf("expected integer cursor type, got %T", cur)
			return false
		} else {
			t.readTags(t.gc.Tags())
			t.cur = typedCur
			return true
		}
	}
	return false
}

func (t *integerGroupTable) Statistics() cursors.CursorStats {
	if t.cur == nil {
		return cursors.CursorStats{}
	}
	cs := t.cur.Stats()
	return cursors.CursorStats{
		ScannedValues: cs.ScannedValues,
		ScannedBytes:  cs.ScannedBytes,
	}
}

//
// *********** Unsigned ***********
//

type unsignedTable struct {
	table
	valBuf []uint64
	mu     sync.Mutex
	cur    cursors.UnsignedArrayCursor
}

func newUnsignedTable(
	done chan struct{},
	cur cursors.UnsignedArrayCursor,
	bounds execute.Bounds,
	key flux.GroupKey,
	cols []flux.ColMeta,
	tags models.Tags,
	defs [][]byte,
) *unsignedTable {
	t := &unsignedTable{
		table: newTable(done, bounds, key, cols, defs),
		cur:   cur,
	}
	t.readTags(tags)
	t.advance()

	return t
}

func (t *unsignedTable) Close() {
	t.mu.Lock()
	if t.cur != nil {
		t.cur.Close()
		t.cur = nil
	}
	t.mu.Unlock()
}

func (t *unsignedTable) Statistics() cursors.CursorStats {
	t.mu.Lock()
	defer t.mu.Unlock()
	cur := t.cur
	if cur == nil {
		return cursors.CursorStats{}
	}
	cs := cur.Stats()
	return cursors.CursorStats{
		ScannedValues: cs.ScannedValues,
		ScannedBytes:  cs.ScannedBytes,
	}
}

func (t *unsignedTable) Do(f func(flux.ColReader) error) error {
	t.mu.Lock()
	defer func() {
		t.closeDone()
		t.mu.Unlock()
	}()

	if !t.Empty() {
		t.err = f(t)
		for !t.isCancelled() && t.err == nil && t.advance() {
			t.err = f(t)
		}
	}

	return t.err
}

func (t *unsignedTable) advance() bool {
	a := t.cur.Next()
	t.l = a.Len()
	if t.l == 0 {
		return false
	}

	if cap(t.timeBuf) < t.l {
		t.timeBuf = make([]int64, t.l)
	} else {
		t.timeBuf = t.timeBuf[:t.l]
	}
	copy(t.timeBuf, a.Timestamps)

	if cap(t.valBuf) < t.l {
		t.valBuf = make([]uint64, t.l)
	} else {
		t.valBuf = t.valBuf[:t.l]
	}
	copy(t.valBuf, a.Values)

	t.colBufs[timeColIdx] = arrow.NewInt(t.timeBuf, &memory.Allocator{})
	t.colBufs[valueColIdx] = t.toArrowBuffer(t.valBuf)
	t.appendTags()
	t.appendBounds()
	return true
}

// group table

type unsignedGroupTable struct {
	table
	valBuf []uint64
	mu     sync.Mutex
	gc     GroupCursor
	cur    cursors.UnsignedArrayCursor
}

func newUnsignedGroupTable(
	done chan struct{},
	gc GroupCursor,
	cur cursors.UnsignedArrayCursor,
	bounds execute.Bounds,
	key flux.GroupKey,
	cols []flux.ColMeta,
	tags models.Tags,
	defs [][]byte,
) *unsignedGroupTable {
	t := &unsignedGroupTable{
		table: newTable(done, bounds, key, cols, defs),
		gc:    gc,
		cur:   cur,
	}
	t.readTags(tags)
	t.advance()

	return t
}

func (t *unsignedGroupTable) Close() {
	t.mu.Lock()
	if t.cur != nil {
		t.cur.Close()
		t.cur = nil
	}
	if t.gc != nil {
		t.gc.Close()
		t.gc = nil
	}
	t.mu.Unlock()
}

func (t *unsignedGroupTable) Do(f func(flux.ColReader) error) error {
	t.mu.Lock()
	defer func() {
		t.closeDone()
		t.mu.Unlock()
	}()

	if !t.Empty() {
		t.err = f(t)
		for !t.isCancelled() && t.err == nil && t.advance() {
			t.err = f(t)
		}
	}

	return t.err
}

func (t *unsignedGroupTable) advance() bool {
RETRY:
	a := t.cur.Next()
	t.l = a.Len()
	if t.l == 0 {
		if t.advanceCursor() {
			goto RETRY
		}

		return false
	}

	if cap(t.timeBuf) < t.l {
		t.timeBuf = make([]int64, t.l)
	} else {
		t.timeBuf = t.timeBuf[:t.l]
	}
	copy(t.timeBuf, a.Timestamps)

	if cap(t.valBuf) < t.l {
		t.valBuf = make([]uint64, t.l)
	} else {
		t.valBuf = t.valBuf[:t.l]
	}
	copy(t.valBuf, a.Values)

	t.colBufs[timeColIdx] = arrow.NewInt(t.timeBuf, &memory.Allocator{})
	t.colBufs[valueColIdx] = t.toArrowBuffer(t.valBuf)
	t.appendTags()
	t.appendBounds()
	return true
}

func (t *unsignedGroupTable) advanceCursor() bool {
	t.cur.Close()
	t.cur = nil
	for t.gc.Next() {
		cur := t.gc.Cursor()
		if cur == nil {
			continue
		}

		if typedCur, ok := cur.(cursors.UnsignedArrayCursor); !ok {
			// TODO(sgc): error or skip?
			cur.Close()
			t.err = errors.Errorf("expected unsigned cursor type, got %T", cur)
			return false
		} else {
			t.readTags(t.gc.Tags())
			t.cur = typedCur
			return true
		}
	}
	return false
}

func (t *unsignedGroupTable) Statistics() cursors.CursorStats {
	if t.cur == nil {
		return cursors.CursorStats{}
	}
	cs := t.cur.Stats()
	return cursors.CursorStats{
		ScannedValues: cs.ScannedValues,
		ScannedBytes:  cs.ScannedBytes,
	}
}

//
// *********** String ***********
//

type stringTable struct {
	table
	valBuf []string
	mu     sync.Mutex
	cur    cursors.StringArrayCursor
}

func newStringTable(
	done chan struct{},
	cur cursors.StringArrayCursor,
	bounds execute.Bounds,
	key flux.GroupKey,
	cols []flux.ColMeta,
	tags models.Tags,
	defs [][]byte,
) *stringTable {
	t := &stringTable{
		table: newTable(done, bounds, key, cols, defs),
		cur:   cur,
	}
	t.readTags(tags)
	t.advance()

	return t
}

func (t *stringTable) Close() {
	t.mu.Lock()
	if t.cur != nil {
		t.cur.Close()
		t.cur = nil
	}
	t.mu.Unlock()
}

func (t *stringTable) Statistics() cursors.CursorStats {
	t.mu.Lock()
	defer t.mu.Unlock()
	cur := t.cur
	if cur == nil {
		return cursors.CursorStats{}
	}
	cs := cur.Stats()
	return cursors.CursorStats{
		ScannedValues: cs.ScannedValues,
		ScannedBytes:  cs.ScannedBytes,
	}
}

func (t *stringTable) Do(f func(flux.ColReader) error) error {
	t.mu.Lock()
	defer func() {
		t.closeDone()
		t.mu.Unlock()
	}()

	if !t.Empty() {
		t.err = f(t)
		for !t.isCancelled() && t.err == nil && t.advance() {
			t.err = f(t)
		}
	}

	return t.err
}

func (t *stringTable) advance() bool {
	a := t.cur.Next()
	t.l = a.Len()
	if t.l == 0 {
		return false
	}

	if cap(t.timeBuf) < t.l {
		t.timeBuf = make([]int64, t.l)
	} else {
		t.timeBuf = t.timeBuf[:t.l]
	}
	copy(t.timeBuf, a.Timestamps)

	if cap(t.valBuf) < t.l {
		t.valBuf = make([]string, t.l)
	} else {
		t.valBuf = t.valBuf[:t.l]
	}
	copy(t.valBuf, a.Values)

	t.colBufs[timeColIdx] = arrow.NewInt(t.timeBuf, &memory.Allocator{})
	t.colBufs[valueColIdx] = t.toArrowBuffer(t.valBuf)
	t.appendTags()
	t.appendBounds()
	return true
}

// group table

type stringGroupTable struct {
	table
	valBuf []string
	mu     sync.Mutex
	gc     GroupCursor
	cur    cursors.StringArrayCursor
}

func newStringGroupTable(
	done chan struct{},
	gc GroupCursor,
	cur cursors.StringArrayCursor,
	bounds execute.Bounds,
	key flux.GroupKey,
	cols []flux.ColMeta,
	tags models.Tags,
	defs [][]byte,
) *stringGroupTable {
	t := &stringGroupTable{
		table: newTable(done, bounds, key, cols, defs),
		gc:    gc,
		cur:   cur,
	}
	t.readTags(tags)
	t.advance()

	return t
}

func (t *stringGroupTable) Close() {
	t.mu.Lock()
	if t.cur != nil {
		t.cur.Close()
		t.cur = nil
	}
	if t.gc != nil {
		t.gc.Close()
		t.gc = nil
	}
	t.mu.Unlock()
}

func (t *stringGroupTable) Do(f func(flux.ColReader) error) error {
	t.mu.Lock()
	defer func() {
		t.closeDone()
		t.mu.Unlock()
	}()

	if !t.Empty() {
		t.err = f(t)
		for !t.isCancelled() && t.err == nil && t.advance() {
			t.err = f(t)
		}
	}

	return t.err
}

func (t *stringGroupTable) advance() bool {
RETRY:
	a := t.cur.Next()
	t.l = a.Len()
	if t.l == 0 {
		if t.advanceCursor() {
			goto RETRY
		}

		return false
	}

	if cap(t.timeBuf) < t.l {
		t.timeBuf = make([]int64, t.l)
	} else {
		t.timeBuf = t.timeBuf[:t.l]
	}
	copy(t.timeBuf, a.Timestamps)

	if cap(t.valBuf) < t.l {
		t.valBuf = make([]string, t.l)
	} else {
		t.valBuf = t.valBuf[:t.l]
	}
	copy(t.valBuf, a.Values)

	t.colBufs[timeColIdx] = arrow.NewInt(t.timeBuf, &memory.Allocator{})
	t.colBufs[valueColIdx] = t.toArrowBuffer(t.valBuf)
	t.appendTags()
	t.appendBounds()
	return true
}

func (t *stringGroupTable) advanceCursor() bool {
	t.cur.Close()
	t.cur = nil
	for t.gc.Next() {
		cur := t.gc.Cursor()
		if cur == nil {
			continue
		}

		if typedCur, ok := cur.(cursors.StringArrayCursor); !ok {
			// TODO(sgc): error or skip?
			cur.Close()
			t.err = errors.Errorf("expected string cursor type, got %T", cur)
			return false
		} else {
			t.readTags(t.gc.Tags())
			t.cur = typedCur
			return true
		}
	}
	return false
}

func (t *stringGroupTable) Statistics() cursors.CursorStats {
	if t.cur == nil {
		return cursors.CursorStats{}
	}
	cs := t.cur.Stats()
	return cursors.CursorStats{
		ScannedValues: cs.ScannedValues,
		ScannedBytes:  cs.ScannedBytes,
	}
}

//
// *********** Boolean ***********
//

type booleanTable struct {
	table
	valBuf []bool
	mu     sync.Mutex
	cur    cursors.BooleanArrayCursor
}

func newBooleanTable(
	done chan struct{},
	cur cursors.BooleanArrayCursor,
	bounds execute.Bounds,
	key flux.GroupKey,
	cols []flux.ColMeta,
	tags models.Tags,
	defs [][]byte,
) *booleanTable {
	t := &booleanTable{
		table: newTable(done, bounds, key, cols, defs),
		cur:   cur,
	}
	t.readTags(tags)
	t.advance()

	return t
}

func (t *booleanTable) Close() {
	t.mu.Lock()
	if t.cur != nil {
		t.cur.Close()
		t.cur = nil
	}
	t.mu.Unlock()
}

func (t *booleanTable) Statistics() cursors.CursorStats {
	t.mu.Lock()
	defer t.mu.Unlock()
	cur := t.cur
	if cur == nil {
		return cursors.CursorStats{}
	}
	cs := cur.Stats()
	return cursors.CursorStats{
		ScannedValues: cs.ScannedValues,
		ScannedBytes:  cs.ScannedBytes,
	}
}

func (t *booleanTable) Do(f func(flux.ColReader) error) error {
	t.mu.Lock()
	defer func() {
		t.closeDone()
		t.mu.Unlock()
	}()

	if !t.Empty() {
		t.err = f(t)
		for !t.isCancelled() && t.err == nil && t.advance() {
			t.err = f(t)
		}
	}

	return t.err
}

func (t *booleanTable) advance() bool {
	a := t.cur.Next()
	t.l = a.Len()
	if t.l == 0 {
		return false
	}

	if cap(t.timeBuf) < t.l {
		t.timeBuf = make([]int64, t.l)
	} else {
		t.timeBuf = t.timeBuf[:t.l]
	}
	copy(t.timeBuf, a.Timestamps)

	if cap(t.valBuf) < t.l {
		t.valBuf = make([]bool, t.l)
	} else {
		t.valBuf = t.valBuf[:t.l]
	}
	copy(t.valBuf, a.Values)

	t.colBufs[timeColIdx] = arrow.NewInt(t.timeBuf, &memory.Allocator{})
	t.colBufs[valueColIdx] = t.toArrowBuffer(t.valBuf)
	t.appendTags()
	t.appendBounds()
	return true
}

// group table

type booleanGroupTable struct {
	table
	valBuf []bool
	mu     sync.Mutex
	gc     GroupCursor
	cur    cursors.BooleanArrayCursor
}

func newBooleanGroupTable(
	done chan struct{},
	gc GroupCursor,
	cur cursors.BooleanArrayCursor,
	bounds execute.Bounds,
	key flux.GroupKey,
	cols []flux.ColMeta,
	tags models.Tags,
	defs [][]byte,
) *booleanGroupTable {
	t := &booleanGroupTable{
		table: newTable(done, bounds, key, cols, defs),
		gc:    gc,
		cur:   cur,
	}
	t.readTags(tags)
	t.advance()

	return t
}

func (t *booleanGroupTable) Close() {
	t.mu.Lock()
	if t.cur != nil {
		t.cur.Close()
		t.cur = nil
	}
	if t.gc != nil {
		t.gc.Close()
		t.gc = nil
	}
	t.mu.Unlock()
}

func (t *booleanGroupTable) Do(f func(flux.ColReader) error) error {
	t.mu.Lock()
	defer func() {
		t.closeDone()
		t.mu.Unlock()
	}()

	if !t.Empty() {
		t.err = f(t)
		for !t.isCancelled() && t.err == nil && t.advance() {
			t.err = f(t)
		}
	}

	return t.err
}

func (t *booleanGroupTable) advance() bool {
RETRY:
	a := t.cur.Next()
	t.l = a.Len()
	if t.l == 0 {
		if t.advanceCursor() {
			goto RETRY
		}

		return false
	}

	if cap(t.timeBuf) < t.l {
		t.timeBuf = make([]int64, t.l)
	} else {
		t.timeBuf = t.timeBuf[:t.l]
	}
	copy(t.timeBuf, a.Timestamps)

	if cap(t.valBuf) < t.l {
		t.valBuf = make([]bool, t.l)
	} else {
		t.valBuf = t.valBuf[:t.l]
	}
	copy(t.valBuf, a.Values)

	t.colBufs[timeColIdx] = arrow.NewInt(t.timeBuf, &memory.Allocator{})
	t.colBufs[valueColIdx] = t.toArrowBuffer(t.valBuf)
	t.appendTags()
	t.appendBounds()
	return true
}

func (t *booleanGroupTable) advanceCursor() bool {
	t.cur.Close()
	t.cur = nil
	for t.gc.Next() {
		cur := t.gc.Cursor()
		if cur == nil {
			continue
		}

		if typedCur, ok := cur.(cursors.BooleanArrayCursor); !ok {
			// TODO(sgc): error or skip?
			cur.Close()
			t.err = errors.Errorf("expected boolean cursor type, got %T", cur)
			return false
		} else {
			t.readTags(t.gc.Tags())
			t.cur = typedCur
			return true
		}
	}
	return false
}

func (t *booleanGroupTable) Statistics() cursors.CursorStats {
	if t.cur == nil {
		return cursors.CursorStats{}
	}
	cs := t.cur.Stats()
	return cursors.CursorStats{
		ScannedValues: cs.ScannedValues,
		ScannedBytes:  cs.ScannedBytes,
	}
}
