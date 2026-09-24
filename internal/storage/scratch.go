package storage

import (
	"bufio"
	"bytes"
	"container/heap"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
)

const (
	DefaultExactQueryMemoryBytes = 64 << 20
	DefaultQueryConcurrency      = 8
	// An aggregation accumulator can coexist with one nested RunQuery accumulator.
	exactAccumulatorDepth   = 2
	pathRecordBytes         = 8
	maxAccumulatedPathBytes = 64 << 10
	minimumAccumulatorBytes = maxAccumulatedPathBytes + pathRecordBytes
	exactQueryMergeFanIn    = 4
	mergeReaderBufferBytes  = 4 << 10
	mergeWriterBufferBytes  = 4 << 10
)

func mergeBufferReserveBytes(fanIn int) int {
	return fanIn*(mergeReaderBufferBytes+2*maxAccumulatedPathBytes) + mergeWriterBufferBytes
}

type exactQueryLimits struct {
	accumulatorBytes int
	maxAccumulators  int
}

func deriveExactQueryLimits(memoryBytes int64, concurrency int) (exactQueryLimits, error) {
	if concurrency <= 0 {
		return exactQueryLimits{}, fmt.Errorf("query concurrency must be positive")
	}
	maxAccumulators := concurrency * exactAccumulatorDepth
	if maxAccumulators/exactAccumulatorDepth != concurrency {
		return exactQueryLimits{}, fmt.Errorf("query concurrency is too large")
	}
	mergeReserve := mergeBufferReserveBytes(exactQueryMergeFanIn)
	workspaceBytes := memoryBytes / int64(maxAccumulators)
	accumulatorBytes := workspaceBytes - int64(mergeReserve)
	if accumulatorBytes < minimumAccumulatorBytes {
		return exactQueryLimits{}, fmt.Errorf(
			"exact-query memory %d bytes is too small for concurrency %d: need at least %d bytes",
			memoryBytes, concurrency, int64(minimumAccumulatorBytes+mergeReserve)*int64(maxAccumulators),
		)
	}
	if accumulatorBytes > int64(^uint32(0)) || accumulatorBytes > int64(^uint(0)>>1) {
		return exactQueryLimits{}, fmt.Errorf("exact-query accumulator size is too large")
	}
	return exactQueryLimits{accumulatorBytes: int(accumulatorBytes), maxAccumulators: maxAccumulators}, nil
}

// PathAccumulator deduplicates paths with bounded memory and temporary sorted runs.
type PathAccumulator struct {
	ctx        context.Context
	work       *QueryWork
	dir        string
	buffer     []byte
	dataBytes  int
	records    int
	runs       []string
	runLevels  []int
	nextRunID  uint64
	mergeFanIn int
	release    func()
}

func (s *Store) NewPathAccumulator(ctx context.Context) (*PathAccumulator, error) {
	slots := s.exactSlots
	select {
	case slots <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	acc, err := newPathAccumulator(ctx, s.scratchDir, s.exactAccumulatorBytes)
	if err != nil {
		<-slots
		return nil, err
	}
	acc.release = func() { <-slots }
	return acc, nil
}

func newPathAccumulator(ctx context.Context, root string, bufferLimit int) (*PathAccumulator, error) {
	return newPathAccumulatorWithFanIn(ctx, root, bufferLimit, exactQueryMergeFanIn)
}

func newPathAccumulatorWithFanIn(ctx context.Context, root string, bufferLimit, mergeFanIn int) (*PathAccumulator, error) {
	if mergeFanIn < 2 {
		return nil, fmt.Errorf("merge fan-in must be at least 2")
	}
	dir, err := os.MkdirTemp(root, "query-")
	if err != nil {
		return nil, err
	}
	return &PathAccumulator{ctx: ctx, work: QueryWorkFromContext(ctx), dir: dir, buffer: make([]byte, bufferLimit), mergeFanIn: mergeFanIn}, nil
}

func (a *PathAccumulator) Add(path string) error {
	if len(path) > maxAccumulatedPathBytes {
		return fmt.Errorf("sort record exceeds %d bytes", maxAccumulatedPathBytes)
	}
	if err := a.work.Checkpoint(a.ctx); err != nil {
		return err
	}
	if len(path)+pathRecordBytes > len(a.buffer) {
		if err := a.flush(); err != nil {
			return err
		}
		run, err := a.newRunPath()
		if err != nil {
			return err
		}
		if err := writePathRun(a.ctx, run, []string{path}); err != nil {
			return err
		}
		return a.addRun(run)
	}
	if a.availableBytes() < len(path)+pathRecordBytes {
		if err := a.flush(); err != nil {
			return err
		}
	}
	start := a.dataBytes
	copy(a.buffer[start:], path)
	a.dataBytes += len(path)
	a.setRecord(a.records, uint32(start), uint32(len(path)))
	a.records++
	return nil
}

func (a *PathAccumulator) Count() (int64, error) {
	var count int64
	err := a.ForEach(func(string) error { count++; return nil })
	return count, err
}

func (a *PathAccumulator) ForEach(visit func(string) error) error {
	if len(a.runs) == 0 {
		return a.visitBuffer(visit)
	}
	if err := a.flush(); err != nil {
		return err
	}
	if err := a.collapseRuns(); err != nil {
		return err
	}
	return mergePathRuns(a.ctx, a.runs, visit)
}

func (a *PathAccumulator) Close() error {
	err := os.RemoveAll(a.dir)
	if a.release != nil {
		a.release()
		a.release = nil
	}
	return err
}

func (a *PathAccumulator) flush() error {
	if a.records == 0 {
		return nil
	}
	path, err := a.newRunPath()
	if err != nil {
		return err
	}
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	writer := bufio.NewWriterSize(queryWorkWriter{file, a.work}, mergeWriterBufferBytes)
	err = a.visitBuffer(func(path string) error { return writePath(writer, path) })
	if flushErr := writer.Flush(); err == nil {
		err = flushErr
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	a.dataBytes = 0
	a.records = 0
	return a.addRun(path)
}

func (a *PathAccumulator) newRunPath() (string, error) {
	if a.nextRunID == ^uint64(0) {
		return "", fmt.Errorf("scratch run counter exhausted")
	}
	path := filepath.Join(a.dir, "run-"+strconv.FormatUint(a.nextRunID, 10))
	a.nextRunID++
	return path, nil
}

// addRun merges equal-sized generations as they arrive. Each record participates
// in at most one merge per level, and live run metadata grows logarithmically.
func (a *PathAccumulator) addRun(path string) error {
	a.runs = append(a.runs, path)
	a.runLevels = append(a.runLevels, 0)
	for len(a.runs) >= a.mergeFanIn {
		start := len(a.runs) - a.mergeFanIn
		level := a.runLevels[start]
		if level != a.runLevels[len(a.runLevels)-1] {
			break
		}
		if err := a.mergeRuns(start); err != nil {
			return err
		}
		a.runLevels = append(a.runLevels[:start], level+1)
	}
	return nil
}

func (a *PathAccumulator) availableBytes() int {
	return len(a.buffer) - a.dataBytes - a.records*pathRecordBytes
}

func (a *PathAccumulator) record(index int) (uint32, uint32) {
	position := len(a.buffer) - (index+1)*pathRecordBytes
	return binary.LittleEndian.Uint32(a.buffer[position:]), binary.LittleEndian.Uint32(a.buffer[position+4:])
}

func (a *PathAccumulator) setRecord(index int, start, size uint32) {
	position := len(a.buffer) - (index+1)*pathRecordBytes
	binary.LittleEndian.PutUint32(a.buffer[position:], start)
	binary.LittleEndian.PutUint32(a.buffer[position+4:], size)
}

func (a *PathAccumulator) recordPath(index int) []byte {
	start, size := a.record(index)
	return a.buffer[int(start):int(start+size)]
}

type pathRecordSorter struct {
	accumulator *PathAccumulator
	err         *error
}

func (s pathRecordSorter) Len() int { return s.accumulator.records }

func (s pathRecordSorter) Less(i, j int) bool {
	if *s.err == nil {
		*s.err = s.accumulator.work.Checkpoint(s.accumulator.ctx)
	}
	if *s.err != nil {
		return false
	}
	s.accumulator.work.Charge(WorkComparisons, 1)
	return bytes.Compare(s.accumulator.recordPath(i), s.accumulator.recordPath(j)) < 0
}

func (s pathRecordSorter) Swap(i, j int) {
	left := len(s.accumulator.buffer) - (i+1)*pathRecordBytes
	right := len(s.accumulator.buffer) - (j+1)*pathRecordBytes
	var record [pathRecordBytes]byte
	copy(record[:], s.accumulator.buffer[left:left+pathRecordBytes])
	copy(s.accumulator.buffer[left:left+pathRecordBytes], s.accumulator.buffer[right:right+pathRecordBytes])
	copy(s.accumulator.buffer[right:right+pathRecordBytes], record[:])
}

func (a *PathAccumulator) visitBuffer(visit func(string) error) error {
	var sortErr error
	sort.Sort(pathRecordSorter{accumulator: a, err: &sortErr})
	if sortErr != nil {
		return sortErr
	}
	var previous string
	for index := 0; index < a.records; index++ {
		if err := a.work.Checkpoint(a.ctx); err != nil {
			return err
		}
		path := string(a.recordPath(index))
		if index > 0 && path == previous {
			continue
		}
		if err := visit(path); err != nil {
			return err
		}
		previous = path
	}
	return nil
}

func (a *PathAccumulator) collapseRuns() error {
	for len(a.runs) > a.mergeFanIn {
		start := len(a.runs) - a.mergeFanIn
		level := a.runLevels[start] + 1
		if err := a.mergeRuns(start); err != nil {
			return err
		}
		a.runLevels = append(a.runLevels[:start], level)
	}
	return nil
}

func (a *PathAccumulator) mergeRuns(start int) error {
	output, err := a.newRunPath()
	if err != nil {
		return err
	}
	file, err := os.Create(output)
	if err != nil {
		return err
	}
	writer := bufio.NewWriterSize(queryWorkWriter{file, a.work}, mergeWriterBufferBytes)
	err = mergePathRuns(a.ctx, a.runs[start:], func(path string) error { return writePath(writer, path) })
	if flushErr := writer.Flush(); err == nil {
		err = flushErr
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	for _, old := range a.runs[start:] {
		if err := os.Remove(old); err != nil {
			return err
		}
	}
	a.runs = append(a.runs[:start], output)
	return nil
}

func writePathRun(ctx context.Context, path string, paths []string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	writer := bufio.NewWriterSize(queryWorkWriter{file, QueryWorkFromContext(ctx)}, mergeWriterBufferBytes)
	for _, path := range paths {
		if err = writePath(writer, path); err != nil {
			break
		}
	}
	if flushErr := writer.Flush(); err == nil {
		err = flushErr
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	return err
}

func writePath(writer io.Writer, path string) error {
	var size [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(size[:], uint64(len(path)))
	if _, err := writer.Write(size[:n]); err != nil {
		return err
	}
	_, err := io.WriteString(writer, path)
	return err
}

type pathRun struct {
	file    *os.File
	reader  *bufio.Reader
	current string
	index   int
}

func (r *pathRun) next() error {
	size, err := binary.ReadUvarint(r.reader)
	if err != nil {
		return err
	}
	if size > maxAccumulatedPathBytes {
		return fmt.Errorf("invalid scratch record size %d", size)
	}
	data := make([]byte, int(size))
	if _, err := io.ReadFull(r.reader, data); err != nil {
		return err
	}
	r.current = string(data)
	return nil
}

type pathRunHeap []*pathRun

func (h pathRunHeap) Len() int           { return len(h) }
func (h pathRunHeap) Less(i, j int) bool { return h[i].current < h[j].current }
func (h pathRunHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *pathRunHeap) Push(value any)    { *h = append(*h, value.(*pathRun)) }
func (h *pathRunHeap) Pop() any {
	old := *h
	last := old[len(old)-1]
	*h = old[:len(old)-1]
	return last
}

func mergePathRuns(ctx context.Context, paths []string, visit func(string) error) error {
	work := QueryWorkFromContext(ctx)
	runs := make([]*pathRun, 0, len(paths))
	defer func() {
		for _, run := range runs {
			_ = run.file.Close()
		}
	}()
	queue := pathRunHeap{}
	for index, path := range paths {
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		run := &pathRun{file: file, reader: bufio.NewReaderSize(queryWorkReader{file, work}, mergeReaderBufferBytes), index: index}
		runs = append(runs, run)
		if err = run.next(); errors.Is(err, io.EOF) {
			continue
		} else if err != nil {
			return err
		}
		heap.Push(&queue, run)
	}
	var previous string
	havePrevious := false
	for queue.Len() > 0 {
		if err := work.Checkpoint(ctx); err != nil {
			return err
		}
		run := heap.Pop(&queue).(*pathRun)
		if !havePrevious || run.current != previous {
			if err := visit(run.current); err != nil {
				return err
			}
			previous, havePrevious = run.current, true
		}
		if err := run.next(); errors.Is(err, io.EOF) {
			continue
		} else if err != nil {
			return err
		}
		heap.Push(&queue, run)
	}
	return nil
}
