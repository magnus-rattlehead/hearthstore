package exportformat

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

const (
	levelDBBlockSize  = 32 * 1024
	levelDBHeaderSize = 7

	recordFull   = 1
	recordFirst  = 2
	recordMiddle = 3
	recordLast   = 4
)

var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

// LogWriter writes records using LevelDB's block-framed log format.
type LogWriter struct {
	w           io.Writer
	blockOffset int
}

func NewLogWriter(w io.Writer) *LogWriter {
	return &LogWriter{w: w}
}

func (w *LogWriter) WriteRecord(record []byte) error {
	remaining := record
	first := true
	for first || len(remaining) > 0 {
		blockRemaining := levelDBBlockSize - w.blockOffset
		if blockRemaining < levelDBHeaderSize {
			if _, err := w.w.Write(make([]byte, blockRemaining)); err != nil {
				return fmt.Errorf("padding LevelDB log block: %w", err)
			}
			w.blockOffset = 0
			blockRemaining = levelDBBlockSize
		}

		fragmentSize := min(len(remaining), blockRemaining-levelDBHeaderSize)
		last := fragmentSize == len(remaining)
		recordType := byte(recordMiddle)
		switch {
		case first && last:
			recordType = recordFull
		case first:
			recordType = recordFirst
		case last:
			recordType = recordLast
		}

		fragment := remaining[:fragmentSize]
		var header [levelDBHeaderSize]byte
		binary.LittleEndian.PutUint32(header[0:4], maskCRC(crc32.Update(0, castagnoliTable, append([]byte{recordType}, fragment...))))
		binary.LittleEndian.PutUint16(header[4:6], uint16(fragmentSize))
		header[6] = recordType
		if _, err := w.w.Write(header[:]); err != nil {
			return fmt.Errorf("writing LevelDB log header: %w", err)
		}
		if _, err := w.w.Write(fragment); err != nil {
			return fmt.Errorf("writing LevelDB log record: %w", err)
		}

		w.blockOffset += levelDBHeaderSize + fragmentSize
		if w.blockOffset == levelDBBlockSize {
			w.blockOffset = 0
		}
		remaining = remaining[fragmentSize:]
		first = false
	}
	return nil
}

// LogReader reads records from LevelDB's block-framed log format.
type LogReader struct {
	r           io.Reader
	blockOffset int
}

func NewLogReader(r io.Reader) *LogReader {
	return &LogReader{r: r}
}

func (r *LogReader) ReadRecord() ([]byte, error) {
	var record []byte
	inFragmentedRecord := false
	for {
		recordType, fragment, err := r.readFragment()
		if err != nil {
			if errors.Is(err, io.EOF) && inFragmentedRecord {
				return nil, io.ErrUnexpectedEOF
			}
			return nil, err
		}

		switch recordType {
		case recordFull:
			if inFragmentedRecord {
				return nil, errors.New("unexpected full record inside fragmented record")
			}
			return fragment, nil
		case recordFirst:
			if inFragmentedRecord {
				return nil, errors.New("unexpected first fragment inside fragmented record")
			}
			record = append(record, fragment...)
			inFragmentedRecord = true
		case recordMiddle:
			if !inFragmentedRecord {
				return nil, errors.New("middle fragment without first fragment")
			}
			record = append(record, fragment...)
		case recordLast:
			if !inFragmentedRecord {
				return nil, errors.New("last fragment without first fragment")
			}
			record = append(record, fragment...)
			return record, nil
		default:
			return nil, fmt.Errorf("unknown LevelDB log record type %d", recordType)
		}
	}
}

func (r *LogReader) readFragment() (byte, []byte, error) {
	blockRemaining := levelDBBlockSize - r.blockOffset
	if blockRemaining < levelDBHeaderSize {
		padding := make([]byte, blockRemaining)
		if _, err := io.ReadFull(r.r, padding); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return 0, nil, io.EOF
			}
			return 0, nil, err
		}
		r.blockOffset = 0
	}

	var header [levelDBHeaderSize]byte
	if _, err := io.ReadFull(r.r, header[:]); err != nil {
		if errors.Is(err, io.EOF) {
			return 0, nil, io.EOF
		}
		return 0, nil, fmt.Errorf("reading LevelDB log header: %w", err)
	}
	length := int(binary.LittleEndian.Uint16(header[4:6]))
	if length > levelDBBlockSize-r.blockOffset-levelDBHeaderSize {
		return 0, nil, fmt.Errorf("LevelDB log fragment length %d exceeds block", length)
	}
	fragment := make([]byte, length)
	if _, err := io.ReadFull(r.r, fragment); err != nil {
		return 0, nil, fmt.Errorf("reading LevelDB log fragment: %w", err)
	}
	r.blockOffset += levelDBHeaderSize + length
	if r.blockOffset == levelDBBlockSize {
		r.blockOffset = 0
	}

	recordType := header[6]
	actualCRC := crc32.Update(0, castagnoliTable, append([]byte{recordType}, fragment...))
	if unmaskCRC(binary.LittleEndian.Uint32(header[0:4])) != actualCRC {
		return 0, nil, errors.New("LevelDB log checksum mismatch")
	}
	return recordType, fragment, nil
}

func maskCRC(crc uint32) uint32 {
	return ((crc >> 15) | (crc << 17)) + 0xa282ead8
}

func unmaskCRC(masked uint32) uint32 {
	rotated := masked - 0xa282ead8
	return (rotated >> 17) | (rotated << 15)
}
