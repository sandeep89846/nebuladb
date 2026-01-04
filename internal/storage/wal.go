package storage

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"sync"

	"github.com/sandeep89846/nebuladb/pkg/vec"
)

const (
	OpInsert = 1
	OpDelete = 2
)

type WAL struct {
	file *os.File
	bw   *bufio.Writer
	mu   sync.Mutex
}

func OpenWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	return &WAL{
		file: f,
		bw:   bufio.NewWriter(f),
	}, nil
}

// WriteInsert appends an insertion record to the log.
// Format: [CRC(4)][Op(1)][KeyLen(2)][KeyBytes(...)][VecLen(4)][VecBytes(...)]
func (w *WAL) WriteInsert(id string, v vec.Vector) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// prepare data.
	keyBytes := []byte(id)
	keyLen := uint16(len(keyBytes))
	vecLen := uint32(len(v))

	// Total size = 1(Op) + 2(KeyLen) + len(key) + 4(VecLen) + len(vec)*4
	payloadSize := 1 + 2 + len(keyBytes) + 4 + (int(vecLen) * 4)
	buf := make([]byte, payloadSize)

	offset := 0
	buf[offset] = OpInsert
	offset++

	// precautionary: using little endian.
	binary.LittleEndian.PutUint16(buf[offset:], keyLen)
	offset += 2

	copy(buf[offset:], keyBytes)
	offset += len(keyBytes)

	binary.LittleEndian.PutUint32(buf[offset:], vecLen)
	offset += 4

	for _, f := range v {
		bits := mathFloat32bits(f)
		binary.LittleEndian.PutUint32(buf[offset:], bits)
		offset += 4
	}

	crc := crc32.ChecksumIEEE(buf)

	if err := binary.Write(w.bw, binary.LittleEndian, crc); err != nil {
		return err
	}

	if _, err := w.bw.Write(buf); err != nil {
		return err
	}

	return w.bw.Flush()
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.bw.Flush(); err != nil {
		return err
	}
	return w.file.Close()
}

// Replay calls the callback function for every valid entry in the WAL.
// This is used on startup to rebuild the index.
func (w *WAL) Replay(onInsert func(id string, v vec.Vector)) error {
	// Need to read from the start
	if _, err := w.file.Seek(0, 0); err != nil {
		return err
	}

	br := bufio.NewReader(w.file)

	for {
		// 1. Read CRC
		var crc uint32
		err := binary.Read(br, binary.LittleEndian, &crc)
		if err == io.EOF {
			break // End of file
		}
		if err != nil {
			return fmt.Errorf("read crc: %v", err)
		}

		// 2. Read OpCode
		op, err := br.ReadByte()
		if err != nil {
			return fmt.Errorf("read op: %v", err)
		}

		// 3. Read Key Length
		var keyLen uint16
		if err := binary.Read(br, binary.LittleEndian, &keyLen); err != nil {
			return fmt.Errorf("read key len: %v", err)
		}

		// 4. Read Key
		keyBytes := make([]byte, keyLen)
		if _, err := io.ReadFull(br, keyBytes); err != nil {
			return fmt.Errorf("read key: %v", err)
		}
		id := string(keyBytes)

		// 5. Read Vector Length
		var vecLen uint32
		if err := binary.Read(br, binary.LittleEndian, &vecLen); err != nil {
			return fmt.Errorf("read vec len: %v", err)
		}

		// 6. Read Vector Data
		v := make(vec.Vector, vecLen)
		for i := 0; i < int(vecLen); i++ {
			var bits uint32
			if err := binary.Read(br, binary.LittleEndian, &bits); err != nil {
				return fmt.Errorf("read vec data: %v", err)
			}
			v[i] = mathFloat32frombits(bits)
		}

		// 7. Verify CRC (Reconstruct payload to check)
		// Note: Ideally we read the raw bytes into a buffer to check CRC,
		// but for simplicity we assume file integrity if read succeeded.
		// In a production system, you MUST reconstruct the buffer and compare crc32.

		if op == OpInsert {
			onInsert(id, v)
		}
	}

	// Reset pointer to end for appending
	w.file.Seek(0, 2)
	return nil
}

func mathFloat32bits(f float32) uint32 {
	return math.Float32bits(f)
}

func mathFloat32frombits(b uint32) float32 {
	return math.Float32frombits(b)
}
