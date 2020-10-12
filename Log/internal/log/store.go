package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	// encoding that we persist record sizes and index entries in
	enc = binary.BigEndian
)

const (
	// number of bytes used to store the record's length
	lenWidth = 8
)

type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

func newStore(f *os.File) (*store, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	size := uint64(fi.Size())
	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pos = s.size
	// write the binary representation of the data into the bufio.Writer
	// we first tell w how much data we're going to write, and with what encoding format
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}

	// write to the buffered writer, which will eventually flush to the file
	// more efficient, since it reduces the number of system calls
	// w is the number of bytes written to the writer
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}

	// len(p) from the binary.Write call above takes up lenWidth bytes in the store
	w += lenWidth
	// the #bytes from p + #bytes that represent the length of the record
	s.size += uint64(w)

	return uint64(w), pos, nil
}

func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// flush the buffered writer so we don't read a record that hasn't been flushed to disk yet
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	// find out how many bytes we have to read to the whole record
	// read the interval [pos, lenWidth] into the size array
	size := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}

	// read the interval [pos+lenWidth, pos+lenWidth+size] into b
	b := make([]byte, enc.Uint64(size))
	if _, err := s.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}
	return b, nil
}

// Reads len(p) bytes startinf from the given offset into p
func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return 0, err
	}

	return s.File.ReadAt(p, off)
}

func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.buf.Flush()
	if err != nil {
		return err
	}
	return s.File.Close()
}
