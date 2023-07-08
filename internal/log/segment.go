package log

import (
	"fmt"
	"os"
	"path"

	api "github.com/kentliuqiao/proglog/api/v1"
	"google.golang.org/protobuf/proto"
)

type Segment struct {
	store *store
	index *index
	// baseOffset is the offset of the first record in the segment.
	baseOffset uint64
	// nextOffset is the offset of the next record that will be appended to the segment.
	nextOffset uint64
	// config is the configuration for the segment.
	config Config
}

func newSegment(dir string, baseOffset uint64, c Config) (*Segment, error) {
	s := &Segment{
		baseOffset: baseOffset,
		config:     c,
	}
	var err error
	storeFile, err := os.OpenFile(
		path.Join(
			dir,
			fmt.Sprintf("%d%s", baseOffset, ".store"),
		),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}
	s.store, err = newStore(storeFile)
	if err != nil {
		return nil, err
	}
	indexFile, err := os.OpenFile(
		path.Join(
			dir,
			fmt.Sprintf("%d%s", baseOffset, ".index"),
		),
		os.O_RDWR|os.O_CREATE,
		0644,
	)
	if err != nil {
		return nil, err
	}
	s.index, err = newIndex(indexFile, c)
	if err != nil {
		return nil, err
	}
	off, _, err := s.index.Read(-1)
	if err != nil {
		s.nextOffset = baseOffset
	} else {
		s.nextOffset = baseOffset + uint64(off) + 1
	}

	return s, nil
}

func (s *Segment) Append(record *api.Record) (offset uint64, err error) {
	curr := s.nextOffset
	record.Offset = curr
	p, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}
	_, pos, err := s.store.Append(p)
	if err != nil {
		return 0, err
	}
	if err = s.index.Write(uint32(s.nextOffset-s.baseOffset), pos); err != nil {
		return 0, err
	}
	s.nextOffset++

	return curr, nil
}

func (s *Segment) Read(off uint64) (*api.Record, error) {
	if off < s.baseOffset {
		return nil, fmt.Errorf("offset %d is less than the base offset %d", off, s.baseOffset)
	}
	if off >= s.nextOffset {
		return nil, fmt.Errorf("offset %d is out of the range", off)
	}
	_, pos, err := s.index.Read(int64(off - s.baseOffset))
	if err != nil {
		return nil, err
	}
	p, err := s.store.Read(pos)
	if err != nil {
		return nil, err
	}
	record := &api.Record{}
	err = proto.Unmarshal(p, record)

	return record, err
}

func (s *Segment) IsMaxed() bool {
	return s.store.size >= s.config.Segment.MaxStoreBytes ||
		s.index.size >= s.config.Segment.MaxIndexBytes
}

func (s *Segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}

	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}

	return os.Remove(s.store.Name())
}

func (s *Segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}

	return s.store.Close()
}

func nearestMultiple(j, k uint64) uint64 {
	// if j >= 0 {
	// 	return (j / k) * k
	// }

	return ((j - k + 1) / k) * k
}
