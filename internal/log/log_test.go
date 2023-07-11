package log

import (
	"io"
	"os"
	"testing"

	api "github.com/kentliuqiao/proglog/api/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestLog(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, l *Log){
		"append and read a record succeeds": testAppendRead,
		"offset out of range error":         testOutOfRangeErr,
		"init with existing segments":       testInitExisting,
		"reader":                            testReader,
		"truncate":                          testTruncate,
		// "segment base offset":               testSegmentBaseOffset,
		// "multiple segments":                 testMultipleSegments,
		// "compact":                           testCompact,
	} {
		t.Run(scenario, func(t *testing.T) {
			dir, err := os.MkdirTemp("", "log-test")
			require.NoError(t, err)
			defer os.RemoveAll(dir)

			c := Config{}
			c.Segment.MaxStoreBytes = 32
			l, err := NewLog(dir, c)
			require.NoError(t, err)

			fn(t, l)
		})
	}
}

func testAppendRead(t *testing.T, l *Log) {
	append := &api.Record{Value: []byte("hello world")}
	off, err := l.Append(append)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	read, err := l.Read(off)
	require.NoError(t, err)
	require.Equal(t, read.Offset, off)
	require.Equal(t, read.Value, append.Value)
}

func testOutOfRangeErr(t *testing.T, l *Log) {
	read, err := l.Read(1)
	require.Nil(t, read)
	require.Error(t, err)
	apiErr := err.(api.ErrOffsetOutOfRange)
	require.Equal(t, apiErr.Offset, uint64(1))
}

func testInitExisting(t *testing.T, l *Log) {
	append := &api.Record{Value: []byte("hello world")}
	for i := 0; i < 3; i++ {
		_, err := l.Append(append)
		require.NoError(t, err)
	}

	require.NoError(t, l.Close())

	off, err := l.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	off, err = l.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), off)

	newLog, err := NewLog(l.Dir, l.Config)
	require.NoError(t, err)

	off, err = newLog.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	off, err = newLog.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), off)

	read, err := newLog.Read(off)
	require.NoError(t, err)
	require.Equal(t, read.Offset, uint64(off))
	require.Equal(t, read.Value, append.Value)
}

func testReader(t *testing.T, l *Log) {
	append := &api.Record{Value: []byte("hello world")}
	off, err := l.Append(append)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	reader := l.Reader()
	b, err := io.ReadAll(reader)
	require.NoError(t, err)

	read := &api.Record{}
	err = proto.Unmarshal(b[lenWidth:], read)
	require.NoError(t, err)
	require.Equal(t, read.Value, append.Value)
}

func testTruncate(t *testing.T, l *Log) {
	append := &api.Record{Value: []byte("hello world")}
	for i := 0; i < 3; i++ {
		_, err := l.Append(append)
		require.NoError(t, err)
	}

	err := l.Truncate(1)
	require.NoError(t, err)

	_, err = l.Read(0)
	require.Error(t, err)

	off, err := l.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), off)

	off, err = l.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), off)

	read, err := l.Read(off)
	require.NoError(t, err)
	require.Equal(t, read.Offset, uint64(off))
	require.Equal(t, read.Value, append.Value)
}
