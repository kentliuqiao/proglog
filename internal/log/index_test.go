package log

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndex(t *testing.T) {
	f, err := os.CreateTemp(os.TempDir(), "index_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	c := Config{}
	c.Segment.MaxIndexBytes = 1024
	idx, err := newIndex(f, c)
	require.NoError(t, err)
	_, _, err = idx.Read(-1)
	require.Equal(t, io.EOF, err)
	require.Equal(t, idx.Name(), f.Name())

	entries := []struct {
		off uint32
		pos uint64
	}{
		{0, 0},
		{1, 10},
	}

	for _, want := range entries {
		err := idx.Write(want.off, want.pos)
		require.NoError(t, err)

		gotOff, gotPos, err := idx.Read(int64(want.off))
		require.NoError(t, err)
		require.Equal(t, want.off, gotOff)
		require.Equal(t, want.pos, gotPos)
	}

	_, _, err = idx.Read(int64(len(entries)))
	require.Equal(t, io.EOF, err)

	err = idx.Close()
	require.NoError(t, err)

	f, err = os.OpenFile(f.Name(), os.O_RDWR, 0600)
	require.NoError(t, err)

	idx, err = newIndex(f, c)
	require.NoError(t, err)

	off, pos, err := idx.Read(-1)
	require.NoError(t, err)
	require.Equal(t, entries[len(entries)-1].off, off)
	require.Equal(t, entries[len(entries)-1].pos, pos)
}
