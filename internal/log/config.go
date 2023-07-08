package log

type Config struct {
	// Segment is the configuration for log segments.
	Segment SegmentConfig
}

type SegmentConfig struct {
	// MaxStoreBytes is the maximum store size in bytes.
	MaxStoreBytes uint64
	// MaxIndexBytes is the maximum index size in bytes.
	MaxIndexBytes uint64
	// InitialOffset is the initial offset of the segment.
	InitialOffset uint64
}
