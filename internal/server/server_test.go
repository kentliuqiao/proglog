package server

import (
	"context"
	"net"
	"os"
	"testing"

	api "github.com/kentliuqiao/proglog/api/v1"
	"github.com/kentliuqiao/proglog/internal/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

const (
	testInitialOffset uint64 = 33
)

func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, client api.LogClient, config *Config){
		"produce and consume a message succeeds": testProduceConsume,
		"produce and consume stream succeeds":    testProduceConsumeStream,
		"consume past log boundary fails":        testConsumePastBoundary,
	} {
		t.Run(scenario, func(t *testing.T) {
			client, config, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, client, config)
		})
	}
}

func testConsumePastBoundary(t *testing.T, client api.LogClient, config *Config) {
	ctx := context.Background()
	produce, err := client.Produce(ctx, &api.ProduceRequest{Record: &api.Record{Value: []byte("hello world")}})
	require.NoError(t, err)
	require.Equal(t, produce.Offset, uint64(testInitialOffset))

	consume, err := client.Consume(ctx, &api.ConsumeRequest{Offset: testInitialOffset + 1})
	require.Nil(t, consume)
	got := status.Code(err)
	want := status.Code(api.ErrOffsetOutOfRange{Offset: 0})
	require.Equal(t, got, want)
}

func testProduceConsumeStream(t *testing.T, client api.LogClient, config *Config) {
	ctx := context.Background()
	want := &api.Record{Value: []byte("hello world")}

	produceStream, err := client.ProduceStream(ctx)
	require.NoError(t, err)
	for i := 0; i < 3; i++ {
		err = produceStream.Send(&api.ProduceRequest{Record: want})
		require.NoError(t, err)
		res, err := produceStream.Recv()
		require.NoError(t, err)
		require.Equal(t, uint64(i)+testInitialOffset, res.Offset)
	}
	err = produceStream.CloseSend()
	require.NoError(t, err)

	consumeStream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{Offset: testInitialOffset + 1})
	require.NoError(t, err)
	for i := uint64(1); i < 3; i++ {
		consume, err := consumeStream.Recv()
		require.NoError(t, err)
		require.Equal(t, want.Value, consume.Record.Value)
		require.Equal(t, testInitialOffset+i, consume.Record.Offset)
	}
}

func testProduceConsume(t *testing.T, client api.LogClient, config *Config) {
	ctx := context.Background()
	want := &api.Record{Value: []byte("hello world")}
	produce, err := client.Produce(ctx, &api.ProduceRequest{Record: want})
	require.NoError(t, err)
	require.Equal(t, produce.Offset, uint64(testInitialOffset))

	consume, err := client.Consume(ctx, &api.ConsumeRequest{Offset: produce.Offset})
	require.NoError(t, err)
	require.Equal(t, want.Value, consume.Record.Value)
	require.Equal(t, testInitialOffset, consume.Record.Offset)

	produce, err = client.Produce(ctx, &api.ProduceRequest{Record: want})
	require.NoError(t, err)
	require.Equal(t, produce.Offset, uint64(testInitialOffset+1))

	consume, err = client.Consume(ctx, &api.ConsumeRequest{Offset: produce.Offset})
	require.NoError(t, err)
	require.Equal(t, want.Value, consume.Record.Value)
	require.Equal(t, testInitialOffset+1, consume.Record.Offset)
}

func setupTest(t *testing.T, fn func(config *Config)) (
	client api.LogClient, config *Config, teardown func(),
) {
	t.Helper()

	l, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

	dir, err := os.MkdirTemp("", "server_test")
	require.NoError(t, err)

	clog, err := log.NewLog(dir, log.Config{Segment: log.SegmentConfig{InitialOffset: testInitialOffset}})
	require.NoError(t, err)

	cfg := &Config{
		CommitLog: clog,
	}
	if fn != nil {
		fn(cfg)
	}

	server, err := NewGRPCServer(cfg)
	require.NoError(t, err)

	go func() {
		err := server.Serve(l)
		require.NoError(t, err)
	}()

	clientOptions := []grpc.DialOption{grpc.WithInsecure()}
	cc, err := grpc.Dial(l.Addr().String(), clientOptions...)
	require.NoError(t, err)

	client = api.NewLogClient(cc)

	return client, cfg, func() {
		server.Stop()
		cc.Close()
		l.Close()
		os.RemoveAll(dir)
	}
}
