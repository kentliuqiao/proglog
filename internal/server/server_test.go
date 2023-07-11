package server

import (
	"context"
	"net"
	"os"
	"testing"

	api "github.com/kentliuqiao/proglog/api/v1"
	"github.com/kentliuqiao/proglog/internal/auth"
	"github.com/kentliuqiao/proglog/internal/config"
	"github.com/kentliuqiao/proglog/internal/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

const (
	testInitialOffset uint64 = 33
)

func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, rootClient, nobodyClient api.LogClient, config *Config){
		"produce and consume a message succeeds": testProduceConsume,
		"produce and consume stream succeeds":    testProduceConsumeStream,
		"consume past log boundary fails":        testConsumePastBoundary,
		"unauthorized fails":                     testUnauthorized,
	} {
		t.Run(scenario, func(t *testing.T) {
			rootClient, nobodyClient, config, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, rootClient, nobodyClient, config)
		})
	}
}

func testUnauthorized(t *testing.T, rootClient, nobodyClient api.LogClient, config *Config) {
	ctx := context.Background()
	produce, err := nobodyClient.Produce(
		ctx,
		&api.ProduceRequest{
			Record: &api.Record{Value: []byte("hello world")},
		},
	)
	require.Nil(t, produce)
	gotCode := status.Code(err)
	wantCode := codes.PermissionDenied
	require.Equal(t, gotCode, wantCode)

	consume, err := nobodyClient.Consume(
		ctx,
		&api.ConsumeRequest{Offset: testInitialOffset},
	)
	require.Nil(t, consume)
	gotCode = status.Code(err)
	require.Equal(t, gotCode, wantCode)
}

func testConsumePastBoundary(t *testing.T, client, _ api.LogClient, config *Config) {
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

func testProduceConsumeStream(t *testing.T, client, _ api.LogClient, config *Config) {
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

func testProduceConsume(t *testing.T, client, _ api.LogClient, config *Config) {
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
	rootClient, nobodyClient api.LogClient, cfg *Config, teardown func(),
) {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	newClient := func(crtPath, keyPath string) (*grpc.ClientConn, api.LogClient, []grpc.DialOption) {
		clientTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
			CertFile: crtPath,
			KeyFile:  keyPath,
			CAFile:   config.CAFile,
			Server:   false,
		})
		require.NoError(t, err)
		clientCreds := credentials.NewTLS(clientTLSConfig)
		opts := []grpc.DialOption{grpc.WithTransportCredentials(clientCreds)}
		conn, err := grpc.Dial(l.Addr().String(), opts...)
		require.NoError(t, err)
		client := api.NewLogClient(conn)
		return conn, client, opts
	}

	var rootConn *grpc.ClientConn
	rootConn, rootClient, _ = newClient(config.RootClientCertFile, config.RootClientKeyFile)

	var nobodyConn *grpc.ClientConn
	nobodyConn, nobodyClient, _ = newClient(config.NobodyClientCertFile, config.NobodyClientKeyFile)

	// clientTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
	// 	CertFile: config.ClientCertFile,
	// 	KeyFile:  config.ClientKeyFile,
	// 	CAFile:   config.CAFile,
	// })
	// require.NoError(t, err)
	// clientCreds := credentials.NewTLS(clientTLSConfig)

	dir, err := os.MkdirTemp("", "server_test")
	require.NoError(t, err)

	clog, err := log.NewLog(dir, log.Config{Segment: log.SegmentConfig{
		InitialOffset: testInitialOffset,
	}})
	require.NoError(t, err)

	authorizer, err := auth.New(config.ACLModelFile, config.ACLPolicyFile)
	require.NoError(t, err)

	cfg = &Config{
		CommitLog:  clog,
		Authorizer: authorizer,
	}
	if fn != nil {
		fn(cfg)
	}

	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		ServerAddress: l.Addr().String(),
		Server:        true,
	})
	require.NoError(t, err)
	serverCreds := credentials.NewTLS(serverTLSConfig)

	server, err := NewGRPCServer(cfg, grpc.Creds(serverCreds))
	require.NoError(t, err)

	go func() {
		err := server.Serve(l)
		require.NoError(t, err)
	}()

	// clientOptions := []grpc.DialOption{
	// 	grpc.WithTransportCredentials(clientCreds),
	// }
	// cc, err := grpc.Dial(l.Addr().String(), clientOptions...)
	// require.NoError(t, err)
	// client = api.NewLogClient(cc)

	return rootClient, nobodyClient, cfg, func() {
		server.Stop()
		rootConn.Close()
		nobodyConn.Close()
		l.Close()
		os.RemoveAll(dir)
	}
}
