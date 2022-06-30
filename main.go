package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/jamiealquiza/tachymeter"
	"google.golang.org/grpc"
	"google.golang.org/grpc/binarylog/grpc_binarylog_v1"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
	v1 "mkm.pub/binlog/proto"
)

func mainE() error {
	rand.Seed(time.Now().UnixNano())

	ctx := context.Background()
	client, err := grpc.DialContext(ctx, "localhost:8091", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	sink := v1.NewLogSinkServiceClient(client)

	errCh := make(chan error)
	prefix := rand.Intn(1024 * 1024 * 1024)

	for i := 0; i < 200; i++ {
		go func() {
			errCh <- worker(ctx, prefix, sink)
		}()
	}

	return <-errCh
}

func worker(ctx context.Context, prefix int, sink v1.LogSinkServiceClient) error {
	prefix2 := rand.Intn(1024 * 1024 * 1024)
	c := tachymeter.New(&tachymeter.Config{Size: 1000})

	for i := 0; ; i++ {
		if i%100 == 0 {
			log.Printf("%d: tick %d, p90: %v", prefix, i, c.Calc().Time.P99)
		}

		req := &v1.WriteRequest{
			Origin: "tsm",
			CallId: fmt.Sprintf("%d-%d-%d", prefix, prefix2, i),
			Entry: &grpc_binarylog_v1.GrpcLogEntry{
				Timestamp: timestamppb.Now(),
				CallId:    uint64(i),
				Type:      grpc_binarylog_v1.GrpcLogEntry_EVENT_TYPE_CLIENT_HEADER,
			},
		}
		start := time.Now()
		_, err := sink.Write(ctx, req)
		c.AddTime(time.Since(start))

		if err != nil {
			return err
		}

	}

	return nil
}

func main() {
	if err := mainE(); err != nil {
		log.Fatal(err)
	}
}
