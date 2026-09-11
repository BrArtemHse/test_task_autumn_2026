package runtime

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const maxGRPCMessageBytes = 4 << 20

func RunHealthServer(ctx context.Context, config Config) error {
	mux := http.NewServeMux()
	mux.Handle("/healthz", NewHealthHandler(config.ServiceName))
	return RunHTTPServer(ctx, config, mux)
}

func RunHTTPServer(ctx context.Context, config Config, handler http.Handler) error {
	if handler == nil {
		handler = NewHealthHandler(config.ServiceName)
	}
	server := &http.Server{
		Addr:              config.HTTPAddr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	errs := make(chan error, 1)
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errs <- err
			return
		}
		errs <- nil
	}()

	select {
	case err := <-errs:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), config.ShutdownTimeout)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			return err
		}
		return <-errs
	}
}

func RunHTTPAndGRPCServers(ctx context.Context, config Config, httpHandler http.Handler, register func(*grpc.Server)) error {
	if register == nil {
		return fmt.Errorf("%w: grpc register func is required", ErrInvalidConfig)
	}
	if httpHandler == nil {
		httpHandler = NewHealthHandler(config.ServiceName)
	}

	listener, err := net.Listen("tcp", config.GRPCAddr)
	if err != nil {
		return err
	}

	httpServer := &http.Server{
		Addr:              config.HTTPAddr,
		Handler:           httpHandler,
		ReadHeaderTimeout: 5 * time.Second,
	}
	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(maxGRPCMessageBytes),
		grpc.MaxSendMsgSize(maxGRPCMessageBytes),
	)
	register(grpcServer)

	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	errs := make(chan error, 2)
	go func() {
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errs <- err
			return
		}
		errs <- nil
	}()
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			errs <- err
			return
		}
		errs <- nil
	}()

	select {
	case err := <-errs:
		shutdownServers(context.Background(), httpServer, grpcServer, config.ShutdownTimeout)
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), config.ShutdownTimeout)
		defer cancel()
		shutdownServers(shutdownCtx, httpServer, grpcServer, config.ShutdownTimeout)
		return firstServerExit(errs)
	}
}

func NewGRPCClientConn(target string) (*grpc.ClientConn, error) {
	if target == "" {
		return nil, fmt.Errorf("%w: grpc target is required", ErrInvalidConfig)
	}

	return grpc.NewClient(
		target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxGRPCMessageBytes),
			grpc.MaxCallSendMsgSize(maxGRPCMessageBytes),
		),
	)
}

func shutdownServers(ctx context.Context, httpServer *http.Server, grpcServer *grpc.Server, timeout time.Duration) {
	done := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(timeout):
		grpcServer.Stop()
	case <-ctx.Done():
		grpcServer.Stop()
	}

	_ = httpServer.Shutdown(ctx)
}

func firstServerExit(errs <-chan error) error {
	for range 2 {
		err := <-errs
		if err != nil {
			return err
		}
	}
	return nil
}
