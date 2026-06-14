package grpc

import (
	"context"
	"io"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/internal/channelz"
	"google.golang.org/grpc/internal/transport"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// clientStream implements ClientStream.
type clientStream struct {
	ctx          context.Context
	callInfo     *callInfo
	cc           *ClientConn
	desc         *StreamDesc
	method       string
	committed    bool
	mu           sync.Mutex
	// other fields omitted for brevity
}

func (cs *clientStream) commit() {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.committed = true
}

func (cs *clientStream) SendMsg(m interface{}) (err error) {
	// ... existing code ...
	err = cs.withRetry(func() error {
		return cs.write(op, data, opts)
	}, cs.commit)
	if err == nil && !cs.desc.IsClientStream() {
		// For unary and server-streaming RPCs, we commit the stream when the
		// request is written to the transport.
		cs.commit()
	}
	return err
}

func (cs *clientStream) retry(err error, retryInfo *retryInfo, history *[]*attempt) (bool, error) {
	if cs.ctx.Err() != nil {
		return false, cs.ctx.Err()
	}
	cs.mu.Lock()
	committed := cs.committed
	cs.mu.Unlock()
	if committed && !cs.callInfo.idempotent {
		return false, err
	}
	// ... existing retry evaluation logic ...
	return true, nil
}