package protocol

import (
	"fmt"

	"github.com/liran/sink/internal/capacity"

	"google.golang.org/grpc/encoding"
	_ "google.golang.org/grpc/encoding/proto"
	"google.golang.org/grpc/mem"
)

const vtProtoCodecName = "proto"

type vtProtoCodecMessage interface {
	MarshalToSizedBufferVT([]byte) (int, error)
	UnmarshalVT([]byte) error
	SizeVT() int
}

// VTProtoCodec uses generated VT helpers and delegates messages without them
// to the standard gRPC protobuf codec.
type VTProtoCodec struct {
	fallback encoding.CodecV2
	pool     mem.BufferPool
}

// NewVTProtoCodec creates a scoped CodecV2 with standard protobuf fallback.
func NewVTProtoCodec() *VTProtoCodec {
	codec := &VTProtoCodec{
		fallback: encoding.GetCodecV2(vtProtoCodecName),
		pool:     mem.DefaultBufferPool(),
	}
	return codec
}

func (*VTProtoCodec) Name() string {
	return vtProtoCodecName
}

func (c *VTProtoCodec) Marshal(value any) (mem.BufferSlice, error) {
	if managed, ok := value.(*ManagedMessage); ok {
		return c.marshalManaged(managed)
	}
	message, ok := value.(vtProtoCodecMessage)
	if !ok {
		if c.fallback == nil {
			return nil, fmt.Errorf("vtproto: no protobuf fallback for message %T", value)
		}
		return c.fallback.Marshal(value)
	}

	size := message.SizeVT()
	if mem.IsBelowBufferPoolingThreshold(size) {
		buffer := make([]byte, size)
		written, err := message.MarshalToSizedBufferVT(buffer)
		if err != nil {
			return nil, err
		}
		if written != size {
			return nil, fmt.Errorf("vtproto: marshaled %d bytes, expected %d", written, size)
		}
		encoded := mem.BufferSlice{mem.SliceBuffer(buffer)}
		return encoded, nil
	}

	pooled := c.pool.Get(size)
	buffer := (*pooled)[:size]
	written, err := message.MarshalToSizedBufferVT(buffer)
	if err != nil {
		c.pool.Put(pooled)
		return nil, err
	}
	if written != size {
		c.pool.Put(pooled)
		return nil, fmt.Errorf("vtproto: marshaled %d bytes, expected %d", written, size)
	}
	encoded := mem.BufferSlice{mem.NewBuffer(pooled, c.pool)}
	return encoded, nil
}

func (c *VTProtoCodec) Unmarshal(data mem.BufferSlice, value any) error {
	message, ok := value.(vtProtoCodecMessage)
	if !ok {
		if c.fallback == nil {
			return fmt.Errorf("vtproto: no protobuf fallback for message %T", value)
		}
		return c.fallback.Unmarshal(data, value)
	}
	buffer := data.MaterializeToBuffer(c.pool)
	defer buffer.Free()
	return message.UnmarshalVT(buffer.ReadOnlyData())
}

// The pool callback runs on the last grpc/mem reference, including references
// held by HTTP/2 after SendMsg or the unary handler has returned.
type ownedBufferPool struct{ release func() }

func (*ownedBufferPool) Get(size int) *[]byte { data := make([]byte, size); return &data }
func (p *ownedBufferPool) Put(*[]byte)        { p.release() }

func (c *VTProtoCodec) marshalManaged(managed *ManagedMessage) (mem.BufferSlice, error) {
	message, ok := managed.Message.(vtProtoCodecMessage)
	if !ok {
		return c.Marshal(managed.Message)
	}
	scope := capacity.FromContext(managed.Context)
	size := message.SizeVT()
	// grpc/mem's small SliceBuffer has no final-free callback. Allocate just
	// above its threshold so even tiny replies retain their ownership correctly.
	allocation := max(size, 1025)
	if err := scope.EnsureOutput(managed.Context, allocation); err != nil {
		return nil, err
	}
	release := scope.Retain()
	buffer := make([]byte, size, allocation)
	written, err := message.MarshalToSizedBufferVT(buffer)
	if err != nil {
		release()
		return nil, err
	}
	if written != size {
		release()
		return nil, fmt.Errorf("vtproto: marshaled %d bytes, expected %d", written, size)
	}
	pool := &ownedBufferPool{release: release}
	encoded := mem.BufferSlice{mem.NewBuffer(&buffer, pool)}
	return encoded, nil
}
