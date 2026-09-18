package capacity

import (
	"context"
	"io"
)

// ReadAll acquires each new backing array before allocating it. During growth
// both arrays are charged; after copying only the retained allocation remains.
// The second copy covers JSON decoding while adapters own the response body.
// The caller owns lease and releases it when the body and decoded working set
// are discarded or transferred to separately charged output buffers.
func ReadAll(ctx context.Context, reader io.Reader, maximum int64, lease *Lease) ([]byte, error) {
	if lease == nil {
		return io.ReadAll(io.LimitReader(reader, maximum))
	}
	var body []byte
	for {
		if len(body) == cap(body) {
			if int64(len(body)) == maximum {
				return body, nil
			}
			next := min(maximum, max(1024, int64(cap(body))*2))
			if err := Grow(ctx, lease, int(2*next)); err != nil {
				return nil, err
			}
			grown := make([]byte, len(body), int(next))
			copy(grown, body)
			lease.Shrink(2 * int64(cap(body)))
			body = grown
		}
		n, err := reader.Read(body[len(body):cap(body)])
		body = body[:len(body)+n]
		if err == io.EOF {
			return body, nil
		}
		if err != nil {
			return nil, err
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
}
