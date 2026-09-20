package forwarding

// FrameBytes bounds allocations made before the receiver can inspect a private
// response. Keep the receive limit fixed even when a peer announces a tiny size.
const FrameBytes = 32 << 10
const FrameMessageBytes = FrameBytes + 32
