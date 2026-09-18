package forwarding

// FrameBytes bounds allocations made before the receiver can inspect a private
// response. Keep the receive limit fixed even when a peer announces a tiny size.
const FrameBytes = 32 << 10
const FrameMessageBytes = FrameBytes + 32

// StreamScratchBytes covers one fixed HTTP/2 window, framing and decoded chunks.
const StreamScratchBytes = 192 << 10
