package service

type writeMemoryReservation struct {
	reservation *admissionReservation
	estimate    writeExecutionEstimate
}

func (m *writeMemoryReservation) retain(snapshotBytes int, candidateBytes int) error {
	// This refines the existing payload reservation, not the process RSS limit.
	// Keep all original input, Lua source and returned-document allowances.
	// The snapshots remain live until applyWriteSnapshots returns. Allow an
	// extra snapshot copy and three candidate copies for conversion/encoding.
	working := min(m.estimate.workingBytes, 2*snapshotBytes+3*candidateBytes)
	bytes := m.estimate.bytes - m.estimate.workingBytes + working
	return m.reservation.resize(bytes)
}
