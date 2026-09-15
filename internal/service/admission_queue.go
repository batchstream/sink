package service

// The caller holds admissionMu. Direct RPCs share one bounded input queue;
// batched RPCs and scans retain their existing separate queue bounds.
func (s *admissionPool) directWaitQueueFull(request admissionRequest) bool {
	if s.queuedRequests >= s.maxQueuedRequests || request.inputBytes > s.maxQueuedBytes-s.queuedBytes {
		return true
	}
	for _, name := range request.stores {
		if s.storeQueuedRequests[name] >= s.maxStoreQueuedRequests {
			return true
		}
	}
	return false
}

func (s *admissionPool) adjustDirectQueue(request admissionRequest, delta int) {
	s.queuedRequests += delta
	s.queuedBytes += delta * request.inputBytes
	for _, name := range request.stores {
		s.storeQueuedRequests[name] += delta
		if s.storeQueuedRequests[name] == 0 {
			delete(s.storeQueuedRequests, name)
		}
	}
	store := s.metrics.RequestStores(request.stores)
	s.metrics.AdjustExecutionQueue(store, delta, delta*request.inputBytes)
}
