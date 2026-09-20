package metrics

import (
	"time"

	forward "github.com/liran/sink/gen/forward"
	"google.golang.org/grpc/codes"
)

// ObserveForward retains the public method labels when the wire RPC is Forward.
func (m *Metrics) ObserveForward(req *forward.ForwardRequest, code codes.Code, elapsed time.Duration) {
	if m == nil {
		return
	}
	method, request, _ := forwardObservation(req, nil)
	if method == "" {
		return
	}
	store := m.RequestStore(request)
	m.requests.WithLabelValues(store, method, code.String()).Inc()
	m.requestDuration.WithLabelValues(store, method).Observe(elapsed.Seconds())
}

// ObserveForwardResult records delivered outcomes without retaining documents.
func (m *Metrics) ObserveForwardResult(req *forward.ForwardRequest, resp *forward.ForwardResponse) {
	if m == nil {
		return
	}
	method, request, response := forwardObservation(req, resp)
	if method != "" {
		m.observeOperationResults(method, request, response)
	}
}
func forwardObservation(req *forward.ForwardRequest, resp *forward.ForwardResponse) (string, any, any) {
	var method string
	var request, response any
	switch body := req.GetRequest().(type) {
	case *forward.ForwardRequest_Read:
		method = "Read"
		request = body.Read
		response = resp.GetRead()
	case *forward.ForwardRequest_Write:
		method = "Write"
		request = body.Write
		response = resp.GetWrite()
	case *forward.ForwardRequest_Delete:
		method = "Delete"
		request = body.Delete
		response = resp.GetDelete()
	case *forward.ForwardRequest_Execute:
		method = "Execute"
		request = body.Execute
		response = resp.GetExecute()
	case *forward.ForwardRequest_Query:
		method = "Query"
		request = body.Query
		response = resp.GetQuery()
	case *forward.ForwardRequest_Count:
		method = "Count"
		request = body.Count
		response = resp.GetCount()
	case *forward.ForwardRequest_Scan:
		method = "Scan"
		request = body.Scan
		response = resp.GetScan()
	default:
		return "", nil, nil
	}
	return method, request, response
}
