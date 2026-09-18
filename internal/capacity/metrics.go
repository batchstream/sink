package capacity

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type observations struct {
	admitted        *prometheus.CounterVec
	capacity        *prometheus.Desc
	opaque          *prometheus.Desc
	used            *prometheus.Desc
	waitingBytes    *prometheus.Desc
	waitingRequests *prometheus.Desc
	oldest          *prometheus.Desc
	borrowers       *prometheus.Desc
	source          *prometheus.Desc
	rejected        *prometheus.CounterVec
	wait            *prometheus.HistogramVec
}

func newObservations(opts Options) *observations {
	labels := prometheus.Labels{"role": opts.Role, "store": opts.Store}
	observed := &observations{}
	observed.opaque = prometheus.NewDesc("sink_memory_opaque_reserved_bytes", "Temporary capacity reserved around driver-owned wire allocations; included in used bytes.", nil, labels)
	observed.capacity = prometheus.NewDesc("sink_memory_capacity_bytes", "Managed capacity; burst is reserved for completing admitted work.", []string{"pool"}, labels)
	observed.used = prometheus.NewDesc("sink_memory_used_bytes", "Owned payload, copy and opaque driver capacity, not process RSS.", []string{"pool"}, labels)
	observed.waitingBytes = prometheus.NewDesc("sink_memory_waiting_bytes", "Additional bytes requested by capacity waiters.", []string{"phase"}, labels)
	observed.waitingRequests = prometheus.NewDesc("sink_memory_waiting_requests", "Number of capacity acquisitions waiting, not a request concurrency limit.", []string{"phase"}, labels)
	observed.oldest = prometheus.NewDesc("sink_memory_oldest_wait_seconds", "Age of the oldest capacity waiter.", []string{"phase"}, labels)
	observed.borrowers = prometheus.NewDesc("sink_memory_burst_borrowers", "Operations owning the completion reserve, zero or one.", nil, labels)
	sourceLabels := prometheus.Labels{"role": opts.Role, "store": opts.Store, "source": opts.Source}
	observed.source = prometheus.NewDesc("sink_memory_capacity_source_info", "Source of the resolved process memory capacity.", nil, sourceLabels)
	admittedOpts := prometheus.CounterOpts{Name: "sink_memory_admitted_total", Help: "Successful capacity acquisitions by phase.", ConstLabels: labels}
	observed.admitted = prometheus.NewCounterVec(admittedOpts, []string{"phase"})
	rejectedOpts := prometheus.CounterOpts{Name: "sink_memory_rejected_total", Help: "Capacity acquisition failures by phase and reason.", ConstLabels: labels}
	observed.rejected = prometheus.NewCounterVec(rejectedOpts, []string{"phase", "reason"})
	waitOpts := prometheus.HistogramOpts{Name: "sink_memory_wait_seconds", Help: "Time waiting for process memory capacity.", ConstLabels: labels, Buckets: prometheus.DefBuckets}
	observed.wait = prometheus.NewHistogramVec(waitOpts, []string{"phase", "outcome"})
	for _, phase := range []Phase{Request, Response} {
		observed.admitted.WithLabelValues(string(phase)).Add(0)
		for _, reason := range []string{"oversize", "busy", "wait_timeout"} {
			observed.rejected.WithLabelValues(string(phase), reason).Add(0)
		}
	}
	return observed
}

func (p *Pool) Describe(ch chan<- *prometheus.Desc) {
	o := p.observed
	for _, desc := range []*prometheus.Desc{o.opaque, o.capacity, o.used, o.waitingBytes, o.waitingRequests, o.oldest, o.borrowers, o.source} {
		ch <- desc
	}
	o.admitted.Describe(ch)
	o.rejected.Describe(ch)
	o.wait.Describe(ch)
}

func (p *Pool) Collect(ch chan<- prometheus.Metric) {
	p.mu.Lock()
	opaque := p.opaque
	used, normal, total := p.used, p.normal, p.total
	borrowers := 0.0
	if p.burstOwner != nil {
		borrowers = 1
	}
	requests := map[Phase]int64{Request: 0, Response: 0}
	bytes := map[Phase]int64{Request: 0, Response: 0}
	oldest := map[Phase]float64{Request: 0, Response: 0}
	for _, w := range p.waiters {
		requests[w.phase]++
		bytes[w.phase] += w.bytes
		oldest[w.phase] = max(oldest[w.phase], time.Since(w.started).Seconds())
	}
	p.mu.Unlock()
	o := p.observed
	ch <- prometheus.MustNewConstMetric(o.opaque, prometheus.GaugeValue, float64(opaque))
	ch <- prometheus.MustNewConstMetric(o.capacity, prometheus.GaugeValue, float64(normal), "normal")
	ch <- prometheus.MustNewConstMetric(o.capacity, prometheus.GaugeValue, float64(total-normal), "burst")
	ch <- prometheus.MustNewConstMetric(o.used, prometheus.GaugeValue, float64(min(used, normal)), "normal")
	ch <- prometheus.MustNewConstMetric(o.used, prometheus.GaugeValue, float64(max(0, used-normal)), "burst")
	ch <- prometheus.MustNewConstMetric(o.borrowers, prometheus.GaugeValue, borrowers)
	ch <- prometheus.MustNewConstMetric(o.source, prometheus.GaugeValue, 1)
	for _, phase := range []Phase{Request, Response} {
		ch <- prometheus.MustNewConstMetric(o.waitingRequests, prometheus.GaugeValue, float64(requests[phase]), string(phase))
		ch <- prometheus.MustNewConstMetric(o.waitingBytes, prometheus.GaugeValue, float64(bytes[phase]), string(phase))
		ch <- prometheus.MustNewConstMetric(o.oldest, prometheus.GaugeValue, oldest[phase], string(phase))
	}
	o.admitted.Collect(ch)
	o.rejected.Collect(ch)
	o.wait.Collect(ch)
}
