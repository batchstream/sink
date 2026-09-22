package backpressure

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type observations struct {
	limit      *prometheus.Desc
	inFlight   *prometheus.Desc
	changes    *prometheus.Desc
	admissions *prometheus.Desc
	feedback   *prometheus.Desc
	duration   *prometheus.Desc
	baseline   *prometheus.Desc
	cooldown   *prometheus.Desc
	admitted   uint64
	rejected   uint64
	increases  uint64
	overloads  uint64
	slowdowns  uint64
	samples    [methodCount][feedbackCount]uint64
	seconds    [methodCount]float64
}

func newObservations(opts Options) observations {
	labels := prometheus.Labels{"role": opts.Role, "store": opts.Store}
	observed := observations{
		limit:      prometheus.NewDesc("sink_store_concurrency_limit", "Current local Store execution window; zero pauses new work.", nil, labels),
		inFlight:   prometheus.NewDesc("sink_store_executions_in_flight", "Admitted sequential Store executions, including existing retries and cursor sends.", nil, labels),
		changes:    prometheus.NewDesc("sink_store_window_changes_total", "Store window adjustments by reason.", []string{"reason"}, labels),
		admissions: prometheus.NewDesc("sink_store_admissions_total", "Store executions admitted or direct requests rejected before execution.", []string{"outcome"}, labels),
		feedback:   prometheus.NewDesc("sink_store_feedback_total", "Real Store calls classified once, regardless of batch result count.", []string{"method", "signal"}, labels),
		duration:   prometheus.NewDesc("sink_store_backend_duration_seconds_total", "Store call time excluding admission and downstream Emit time.", []string{"method"}, labels),
		baseline:   prometheus.NewDesc("sink_store_latency_baseline_seconds", "Learned Store latency per operation and batch-size class; zero until sampled.", []string{"method", "batch_size"}, labels),
		cooldown:   prometheus.NewDesc("sink_store_cooldown_seconds", "Remaining delay before real work may probe a paused Store.", nil, labels),
	}
	return observed
}

func (c *Controller) Describe(ch chan<- *prometheus.Desc) {
	for _, desc := range []*prometheus.Desc{c.observed.limit, c.observed.inFlight, c.observed.changes, c.observed.admissions, c.observed.feedback, c.observed.duration, c.observed.baseline, c.observed.cooldown} {
		ch <- desc
	}
}

func (c *Controller) Collect(ch chan<- prometheus.Metric) {
	c.mu.Lock()
	observed, limit, inFlight := c.observed, c.limit, c.inFlight
	latency, cooldown := c.latency, max(0, time.Until(c.resumeAt).Seconds())
	c.mu.Unlock()
	ch <- prometheus.MustNewConstMetric(observed.limit, prometheus.GaugeValue, float64(limit))
	ch <- prometheus.MustNewConstMetric(observed.inFlight, prometheus.GaugeValue, float64(inFlight))
	ch <- prometheus.MustNewConstMetric(observed.cooldown, prometheus.GaugeValue, cooldown)
	ch <- prometheus.MustNewConstMetric(observed.changes, prometheus.CounterValue, float64(observed.increases), "increase")
	ch <- prometheus.MustNewConstMetric(observed.changes, prometheus.CounterValue, float64(observed.overloads), "overload")
	ch <- prometheus.MustNewConstMetric(observed.changes, prometheus.CounterValue, float64(observed.slowdowns), "latency")
	ch <- prometheus.MustNewConstMetric(observed.admissions, prometheus.CounterValue, float64(observed.admitted), "admitted")
	ch <- prometheus.MustNewConstMetric(observed.admissions, prometheus.CounterValue, float64(observed.rejected), "rejected")
	for kind, name := range methodNames {
		for signal, label := range []string{"ignored", "healthy", "congested"} {
			ch <- prometheus.MustNewConstMetric(observed.feedback, prometheus.CounterValue, float64(observed.samples[kind][signal]), name, label)
		}
		ch <- prometheus.MustNewConstMetric(observed.duration, prometheus.CounterValue, observed.seconds[kind], name)
		for size, label := range []string{"1", "2_32", "33_128", "129_plus"} {
			ch <- prometheus.MustNewConstMetric(observed.baseline, prometheus.GaugeValue, latency[kind][size].baseline/float64(time.Second), name, label)
		}
	}
}
