package capacity

import "github.com/prometheus/client_golang/prometheus"

type observations struct {
	limit     *prometheus.Desc
	used      *prometheus.Desc
	watermark *prometheus.Desc
	blocked   *prometheus.Desc
	source    *prometheus.Desc
	admitted  prometheus.Counter
	rejected  prometheus.Counter
}

func newObservations(opts Options) *observations {
	labels := prometheus.Labels{"role": opts.Role, "store": opts.Store}
	admittedOpts := prometheus.CounterOpts{Name: "sink_memory_admitted_total", Help: "New requests admitted below the memory watermarks.", ConstLabels: labels}
	rejectedOpts := prometheus.CounterOpts{Name: "sink_memory_rejected_total", Help: "New requests rejected by memory pressure before execution.", ConstLabels: labels}
	observed := &observations{
		limit:     prometheus.NewDesc("sink_memory_limit_bytes", "Effective process memory ceiling used to calculate admission watermarks.", nil, labels),
		used:      prometheus.NewDesc("sink_memory_used_bytes", "Observed process memory; RSS on Linux, Go runtime memory when RSS is unavailable.", []string{"source"}, labels),
		watermark: prometheus.NewDesc("sink_memory_watermark_bytes", "High rejection and low recovery watermarks.", []string{"watermark"}, labels),
		blocked:   prometheus.NewDesc("sink_memory_pressure", "Whether new work is paused by the high/low memory watermark policy.", nil, labels),
		source:    prometheus.NewDesc("sink_memory_limit_source_info", "Source of the effective memory ceiling.", []string{"source"}, labels),
		admitted:  prometheus.NewCounter(admittedOpts), rejected: prometheus.NewCounter(rejectedOpts),
	}
	return observed
}
func (g *Guard) Describe(ch chan<- *prometheus.Desc) {
	for _, desc := range []*prometheus.Desc{g.observed.limit, g.observed.used, g.observed.watermark, g.observed.blocked, g.observed.source} {
		ch <- desc
	}
	g.observed.admitted.Describe(ch)
	g.observed.rejected.Describe(ch)
}
func (g *Guard) Collect(ch chan<- prometheus.Metric) {
	used, blocked, source := g.snapshot()
	pressure := 0.0
	if blocked {
		pressure = 1
	}
	ch <- prometheus.MustNewConstMetric(g.observed.limit, prometheus.GaugeValue, float64(g.limit))
	ch <- prometheus.MustNewConstMetric(g.observed.used, prometheus.GaugeValue, float64(used), source)
	ch <- prometheus.MustNewConstMetric(g.observed.watermark, prometheus.GaugeValue, float64(g.high), "high")
	ch <- prometheus.MustNewConstMetric(g.observed.watermark, prometheus.GaugeValue, float64(g.low), "low")
	ch <- prometheus.MustNewConstMetric(g.observed.blocked, prometheus.GaugeValue, pressure)
	ch <- prometheus.MustNewConstMetric(g.observed.source, prometheus.GaugeValue, 1, g.limitSource)
	g.observed.admitted.Collect(ch)
	g.observed.rejected.Collect(ch)
}
