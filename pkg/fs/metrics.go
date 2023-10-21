package fs

import (
	"time"

	"github.com/lambertxiao/go-s3fs/pkg/utils"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

var (
	start = time.Now()
	cpu   = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "cpu_usage",
		Help: "Accumulated CPU usage in seconds.",
	}, func() float64 {
		ru := utils.GetRusage()
		return ru.GetStime() + ru.GetUtime()
	})

	memory = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "memory",
		Help: "Used memory in bytes.",
	}, func() float64 {
		_, rss := utils.MemoryUsage()
		return float64(rss)
	})

	uptime = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "uptime",
		Help: "Total running time in seconds.",
	}, func() float64 {
		return time.Since(start).Seconds()
	})
)

func InitMetricRegistry(mountPoint string, bucket string) (*prometheus.Registry, prometheus.Registerer) {
	registry := prometheus.NewRegistry()
	registerer := prometheus.WrapRegistererWithPrefix(
		"gos3fs_",
		prometheus.WrapRegistererWith(prometheus.Labels{"mp": mountPoint, "bucket": bucket}, registry))

	registerer.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	registerer.MustRegister(collectors.NewGoCollector())
	return registry, registerer
}

func RegistMetrics(registerer prometheus.Registerer) {
	if registerer == nil {
		return
	}
	registerer.MustRegister(cpu)
	registerer.MustRegister(memory)
	registerer.MustRegister(uptime)
}
