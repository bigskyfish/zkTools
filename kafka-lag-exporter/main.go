package main

import (
	"flag"
	"fmt"
	"github.com/golang/glog"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"kafka-lag-exporter/config"
	"kafka-lag-exporter/kafka"
	"net/http"
	"os"
)

var (
	listenAddress = flag.String("web.listen-address", ":8000",
		"Address to listen on for telemetry")
	metricsPath = flag.String("web.telemetry-path", "/metrics",
		"Path under which to expose metrics")
	kafkaFile = flag.String("config", "/Users/floatcloud/goWorkSpace/kafka-lag-exporter/kafka_lag_exporter.yml", "Config file location.")
)

func main() {

	var err error
	defer func() {
		if err != nil {
			fmt.Fprintf(os.Stderr, "%+v", err)
			os.Exit(1)
		}
		os.Exit(0)
	}()

	flag.Parse()

	conf, err := config.ReadConfig(*kafkaFile)
	if err != nil {
		return
	}

	// 将 exporter 注册到 prometheus
	kafkaExporter := kafka.NewKafkaCollectExporter(conf)
	prometheus.MustRegister(kafkaExporter)

	http.Handle(*metricsPath, promhttp.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
             <head><title>floatcloud Kafka Offsets Exporter</title></head>
             <body>
             <h1>floatcloud Kafka Offsets Exporter</h1>
             <p><a href='` + *metricsPath + `'>Metrics</a></p>
             </body>
             </html>`))
	})
	glog.Fatal(http.ListenAndServe(*listenAddress, nil))

}
