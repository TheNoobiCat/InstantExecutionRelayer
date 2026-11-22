package metrics

import (
	"fmt"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
)

// Metrics holds all Prometheus metrics for the keeper bot.
type Metrics struct {
	// Price stream metrics
	PythMsgLatency *prometheus.HistogramVec
	PythReconnects prometheus.Counter
	PythConnected  prometheus.Gauge

	// Order tracking metrics
	ActiveOrders    *prometheus.GaugeVec
	OrdersTriggered *prometheus.CounterVec
	OrdersExecuted  *prometheus.CounterVec
	OrdersFailed    *prometheus.CounterVec
	OrdersDropped   *prometheus.CounterVec

	// Trigger engine metrics
	TriggerLatency    prometheus.Histogram   // end-to-end: Pyth msg received -> trigger detected
	SimulationLatency prometheus.Histogram   // time spent in eth_call simulation
	SimulationResults *prometheus.CounterVec // success/failure reasons

	// Execution metrics
	TxSubmitted *prometheus.CounterVec
	TxConfirmed *prometheus.CounterVec
	TxFailed    *prometheus.CounterVec
	TxGasUsed   *prometheus.HistogramVec
	E2ELatency  prometheus.Histogram // Pyth msg -> tx broadcast

	// Sync metrics
	SyncDuration prometheus.Histogram
	SyncErrors   prometheus.Counter
	LastSyncTime prometheus.Gauge
}

// NewMetrics initializes all Prometheus metrics.
func NewMetrics() *Metrics {
	return &Metrics{
		PythMsgLatency: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "keeper",
			Subsystem: "pyth",
			Name:      "msg_latency_ms",
			Help:      "Latency from Pyth publish_time to local receipt (milliseconds)",
			Buckets:   []float64{50, 100, 200, 500, 1000, 2000, 5000, 10000},
		}, []string{"feed_id"}),

		PythReconnects: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "keeper",
			Subsystem: "pyth",
			Name:      "reconnects_total",
			Help:      "Total number of SSE stream reconnections",
		}),

		PythConnected: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: "keeper",
			Subsystem: "pyth",
			Name:      "connected",
			Help:      "Whether the Pyth SSE stream is currently connected (1=yes, 0=no)",
		}),

		ActiveOrders: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "keeper",
			Subsystem: "orderbook",
			Name:      "active_orders",
			Help:      "Number of active triggerable orders per market",
		}, []string{"market"}),

		OrdersTriggered: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "keeper",
			Subsystem: "orderbook",
			Name:      "triggered_total",
			Help:      "Total orders triggered by price movements",
		}, []string{"market"}),

		OrdersExecuted: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "keeper",
			Subsystem: "execution",
			Name:      "executed_total",
			Help:      "Total orders successfully executed on-chain",
		}, []string{"market", "type"}),

		OrdersFailed: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "keeper",
			Subsystem: "execution",
			Name:      "failed_total",
			Help:      "Total order executions that failed",
		}, []string{"market", "reason"}),

		OrdersDropped: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "keeper",
			Subsystem: "orderbook",
			Name:      "dropped_total",
			Help:      "Total orders dropped after max retries",
		}, []string{"market"}),

		TriggerLatency: promauto.NewHistogram(prometheus.HistogramOpts{
			Namespace: "keeper",
			Subsystem: "trigger",
			Name:      "detection_latency_seconds",
			Help:      "Time from price update receipt to trigger detection",
			Buckets:   prometheus.DefBuckets,
		}),

		SimulationLatency: promauto.NewHistogram(prometheus.HistogramOpts{
			Namespace: "keeper",
			Subsystem: "trigger",
			Name:      "simulation_latency_seconds",
			Help:      "Time spent in eth_call simulation",
			Buckets:   []float64{0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0},
		}),

		SimulationResults: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "keeper",
			Subsystem: "trigger",
			Name:      "simulation_results_total",
			Help:      "Simulation results by outcome",
		}, []string{"market", "result"}),

		TxSubmitted: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "keeper",
			Subsystem: "tx",
			Name:      "submitted_total",
			Help:      "Total transactions submitted",
		}, []string{"market"}),

		TxConfirmed: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "keeper",
			Subsystem: "tx",
			Name:      "confirmed_total",
			Help:      "Total transactions confirmed",
		}, []string{"market"}),

		TxFailed: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "keeper",
			Subsystem: "tx",
			Name:      "failed_total",
			Help:      "Total transactions that failed",
		}, []string{"market"}),

		TxGasUsed: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "keeper",
			Subsystem: "tx",
			Name:      "gas_used",
			Help:      "Gas used per transaction",
			Buckets:   []float64{50000, 100000, 200000, 300000, 500000, 750000, 1000000},
		}, []string{"market", "type"}),

		E2ELatency: promauto.NewHistogram(prometheus.HistogramOpts{
			Namespace: "keeper",
			Subsystem: "trigger",
			Name:      "e2e_latency_seconds",
			Help:      "End-to-end latency from Pyth message receipt to transaction broadcast",
			Buckets:   []float64{0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0},
		}),

		SyncDuration: promauto.NewHistogram(prometheus.HistogramOpts{
			Namespace: "keeper",
			Subsystem: "sync",
			Name:      "duration_seconds",
			Help:      "Duration of full on-chain sync operations",
			Buckets:   []float64{0.5, 1.0, 2.5, 5.0, 10.0, 30.0},
		}),

		SyncErrors: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "keeper",
			Subsystem: "sync",
			Name:      "errors_total",
			Help:      "Total sync errors",
		}),

		LastSyncTime: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: "keeper",
			Subsystem: "sync",
			Name:      "last_sync_timestamp",
			Help:      "Unix timestamp of last successful sync",
		}),
	}
}

// Serve starts the Prometheus HTTP metrics server.
func Serve(port int, logger zerolog.Logger) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	addr := fmt.Sprintf(":%d", port)
	logger.Info().Str("addr", addr).Msg("starting metrics server")

	go func() {
		if err := http.ListenAndServe(addr, mux); err != nil {
			logger.Fatal().Err(err).Msg("metrics server failed")
		}
	}()
}
