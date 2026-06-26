package api

import (
	"container/list"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// maxPerTaskLabels bounds the cardinality of the by-task Prometheus
// metrics.  Each task that emits a sample evicts the least-recently
// used task label once the cap is hit, so a long-running node cannot
// accumulate an unbounded number of time series even if the task
// manager spawns an enormous number of short-lived tasks.
const maxPerTaskLabels = 256

var (
	taskCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nexus_dl_tasks_total",
			Help: "Total number of tasks processed",
		},
		[]string{"status"},
	)

	taskDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "nexus_dl_task_duration_seconds",
			Help:    "Duration of tasks in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"status"},
	)

	downloadSpeed = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "nexus_dl_download_speed_bytes_per_second",
			Help: "Total download speed across all active tasks in bytes per second",
		},
	)

	downloadSpeedByTask = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "nexus_dl_download_speed_by_task",
			Help: "Per-task download speed (LRU-capped to " + strconv.Itoa(maxPerTaskLabels) + " active task labels)",
		},
		[]string{"task_id"},
	)

	downloadProgressByTask = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "nexus_dl_download_progress_by_task",
			Help: "Per-task download progress percent (LRU-capped)",
		},
		[]string{"task_id"},
	)

	downloadProgress = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "nexus_dl_download_progress_percent",
			Help: "Average download progress across all active tasks",
		},
	)

	activeConnections = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "nexus_dl_active_connections",
			Help: "Number of active connections",
		},
	)

	nodeCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "nexus_dl_cluster_nodes",
			Help: "Number of nodes in the cluster",
		},
		[]string{"status"},
	)

	httpRequests = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nexus_dl_http_requests_total",
			Help: "Total number of HTTP requests",
		},
		// "endpoint" is a registered route pattern (e.g. /api/tasks/{id})
		// rather than r.URL.Path so we do not blow up Prometheus
		// cardinality on dynamic segments such as task IDs.
		[]string{"method", "endpoint", "status"},
	)

	httpRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "nexus_dl_http_request_duration_seconds",
			Help:    "Duration of HTTP requests in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "endpoint"},
	)

	// perTaskLRU keeps track of which task IDs currently own a label
	// slot in downloadSpeedByTask / downloadProgressByTask.  When the
	// cap is reached, the oldest entry is evicted and its label
	// removed from the underlying GaugeVec to free the time series.
	perTaskLRU   = list.New()
	perTaskEntry = make(map[string]*list.Element)
	perTaskMu    sync.Mutex
)

type perTaskEntry_t struct {
	taskID string
}

func init() {
	prometheus.MustRegister(
		taskCounter,
		taskDuration,
		downloadSpeed,
		downloadSpeedByTask,
		downloadProgressByTask,
		downloadProgress,
		activeConnections,
		nodeCount,
		httpRequests,
		httpRequestDuration,
	)
}

func MetricsHandler() http.Handler {
	return promhttp.Handler()
}

func RecordTaskStatus(status string, duration time.Duration) {
	taskCounter.WithLabelValues(status).Inc()
	taskDuration.WithLabelValues(status).Observe(duration.Seconds())
}

// touchPerTaskLRU records that taskID is in active use.  When the LRU
// grows beyond maxPerTaskLabels, the oldest entry is dropped and its
// label removed from the underlying GaugeVec to keep memory bounded.
func touchPerTaskLRU(taskID string) {
	perTaskMu.Lock()
	defer perTaskMu.Unlock()

	if el, ok := perTaskEntry[taskID]; ok {
		perTaskLRU.MoveToFront(el)
		return
	}
	el := perTaskLRU.PushFront(&perTaskEntry_t{taskID: taskID})
	perTaskEntry[taskID] = el

	for perTaskLRU.Len() > maxPerTaskLabels {
		oldest := perTaskLRU.Back()
		if oldest == nil {
			return
		}
		perTaskLRU.Remove(oldest)
		evicted := oldest.Value.(*perTaskEntry_t).taskID
		delete(perTaskEntry, evicted)
		downloadSpeedByTask.DeleteLabelValues(evicted)
		downloadProgressByTask.DeleteLabelValues(evicted)
	}
}

// ForgetPerTask releases the label slot for a task (e.g. when it
// finishes or is removed) so the entry does not occupy LRU capacity
// until it is recycled.
func ForgetPerTask(taskID string) {
	perTaskMu.Lock()
	defer perTaskMu.Unlock()
	if el, ok := perTaskEntry[taskID]; ok {
		perTaskLRU.Remove(el)
		delete(perTaskEntry, taskID)
	}
	downloadSpeedByTask.DeleteLabelValues(taskID)
	downloadProgressByTask.DeleteLabelValues(taskID)
}

func RecordDownloadSpeed(taskID string, speed int64) {
	touchPerTaskLRU(taskID)
	downloadSpeedByTask.WithLabelValues(taskID).Set(float64(speed))
}

func RecordDownloadProgress(taskID string, progress float64) {
	touchPerTaskLRU(taskID)
	downloadProgressByTask.WithLabelValues(taskID).Set(progress)
}

func RecordAggregateDownloadSpeed(totalSpeed int64) {
	downloadSpeed.Set(float64(totalSpeed))
}

func SetAverageDownloadProgress(avgProgress float64) {
	downloadProgress.Set(avgProgress)
}

func SetActiveConnections(count int) {
	activeConnections.Set(float64(count))
}

func SetNodeCount(status string, count int) {
	nodeCount.WithLabelValues(status).Set(float64(count))
}

// RecordHTTPRequest records an HTTP request.  The endpoint label
// should be a static route pattern, not a raw path; see
// NormalizeEndpoint.
func RecordHTTPRequest(method, endpoint string, status int, duration time.Duration) {
	httpRequests.WithLabelValues(method, endpoint, strconv.Itoa(status)).Inc()
	httpRequestDuration.WithLabelValues(method, endpoint).Observe(duration.Seconds())
}

// NormalizeEndpoint maps a request path to a low-cardinality route
// pattern.  Dynamic segments (UUIDs, hex hashes) are collapsed to
// placeholders so Prometheus does not accumulate a new time series
// per request.  Unknown paths are bucketed into a single label to
// preserve the observability signal.
func NormalizeEndpoint(path string) string {
	if path == "" {
		return "<empty>"
	}
	leadingSlash := strings.HasPrefix(path, "/")
	segs := splitPath(path)
	for i, seg := range segs {
		if isLikelyID(seg) {
			segs[i] = "{id}"
		}
	}
	if len(segs) == 0 {
		return "/"
	}
	out := strings.Join(segs, "/")
	if leadingSlash {
		return "/" + out
	}
	return out
}

func splitPath(p string) []string {
	out := make([]string, 0, strings.Count(p, "/")+1)
	start := 0
	for i := 0; i < len(p); i++ {
		if p[i] == '/' {
			if i > start {
				out = append(out, p[start:i])
			}
			start = i + 1
		}
	}
	if start < len(p) {
		out = append(out, p[start:])
	}
	return out
}

func isLikelyID(seg string) bool {
	if len(seg) < 8 {
		return false
	}
	dashCount := 0
	hexCount := 0
	for i := 0; i < len(seg); i++ {
		c := seg[i]
		switch {
		case c == '-':
			dashCount++
		case (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F'):
			hexCount++
		default:
			return false
		}
	}
	if dashCount > 0 {
		return hexCount == len(seg)-dashCount
	}
	return hexCount == len(seg) && len(seg) >= 16
}

func PrometheusMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		wrapped := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

		next.ServeHTTP(wrapped, r)

		duration := time.Since(start)
		endpoint := NormalizeEndpoint(r.URL.Path)
		RecordHTTPRequest(r.Method, endpoint, wrapped.statusCode, duration)
	})
}
