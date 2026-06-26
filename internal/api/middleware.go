package api

import (
	"crypto/subtle"
	"fmt"
	"net"
	"net/http"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/nexus-dl/afd/pkg/logger"
)

type Middleware func(http.Handler) http.Handler

func Chain(h http.Handler, middlewares ...Middleware) http.Handler {
	for i := len(middlewares) - 1; i >= 0; i-- {
		h = middlewares[i](h)
	}
	return h
}

func CORS(enabled bool, allowedOrigins ...string) Middleware {
	allowed := make(map[string]struct{}, len(allowedOrigins))
	for _, o := range allowedOrigins {
		allowed[strings.TrimSpace(o)] = struct{}{}
	}
	useWildcard := len(allowed) == 0
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !enabled {
				next.ServeHTTP(w, r)
				return
			}
			origin := r.Header.Get("Origin")
			if useWildcard {
				w.Header().Set("Access-Control-Allow-Origin", "*")
			} else if origin != "" {
				if _, ok := allowed[origin]; ok {
					w.Header().Set("Access-Control-Allow-Origin", origin)
					w.Header().Set("Vary", "Origin")
					w.Header().Set("Access-Control-Allow-Credentials", "true")
				} else {
					// Reject the cross-origin request rather than
					// silently reflecting an untrusted origin.
					w.WriteHeader(http.StatusForbidden)
					return
				}
			}
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-API-Key")
			if r.Method == "OPTIONS" {
				w.WriteHeader(http.StatusOK)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func Recovery() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if err := recover(); err != nil {
					logger.Log.Errorw("panic recovered",
						"error", fmt.Sprintf("%v", err),
						"stack", string(debug.Stack()),
						"method", r.Method,
						"path", r.URL.Path,
					)
					sendError(w, http.StatusInternalServerError, "Internal Server Error", "")
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

func Auth(token string) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if token == "" {
				// 未配置认证，放行（但启动时应有警告）
				next.ServeHTTP(w, r)
				return
			}

			if !validateToken(r, token) {
				sendError(w, http.StatusUnauthorized, "Unauthorized", "")
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

func validateToken(r *http.Request, token string) bool {
	authHeader := r.Header.Get("Authorization")
	if authHeader != "" {
		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) == 2 && parts[0] == "Bearer" && secureCompare(parts[1], token) {
			return true
		}
	}

	apiKey := r.Header.Get("X-API-Key")
	if apiKey != "" && secureCompare(apiKey, token) {
		return true
	}

	return false
}

func secureCompare(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

func Logging() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			wrapped := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

			next.ServeHTTP(wrapped, r)

			logger.Log.Infow("API request",
				"method", r.Method,
				"path", r.URL.Path,
				"status", wrapped.statusCode,
				"duration", time.Since(start),
				"remote_ip", getClientIP(r),
			)
		})
	}
}

type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

func getClientIP(r *http.Request) string {
	// 优先使用直连地址，避免 X-Forwarded-For 被伪造绕过限流
	if ip, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		return ip
	}
	// 回退到 XFF（不安全，但保持兼容）
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		return strings.TrimSpace(strings.Split(xff, ",")[0])
	}
	return r.RemoteAddr
}

type rateLimiter struct {
	mu       sync.Mutex
	requests map[string]requestWindow
	limit    int
	window   time.Duration
	done     chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

type requestWindow struct {
	count       int
	windowStart time.Time
}

func newRateLimiter(limit int, window time.Duration) *rateLimiter {
	rl := &rateLimiter{
		requests: make(map[string]requestWindow),
		limit:    limit,
		window:   window,
		done:     make(chan struct{}),
	}
	rl.wg.Add(1)
	go func() {
		defer rl.wg.Done()
		rl.cleanup()
	}()
	return rl
}

func (rl *rateLimiter) cleanup() {
	ticker := time.NewTicker(rl.window)
	defer ticker.Stop()
	for {
		select {
		case <-rl.done:
			return
		case <-ticker.C:
			rl.mu.Lock()
			now := time.Now()
			for ip, rw := range rl.requests {
				if now.Sub(rw.windowStart) >= rl.window {
					delete(rl.requests, ip)
				}
			}
			rl.mu.Unlock()
		}
	}
}

func (rl *rateLimiter) stop() {
	rl.stopOnce.Do(func() {
		close(rl.done)
	})
	rl.wg.Wait()
}

func (rl *rateLimiter) allow(ip string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	rw, exists := rl.requests[ip]

	if !exists || now.Sub(rw.windowStart) >= rl.window {
		rl.requests[ip] = requestWindow{
			count:       1,
			windowStart: now,
		}
		return true
	}

	if rw.count >= rl.limit {
		return false
	}

	rw.count++
	rl.requests[ip] = rw
	return true
}

func RateLimit(limit int, window time.Duration) (Middleware, func()) {
	if limit <= 0 {
		return func(next http.Handler) http.Handler { return next }, func() {}
	}

	limiter := newRateLimiter(limit, window)
	mw := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ip := getClientIP(r)
			if !limiter.allow(ip) {
				w.Header().Set("Retry-After", fmt.Sprintf("%d", int(window.Seconds())))
				sendError(w, http.StatusTooManyRequests, "Rate limit exceeded", "")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
	return mw, limiter.stop
}

func (s *Server) rateLimiterMiddleware(limit int, window time.Duration) Middleware {
	mw, stop := RateLimit(limit, window)
	s.rateLimitStop = stop
	return mw
}

func RequestValidation() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.ContentLength > 10*1024*1024 {
				sendError(w, http.StatusRequestEntityTooLarge, "Request too large", fmt.Sprintf("max: 10MB"))
				return
			}

			contentType := r.Header.Get("Content-Type")
			if r.Method == http.MethodPost && contentType != "" &&
				!strings.HasPrefix(contentType, "application/json") &&
				!strings.HasPrefix(contentType, "application/x-www-form-urlencoded") {
				sendError(w, http.StatusUnsupportedMediaType, "Unsupported content type", contentType)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

func Deprecation() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Deprecation", "true")
			w.Header().Set("Link", `</api/v1/>; rel="successor-version"`)
			w.Header().Set("X-NexusDL-Deprecation-Notice", "Use /api/v1/ instead; this path will be removed in a future major version")
			next.ServeHTTP(w, r)
		})
	}
}
