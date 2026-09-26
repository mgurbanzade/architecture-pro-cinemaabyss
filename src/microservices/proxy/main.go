// Proxy service (API Gateway) for CinemaAbyss.
//
// Implements the Strangler Fig pattern: a single entry point in front of the
// monolith and the extracted microservices. Traffic for the movies domain is
// gradually shifted from the monolith to the movies microservice using a
// feature flag (GRADUAL_MIGRATION) and a percentage (MOVIES_MIGRATION_PERCENT).
package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds everything the proxy needs, read from environment variables.
type Config struct {
	Port                   string
	MonolithURL            *url.URL
	MoviesServiceURL       *url.URL
	EventsServiceURL       *url.URL
	GradualMigration       bool
	MoviesMigrationPercent int
}

func loadConfig() Config {
	cfg := Config{
		Port:                   getenv("PORT", "8000"),
		MonolithURL:            mustParseURL(getenv("MONOLITH_URL", "http://localhost:8080")),
		MoviesServiceURL:       mustParseURL(getenv("MOVIES_SERVICE_URL", "http://localhost:8081")),
		EventsServiceURL:       mustParseURL(getenv("EVENTS_SERVICE_URL", "http://localhost:8082")),
		GradualMigration:       strings.EqualFold(getenv("GRADUAL_MIGRATION", "false"), "true"),
		MoviesMigrationPercent: 0,
	}

	percent, err := strconv.Atoi(getenv("MOVIES_MIGRATION_PERCENT", "0"))
	if err != nil || percent < 0 || percent > 100 {
		log.Printf("invalid MOVIES_MIGRATION_PERCENT=%q, falling back to 0", os.Getenv("MOVIES_MIGRATION_PERCENT"))
		percent = 0
	}
	cfg.MoviesMigrationPercent = percent
	return cfg
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func mustParseURL(raw string) *url.URL {
	u, err := url.Parse(raw)
	if err != nil {
		log.Fatalf("invalid URL %q: %v", raw, err)
	}
	return u
}

// Router decides which upstream serves a request.
type Router struct {
	cfg      Config
	monolith http.Handler
	movies   http.Handler
	events   http.Handler
	rnd      *rand.Rand
}

func NewRouter(cfg Config) *Router {
	return &Router{
		cfg:      cfg,
		monolith: newReverseProxy(cfg.MonolithURL, "monolith"),
		movies:   newReverseProxy(cfg.MoviesServiceURL, "movies-service"),
		events:   newReverseProxy(cfg.EventsServiceURL, "events-service"),
		rnd:      rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// newReverseProxy builds a reverse proxy that marks responses with the
// upstream name and turns upstream failures into a JSON 502.
func newReverseProxy(target *url.URL, name string) http.Handler {
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.ModifyResponse = func(resp *http.Response) error {
		resp.Header.Set("X-Upstream", name)
		return nil
	}
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		log.Printf("upstream %s error for %s %s: %v", name, r.Method, r.URL.Path, err)
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "upstream " + name + " unavailable"})
	}
	return proxy
}

// pickMoviesUpstream implements the feature flag:
//   - GRADUAL_MIGRATION=true: MOVIES_MIGRATION_PERCENT % of requests go to the
//     movies microservice, the rest stay on the monolith;
//   - GRADUAL_MIGRATION=false: migration mode is off and the whole domain is
//     served by the microservice (see README, "Паттерн Strangler Fig").
func (rt *Router) pickMoviesUpstream() (http.Handler, string) {
	if !rt.cfg.GradualMigration {
		return rt.movies, "movies-service"
	}
	if rt.rnd.Intn(100) < rt.cfg.MoviesMigrationPercent {
		return rt.movies, "movies-service"
	}
	return rt.monolith, "monolith"
}

func (rt *Router) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	path := r.URL.Path

	var upstream http.Handler
	var name string

	switch {
	case path == "/health":
		writeJSON(w, http.StatusOK, map[string]bool{"status": true})
		return
	case path == "/api/movies/health":
		upstream, name = rt.movies, "movies-service"
	case path == "/api/movies" || strings.HasPrefix(path, "/api/movies/"):
		upstream, name = rt.pickMoviesUpstream()
	case path == "/api/events" || strings.HasPrefix(path, "/api/events/"):
		upstream, name = rt.events, "events-service"
	default:
		upstream, name = rt.monolith, "monolith"
	}

	upstream.ServeHTTP(w, r)
	log.Printf("%s %s -> %s (%s)", r.Method, r.URL.RequestURI(), name, time.Since(start).Round(time.Millisecond))
}

func writeJSON(w http.ResponseWriter, status int, body interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func main() {
	cfg := loadConfig()
	log.Printf("proxy config: monolith=%s movies=%s events=%s gradual_migration=%t movies_migration_percent=%d",
		cfg.MonolithURL, cfg.MoviesServiceURL, cfg.EventsServiceURL, cfg.GradualMigration, cfg.MoviesMigrationPercent)

	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           NewRouter(cfg),
		ReadHeaderTimeout: 10 * time.Second,
	}
	log.Printf("starting proxy service on port %s", cfg.Port)
	log.Fatal(srv.ListenAndServe())
}
