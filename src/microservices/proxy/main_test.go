package main

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

// fakeUpstream returns a test server that answers with its own name.
func fakeUpstream(name string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Served-By", name)
		w.WriteHeader(http.StatusOK)
	}))
}

func newTestRouter(t *testing.T, gradual bool, percent int) (*Router, func()) {
	t.Helper()
	mono, movies, events := fakeUpstream("monolith"), fakeUpstream("movies-service"), fakeUpstream("events-service")
	parse := func(s string) *url.URL {
		u, err := url.Parse(s)
		if err != nil {
			t.Fatal(err)
		}
		return u
	}
	cfg := Config{
		MonolithURL:            parse(mono.URL),
		MoviesServiceURL:       parse(movies.URL),
		EventsServiceURL:       parse(events.URL),
		GradualMigration:       gradual,
		MoviesMigrationPercent: percent,
	}
	return NewRouter(cfg), func() { mono.Close(); movies.Close(); events.Close() }
}

func servedBy(rt *Router, path string) string {
	rec := httptest.NewRecorder()
	rt.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec.Header().Get("X-Served-By")
}

func TestStaticRoutes(t *testing.T) {
	rt, done := newTestRouter(t, true, 50)
	defer done()

	cases := map[string]string{
		"/api/users":         "monolith",
		"/api/payments?id=1": "monolith",
		"/api/subscriptions": "monolith",
		"/api/events/movie":  "events-service",
		"/api/events/health": "events-service",
		"/api/movies/health": "movies-service",
	}
	for path, want := range cases {
		if got := servedBy(rt, path); got != want {
			t.Errorf("%s: served by %q, want %q", path, got, want)
		}
	}
}

func TestOwnHealth(t *testing.T) {
	rt, done := newTestRouter(t, true, 50)
	defer done()
	rec := httptest.NewRecorder()
	rt.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/health status %d", rec.Code)
	}
}

func TestMoviesMigrationPercent(t *testing.T) {
	for _, tc := range []struct {
		gradual bool
		percent int
		wantMin int // minimal share of movies-service hits out of 1000
		wantMax int
	}{
		{true, 0, 0, 0},
		{true, 100, 1000, 1000},
		{true, 50, 400, 600},
		{false, 0, 1000, 1000}, // flag off: whole domain on the microservice
	} {
		rt, done := newTestRouter(t, tc.gradual, tc.percent)
		hits := 0
		for i := 0; i < 1000; i++ {
			if servedBy(rt, "/api/movies") == "movies-service" {
				hits++
			}
		}
		done()
		if hits < tc.wantMin || hits > tc.wantMax {
			t.Errorf("gradual=%t percent=%d: movies-service hits=%d, want in [%d,%d]", tc.gradual, tc.percent, hits, tc.wantMin, tc.wantMax)
		}
	}
}
