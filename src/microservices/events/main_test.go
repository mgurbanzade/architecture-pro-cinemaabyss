package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/IBM/sarama"
	"github.com/IBM/sarama/mocks"
)

func newTestServer(t *testing.T) (*server, *mocks.SyncProducer) {
	t.Helper()
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	mp := mocks.NewSyncProducer(t, cfg)
	return &server{producer: &Producer{sp: mp}}, mp
}

func post(s *server, path, body string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	s.handleEvent(rec, req)
	return rec
}

func TestHealth(t *testing.T) {
	s, _ := newTestServer(t)
	rec := httptest.NewRecorder()
	s.handleHealth(rec, httptest.NewRequest(http.MethodGet, "/api/events/health", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"status":true`) {
		t.Fatalf("health: code=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestCreateEvents(t *testing.T) {
	cases := []struct {
		path, body, wantTopic, wantID string
	}{
		{"/api/events/movie", `{"movie_id":1,"title":"Test","action":"viewed","user_id":1}`, topicMovie, "movie-1-viewed"},
		{"/api/events/user", `{"user_id":1,"username":"u","action":"logged_in","timestamp":"2024-01-01T00:00:00Z"}`, topicUser, "user-1-logged_in"},
		{"/api/events/payment", `{"payment_id":7,"user_id":1,"amount":9.99,"status":"completed","timestamp":"2024-01-01T00:00:00Z","method_type":"credit_card"}`, topicPayment, "payment-7-completed"},
	}
	for _, tc := range cases {
		s, mp := newTestServer(t)
		mp.ExpectSendMessageWithMessageCheckerFunctionAndSucceed(func(msg *sarama.ProducerMessage) error {
			if msg.Topic != tc.wantTopic {
				t.Errorf("%s: topic %q, want %q", tc.path, msg.Topic, tc.wantTopic)
			}
			return nil
		})
		rec := post(s, tc.path, tc.body)
		if rec.Code != http.StatusCreated {
			t.Fatalf("%s: code=%d body=%s", tc.path, rec.Code, rec.Body.String())
		}
		var resp EventResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatal(err)
		}
		if resp.Status != "success" || resp.Event.ID != tc.wantID || resp.Event.Type != strings.TrimPrefix(tc.path, "/api/events/") {
			t.Errorf("%s: unexpected response %+v", tc.path, resp)
		}
	}
}

func TestValidation(t *testing.T) {
	s, _ := newTestServer(t)
	if rec := post(s, "/api/events/movie", `{"title":"no id"}`); rec.Code != http.StatusBadRequest {
		t.Errorf("missing field: code=%d", rec.Code)
	}
	if rec := post(s, "/api/events/movie", `not json`); rec.Code != http.StatusBadRequest {
		t.Errorf("bad json: code=%d", rec.Code)
	}
	if rec := post(s, "/api/events/unknown", `{}`); rec.Code != http.StatusNotFound {
		t.Errorf("unknown type: code=%d", rec.Code)
	}
	rec := httptest.NewRecorder()
	s.handleEvent(rec, httptest.NewRequest(http.MethodGet, "/api/events/movie", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET: code=%d", rec.Code)
	}
}
