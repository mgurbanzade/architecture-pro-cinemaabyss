// Events service (MVP) for CinemaAbyss.
//
// Proves the Kafka hypothesis: an HTTP API creates User / Payment / Movie
// events, publishes them to Kafka topics, and the same service consumes those
// topics and writes every processed event to its log.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

const (
	topicMovie    = "movie-events"
	topicUser     = "user-events"
	topicPayment  = "payment-events"
	consumerGroup = "events-service"
)

// Event is the envelope written to Kafka (see api-specification.yaml, schema Event).
type Event struct {
	ID        string                 `json:"id"`
	Type      string                 `json:"type"`
	Timestamp time.Time              `json:"timestamp"`
	Payload   map[string]interface{} `json:"payload"`
}

// EventResponse is returned to the API caller (schema EventResponse).
type EventResponse struct {
	Status    string `json:"status"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Event     Event  `json:"event"`
}

// eventKind describes one event type: its topic, required fields and how to build the event id.
type eventKind struct {
	name     string
	topic    string
	required []string
	idFields []string // fields concatenated into the event id, e.g. movie-1-viewed
}

var kinds = map[string]eventKind{
	"movie":   {name: "movie", topic: topicMovie, required: []string{"movie_id", "title", "action"}, idFields: []string{"movie_id", "action"}},
	"user":    {name: "user", topic: topicUser, required: []string{"user_id", "action", "timestamp"}, idFields: []string{"user_id", "action"}},
	"payment": {name: "payment", topic: topicPayment, required: []string{"payment_id", "user_id", "amount", "status", "timestamp"}, idFields: []string{"payment_id", "status"}},
}

// ---------- Producer ----------

type Producer struct {
	sp sarama.SyncProducer
}

func newProducer(brokers []string) (*Producer, error) {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_7_0_0
	cfg.Producer.Return.Successes = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Retry.Max = 5
	sp, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		return nil, err
	}
	return &Producer{sp: sp}, nil
}

func (p *Producer) Publish(topic string, ev Event) (int32, int64, error) {
	body, err := json.Marshal(ev)
	if err != nil {
		return 0, 0, err
	}
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(ev.ID),
		Value: sarama.ByteEncoder(body),
	}
	return p.sp.SendMessage(msg)
}

// ---------- Consumer ----------

// consumerHandler logs every message it receives.
type consumerHandler struct{}

func (consumerHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (consumerHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }
func (consumerHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		var ev Event
		if err := json.Unmarshal(msg.Value, &ev); err != nil {
			log.Printf("[consumer] topic=%s partition=%d offset=%d: bad payload: %v", msg.Topic, msg.Partition, msg.Offset, err)
		} else {
			log.Printf("[consumer] topic=%s partition=%d offset=%d id=%s type=%s payload=%v",
				msg.Topic, msg.Partition, msg.Offset, ev.ID, ev.Type, ev.Payload)
		}
		sess.MarkMessage(msg, "")
	}
	return nil
}

func runConsumer(ctx context.Context, brokers []string) error {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_7_0_0
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	group, err := sarama.NewConsumerGroup(brokers, consumerGroup, cfg)
	if err != nil {
		return err
	}
	defer group.Close()

	topics := []string{topicMovie, topicUser, topicPayment}
	log.Printf("[consumer] subscribed to %v", topics)
	for {
		if err := group.Consume(ctx, topics, consumerHandler{}); err != nil {
			log.Printf("[consumer] error: %v", err)
			time.Sleep(2 * time.Second)
		}
		if ctx.Err() != nil {
			return nil
		}
	}
}

// ---------- HTTP API ----------

type server struct {
	producer *Producer
}

func (s *server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]bool{"status": true})
}

// handleEvent serves POST /api/events/{movie|user|payment}.
func (s *server) handleEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	kindName := strings.TrimPrefix(r.URL.Path, "/api/events/")
	kind, ok := kinds[kindName]
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown event type"})
		return
	}

	var payload map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
		return
	}
	for _, f := range kind.required {
		if _, present := payload[f]; !present {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "missing required field: " + f})
			return
		}
	}

	ev := Event{
		ID:        buildID(kind, payload),
		Type:      kind.name,
		Timestamp: time.Now().UTC(),
		Payload:   payload,
	}
	partition, offset, err := s.producer.Publish(kind.topic, ev)
	if err != nil {
		log.Printf("[producer] failed to publish to %s: %v", kind.topic, err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to publish event"})
		return
	}
	log.Printf("[producer] topic=%s partition=%d offset=%d id=%s", kind.topic, partition, offset, ev.ID)

	writeJSON(w, http.StatusCreated, EventResponse{Status: "success", Partition: partition, Offset: offset, Event: ev})
}

func buildID(kind eventKind, payload map[string]interface{}) string {
	parts := []string{kind.name}
	for _, f := range kind.idFields {
		parts = append(parts, fmt.Sprint(payload[f]))
	}
	return strings.Join(parts, "-")
}

func writeJSON(w http.ResponseWriter, status int, body interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// ---------- main ----------

func main() {
	port := getenv("PORT", "8082")
	brokers := strings.Split(getenv("KAFKA_BROKERS", "localhost:9092"), ",")

	producer := connectWithRetry(brokers)
	defer producer.sp.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		if err := runConsumer(ctx, brokers); err != nil {
			log.Fatalf("[consumer] fatal: %v", err)
		}
	}()

	s := &server{producer: producer}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/events/health", s.handleHealth)
	mux.HandleFunc("/api/events/", s.handleEvent)

	srv := &http.Server{Addr: ":" + port, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	log.Printf("starting events service on port %s, brokers=%v", port, brokers)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}

// connectWithRetry keeps trying to reach Kafka: in docker-compose the broker
// usually starts later than this service.
func connectWithRetry(brokers []string) *Producer {
	for attempt := 1; ; attempt++ {
		p, err := newProducer(brokers)
		if err == nil {
			log.Printf("[producer] connected to Kafka %v", brokers)
			return p
		}
		if attempt >= 30 {
			log.Fatalf("[producer] cannot connect to Kafka after %d attempts: %v", attempt, err)
		}
		log.Printf("[producer] Kafka not ready (attempt %d): %v", attempt, err)
		time.Sleep(3 * time.Second)
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
