package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	topicMovie   = "movie-events"
	topicUser    = "user-events"
	topicPayment = "payment-events"
)

// Event is the generic envelope written to Kafka, matching the Event schema
// in api-specification.yaml.
type Event struct {
	ID        string      `json:"id"`
	Type      string      `json:"type"`
	Timestamp string      `json:"timestamp"`
	Payload   interface{} `json:"payload"`
}

type EventResponse struct {
	Status    string `json:"status"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
	Event     Event  `json:"event"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}

var writers map[string]*kafka.Writer

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func brokerList() []string {
	return strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ",")
}

func newWriter(brokers []string, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Topic:                  topic,
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: true,
	}
}

func writeError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(ErrorResponse{Error: message})
}

func produceEvent(w http.ResponseWriter, r *http.Request, eventType, topic string) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var payload map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	event := Event{
		ID:        fmt.Sprintf("%s-%d", eventType, time.Now().UnixNano()),
		Type:      eventType,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Payload:   payload,
	}

	value, err := json.Marshal(event)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to marshal event")
		return
	}

	msgs := []kafka.Message{{Key: []byte(event.ID), Value: value}}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	if err := writers[topic].WriteMessages(ctx, msgs...); err != nil {
		log.Printf("[producer:%s] failed to publish event %s: %v", topic, event.ID, err)
		writeError(w, http.StatusInternalServerError, "failed to publish event")
		return
	}

	sent := msgs[0]
	log.Printf("[producer:%s] published event %s (partition=%d offset=%d)", topic, event.ID, sent.Partition, sent.Offset)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(EventResponse{
		Status:    "success",
		Partition: sent.Partition,
		Offset:    sent.Offset,
		Event:     event,
	})
}

func startConsumer(ctx context.Context, brokers []string, topic, groupID string) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: groupID,
	})
	defer reader.Close()

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("[consumer:%s] read error: %v", topic, err)
			time.Sleep(time.Second)
			continue
		}
		log.Printf("[consumer:%s] consumed event (partition=%d offset=%d): %s", topic, msg.Partition, msg.Offset, string(msg.Value))
	}
}

func main() {
	port := getEnv("PORT", "8082")
	brokers := brokerList()

	writers = map[string]*kafka.Writer{
		topicMovie:   newWriter(brokers, topicMovie),
		topicUser:    newWriter(brokers, topicUser),
		topicPayment: newWriter(brokers, topicPayment),
	}
	defer func() {
		for _, writer := range writers {
			writer.Close()
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for topic := range writers {
		go startConsumer(ctx, brokers, topic, "events-service")
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/api/events/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]bool{"status": true})
	})

	mux.HandleFunc("/api/events/movie", func(w http.ResponseWriter, r *http.Request) {
		produceEvent(w, r, "movie", topicMovie)
	})
	mux.HandleFunc("/api/events/user", func(w http.ResponseWriter, r *http.Request) {
		produceEvent(w, r, "user", topicUser)
	})
	mux.HandleFunc("/api/events/payment", func(w http.ResponseWriter, r *http.Request) {
		produceEvent(w, r, "payment", topicPayment)
	})

	log.Printf("Starting events service on port %s (brokers=%v)", port, brokers)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}
