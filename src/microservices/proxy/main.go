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
)

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func newReverseProxy(target string) *httputil.ReverseProxy {
	u, err := url.Parse(target)
	if err != nil {
		log.Fatalf("invalid target URL %s: %v", target, err)
	}
	return httputil.NewSingleHostReverseProxy(u)
}

func main() {
	monolithURL := getEnv("MONOLITH_URL", "http://localhost:8080")
	moviesServiceURL := getEnv("MOVIES_SERVICE_URL", "http://localhost:8081")
	gradualMigration := getEnv("GRADUAL_MIGRATION", "true") == "true"
	migrationPercent, _ := strconv.Atoi(getEnv("MOVIES_MIGRATION_PERCENT", "0"))
	port := getEnv("PORT", "8000")

	monolithProxy := newReverseProxy(monolithURL)
	moviesProxy := newReverseProxy(moviesServiceURL)

	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]bool{"status": true})
	})

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		isMoviesPath := path == "/api/movies" || strings.HasPrefix(path, "/api/movies/")

		if isMoviesPath && gradualMigration && rand.Intn(100) < migrationPercent {
			log.Printf("[proxy] -> movies-service: %s %s", r.Method, path)
			moviesProxy.ServeHTTP(w, r)
			return
		}

		log.Printf("[proxy] -> monolith: %s %s", r.Method, path)
		monolithProxy.ServeHTTP(w, r)
	})

	log.Printf("Starting proxy on port %s (gradual_migration=%v, migration_percent=%d%%)",
		port, gradualMigration, migrationPercent)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}
