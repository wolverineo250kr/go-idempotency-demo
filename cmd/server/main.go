package main

import (
	"encoding/json"
	"errors"
	"log"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/wolverineo250kr/go-idempotency-demo/internal/idempotency"
)

type createOrderRequest struct {
	ProductID int `json:"product_id"`
	Quantity  int `json:"quantity"`
}

type orderResponse struct {
	OrderID   int64  `json:"order_id"`
	ProductID int    `json:"product_id"`
	Quantity  int    `json:"quantity"`
	Status    string `json:"status"`
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	if r.status != 0 {
		return
	}
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (r *statusRecorder) Write(data []byte) (int, error) {
	if r.status == 0 {
		r.WriteHeader(http.StatusOK)
	}
	return r.ResponseWriter.Write(data)
}

func main() {
	logger := slog.New(slog.NewJSONHandler(log.Writer(), nil))
	store := idempotency.NewStore()
	var nextOrderID atomic.Int64

	mux := http.NewServeMux()

	mux.HandleFunc("POST /orders", func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
		if key == "" {
			http.Error(w, "Idempotency-Key header is required", http.StatusBadRequest)
			return
		}
		if len(key) > 200 {
			http.Error(w, "Idempotency-Key is too long", http.StatusBadRequest)
			return
		}

		var req createOrderRequest
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&req); err != nil {
			http.Error(w, "invalid JSON body", http.StatusBadRequest)
			return
		}
		if req.ProductID <= 0 || req.Quantity <= 0 {
			http.Error(w, "product_id and quantity must be positive", http.StatusBadRequest)
			return
		}

		payload, err := json.Marshal(req)
		if err != nil {
			http.Error(w, "could not encode request", http.StatusInternalServerError)
			return
		}

		result, replayed, err := store.Do(
			r.Context(),
			key,
			string(payload),
			func() (idempotency.Result, error) {
				time.Sleep(100 * time.Millisecond)

				order := orderResponse{
					OrderID:   nextOrderID.Add(1),
					ProductID: req.ProductID,
					Quantity:  req.Quantity,
					Status:    "created",
				}
				body, err := json.Marshal(order)
				if err != nil {
					return idempotency.Result{}, err
				}
				body = append(body, '\n')

				return idempotency.Result{
					Status: http.StatusCreated,
					Body:   body,
				}, nil
			},
		)
		if err != nil {
			if errors.Is(err, idempotency.ErrPayloadMismatch) {
				http.Error(w, err.Error(), http.StatusConflict)
				return
			}
			logger.Warn("idempotent request failed", "error", err)
			http.Error(w, "could not create order", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if replayed {
			w.Header().Set("Idempotency-Replayed", "true")
		}
		w.WriteHeader(result.Status)
		_, _ = w.Write(result.Body)
	})

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		recorder := &statusRecorder{ResponseWriter: w}
		mux.ServeHTTP(recorder, r)

		status := recorder.status
		if status == 0 {
			status = http.StatusOK
		}
		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}

		logger.Info("http request",
			"method", r.Method,
			"route", route,
			"status", status,
			"duration_ms", strconv.FormatFloat(float64(time.Since(start).Microseconds())/1000, 'f', 2, 64),
		)
	})

	server := &http.Server{
		Addr:              ":8080",
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}

	logger.Info("server starting", "addr", server.Addr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("server stopped", "error", err)
		log.Fatal(err)
	}
}
