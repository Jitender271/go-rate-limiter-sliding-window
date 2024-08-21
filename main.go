package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/gorilla/mux"
	"github.com/redis/go-redis/v9"
	"net/http"
	"sync"
	"time"
)

var ctx = context.Background()

// Redis Client Initialization
func NewRedisClient() *redis.Client {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // No password set
		DB:       0,  // Use default DB
	})

	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		fmt.Println("Failed to connect to Redis:", err)
	}
	return rdb
}

// IncrementOrderCount increases the user's order count and checks if it exceeds the limit
func IncrementOrderCount(rdb *redis.Client, userID string, limit int, windowSize time.Duration) (bool, error) {
	now := time.Now().UnixNano()
	key := fmt.Sprintf("order_count:%s", userID)

	// Remove entries outside the sliding window
	expirationTime := now - windowSize.Nanoseconds()
	_, err := rdb.ZRemRangeByScore(ctx, key, "0", fmt.Sprintf("%d", expirationTime)).Result()
	if err != nil {
		return false, err
	}

	// Get the number of requests in the current window
	count, err := rdb.ZCard(ctx, key).Result()
	if err != nil {
		return false, err
	}

	// Check if the request count exceeds the limit
	if int(count) >= limit {
		fmt.Printf("Request limit exceeded for user %s: %d requests in window\n", userID, count)
		return false, nil
	}

	// Add the current timestamp to the sorted set only if within limit
	_, err = rdb.ZAdd(ctx, key, redis.Z{
		Score:  float64(now),
		Member: userID,
	}).Result()

	if err != nil {
		return false, err
	}

	// Set expiration on the key to ensure old keys are cleaned up
	rdb.Expire(ctx, key, windowSize)
	fmt.Printf("Request allowed for user %s: %d requests in window\n", userID, count+1)

	return true, nil
}

// RequestHandler defines the interface for processing different types of requests
type RequestHandler interface {
	HandleRequest(userID string, requestData map[string]interface{}) (string, error)
}

// HandlerRegistry stores and retrieves request handlers for different request types
type HandlerRegistry struct {
	handlers map[string]RequestHandler
	mu       sync.RWMutex
}

// NewHandlerRegistry initializes a new HandlerRegistry
func NewHandlerRegistry() *HandlerRegistry {
	return &HandlerRegistry{
		handlers: make(map[string]RequestHandler),
	}
}

// RegisterHandler registers a handler for a specific request type
func (hr *HandlerRegistry) RegisterHandler(requestType string, handler RequestHandler) {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	hr.handlers[requestType] = handler
}

// GetHandler retrieves a handler for a specific request type
func (hr *HandlerRegistry) GetHandler(requestType string) (RequestHandler, bool) {
	hr.mu.RLock()
	defer hr.mu.RUnlock()
	handler, exists := hr.handlers[requestType]
	return handler, exists
}

// OrderHandler processes order-related requests and enforces the order limit
type OrderHandler struct {
	redisClient *redis.Client
	limit       int
	windowSize  time.Duration
}

func NewOrderHandler(redisClient *redis.Client, limit int, windowSize time.Duration) *OrderHandler {
	return &OrderHandler{
		redisClient: redisClient,
		limit:       limit,
		windowSize:  windowSize,
	}
}

// HandleRequest processes an order request, enforcing rate limits
func (h *OrderHandler) HandleRequest(userID string, requestData map[string]interface{}) (string, error) {
	// Check if the user has exceeded the order limit
	allowed, err := IncrementOrderCount(h.redisClient, userID, h.limit, h.windowSize)
	if err != nil {
		return "", fmt.Errorf("error checking order limit: %v", err)
	}
	if !allowed {
		return "", fmt.Errorf("429: order limit exceeded")
	}

	// Handle successful request
	// Your business logic here
	orderID := "order123" // Example order ID, you can generate or use actual business logic here
	return orderID, nil
}

// RateLimiterService combines the rate limiter and the request processing logic
type RateLimiterService struct {
	handlerRegistry *HandlerRegistry
}

func NewRateLimiterService(registry *HandlerRegistry) *RateLimiterService {
	return &RateLimiterService{
		handlerRegistry: registry,
	}
}

// HandleOrder processes an order request, enforcing rate limits
func (rls *RateLimiterService) HandleOrder(w http.ResponseWriter, r *http.Request) {
	userID := r.Header.Get("User-ID")
	if userID == "" {
		http.Error(w, "User-ID header missing", http.StatusBadRequest)
		return
	}

	var requestData map[string]interface{}
	err := json.NewDecoder(r.Body).Decode(&requestData)
	if err != nil {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}

	handler, exists := rls.handlerRegistry.GetHandler("order")
	if !exists {
		http.Error(w, "Order handler not found", http.StatusInternalServerError)
		return
	}

	result, err := handler.HandleRequest(userID, requestData)
	if err != nil {
		if err.Error() == "429: order limit exceeded" {
			http.Error(w, "429 Too Many Requests", http.StatusTooManyRequests)
		} else {
			http.Error(w, fmt.Sprintf("Error: %v", err), http.StatusInternalServerError)
		}
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf("Order processed successfully: %s", result)))
}

func main() {
	redisClient := NewRedisClient()
	registry := NewHandlerRegistry()

	orderHandler := NewOrderHandler(redisClient, 2, time.Minute)
	registry.RegisterHandler("order", orderHandler)

	rateLimiterService := NewRateLimiterService(registry)

	router := mux.NewRouter()
	router.HandleFunc("/api/v1/create/order", rateLimiterService.HandleOrder).Methods("POST")

	fmt.Println("Server starting on port 8080...")
	http.ListenAndServe(":8080", router)
}
