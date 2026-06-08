package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

type Config struct {
	Port           string
	RedisURL       string
	KafkaBrokers   []string
	TickIntervalMS int
}

func loadConfig() Config {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8002"
	}
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "localhost:6379"
	}
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9092"
	}
	tickMS := 100
	if v := os.Getenv("TICK_INTERVAL_MS"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil && parsed > 0 {
			tickMS = parsed
		}
	}
	return Config{
		Port:           port,
		RedisURL:       redisURL,
		KafkaBrokers:   strings.Split(brokers, ","),
		TickIntervalMS: tickMS,
	}
}

// ---------------------------------------------------------------------------
// Domain types
// ---------------------------------------------------------------------------

type AssetClass int

const (
	Equity AssetClass = iota
	FX
	Crypto
	Derivatives
)

type SymbolMeta struct {
	BasePrice  float64
	AssetClass AssetClass
}

type BookLevel struct {
	Price float64 `json:"price"`
	Size  float64 `json:"size"`
	Count int     `json:"count"`
}

type OrderBook struct {
	Bids      []BookLevel `json:"bids"`
	Asks      []BookLevel `json:"asks"`
	Imbalance float64     `json:"imbalance"`
	Spread    float64     `json:"spread"`
}

type Quote struct {
	Symbol        string  `json:"symbol"`
	Bid           float64 `json:"bid"`
	Ask           float64 `json:"ask"`
	BidSize       float64 `json:"bid_size"`
	AskSize       float64 `json:"ask_size"`
	LastPrice     float64 `json:"last_price"`
	LastSize      float64 `json:"last_size"`
	Volume        float64 `json:"volume"`
	VWAP          float64 `json:"vwap"`
	TWAP          float64 `json:"twap"`
	SpreadBps     float64 `json:"spread_bps"`
	ChangePct     float64 `json:"change_pct"`
	High          float64 `json:"high"`
	Low           float64 `json:"low"`
	Open          float64 `json:"open"`
	RealizedVol   float64 `json:"realized_vol"`
	ImpliedVol    float64 `json:"implied_vol"`
	BookImbalance float64 `json:"book_imbalance"`
	TimestampNs   int64   `json:"timestamp_nanos"`
}

// internal price state used by the simulator
type priceState struct {
	last       float64
	open       float64
	high       float64
	low        float64
	volume     float64
	vwapNum    float64 // cumulative price*volume
	vwapDen    float64 // cumulative volume
	twapSum    float64
	twapCount  int64
	returns    []float64 // recent log-returns for realized vol
	book       OrderBook
	lastQuote  Quote
}

// ---------------------------------------------------------------------------
// Structured JSON logger
// ---------------------------------------------------------------------------

type logEntry struct {
	Timestamp string `json:"ts"`
	Level     string `json:"level"`
	Msg       string `json:"msg"`
	TraceID   string `json:"trace_id,omitempty"`
	Symbol    string `json:"symbol,omitempty"`
	LatencyUs int64  `json:"latency_us,omitempty"`
	Extra     string `json:"extra,omitempty"`
}

func logJSON(level, msg string, fields ...string) {
	e := logEntry{
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		Level:     level,
		Msg:       msg,
	}
	for i := 0; i+1 < len(fields); i += 2 {
		switch fields[i] {
		case "trace_id":
			e.TraceID = fields[i+1]
		case "symbol":
			e.Symbol = fields[i+1]
		case "latency_us":
			e.LatencyUs, _ = strconv.ParseInt(fields[i+1], 10, 64)
		case "extra":
			e.Extra = fields[i+1]
		}
	}
	b, _ := json.Marshal(e)
	fmt.Fprintln(os.Stdout, string(b))
}

// ---------------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------------

type QuoteService struct {
	cfg    Config
	rdb    *redis.Client
	writer *kafka.Writer

	mu     sync.RWMutex
	states map[string]*priceState
	metas  map[string]SymbolMeta

	rng *rand.Rand
}

func NewQuoteService(cfg Config) *QuoteService {
	symbols := map[string]SymbolMeta{
		"AAPL": {185, Equity}, "MSFT": {420, Equity}, "GOOGL": {175, Equity},
		"AMZN": {185, Equity}, "TSLA": {245, Equity}, "JPM": {195, Equity},
		"GS": {450, Equity}, "NVDA": {880, Equity}, "META": {500, Equity},
		"SPY": {520, Equity},
		"EUR/USD": {1.0850, FX}, "GBP/USD": {1.2650, FX}, "USD/JPY": {149.50, FX},
		"BTC-USD": {67500, Crypto}, "ETH-USD": {3400, Crypto},
		"ES": {5250, Derivatives}, "NQ": {18500, Derivatives},
		"CL": {78.5, Derivatives}, "GC": {2350, Derivatives},
	}

	states := make(map[string]*priceState, len(symbols))
	for sym, meta := range symbols {
		states[sym] = &priceState{
			last: meta.BasePrice,
			open: meta.BasePrice,
			high: meta.BasePrice,
			low:  meta.BasePrice,
		}
	}

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisURL})

	writer := &kafka.Writer{
		Addr:         kafka.TCP(cfg.KafkaBrokers...),
		Topic:        "market-data",
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond,
		Async:        true,
	}

	return &QuoteService{
		cfg:    cfg,
		rdb:    rdb,
		writer: writer,
		states: states,
		metas:  symbols,
		rng:    rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// spreadBps returns a random spread in basis points based on asset class.
func (qs *QuoteService) spreadBps(ac AssetClass) float64 {
	switch ac {
	case Equity:
		return 1 + qs.rng.Float64()*4 // 1-5
	case FX:
		return 0.5 + qs.rng.Float64()*1.5 // 0.5-2
	case Crypto:
		return 5 + qs.rng.Float64()*15 // 5-20
	case Derivatives:
		return 2 + qs.rng.Float64()*6 // 2-8
	}
	return 3
}

func (qs *QuoteService) buildBook(mid float64, ac AssetClass) OrderBook {
	levels := 10
	bids := make([]BookLevel, levels)
	asks := make([]BookLevel, levels)
	halfSpread := mid * qs.spreadBps(ac) / 20000 // half spread

	var totalBidSize, totalAskSize float64
	for i := 0; i < levels; i++ {
		offset := halfSpread * float64(i+1)
		bSize := 100 + qs.rng.Float64()*900
		aSize := 100 + qs.rng.Float64()*900
		bids[i] = BookLevel{Price: roundPrice(mid - offset), Size: roundSize(bSize), Count: 1 + qs.rng.Intn(10)}
		asks[i] = BookLevel{Price: roundPrice(mid + offset), Size: roundSize(aSize), Count: 1 + qs.rng.Intn(10)}
		totalBidSize += bSize
		totalAskSize += aSize
	}

	imbalance := 0.0
	if totalBidSize+totalAskSize > 0 {
		imbalance = (totalBidSize - totalAskSize) / (totalBidSize + totalAskSize)
	}
	spread := asks[0].Price - bids[0].Price

	return OrderBook{Bids: bids, Asks: asks, Imbalance: roundPrice(imbalance), Spread: roundPrice(spread)}
}

func (qs *QuoteService) tick(ctx context.Context) {
	qs.mu.Lock()
	defer qs.mu.Unlock()

	now := time.Now().UnixNano()

	for sym, meta := range qs.metas {
		st := qs.states[sym]

		// random walk
		delta := st.last * (qs.rng.Float64()*0.002 - 0.001)
		newPrice := st.last + delta
		if newPrice <= 0 {
			newPrice = st.last
		}

		logRet := math.Log(newPrice / st.last)
		st.returns = append(st.returns, logRet)
		if len(st.returns) > 1000 {
			st.returns = st.returns[len(st.returns)-1000:]
		}

		st.last = newPrice
		if newPrice > st.high {
			st.high = newPrice
		}
		if newPrice < st.low {
			st.low = newPrice
		}

		tradeSize := 10 + qs.rng.Float64()*490
		st.volume += tradeSize
		st.vwapNum += newPrice * tradeSize
		st.vwapDen += tradeSize
		st.twapSum += newPrice
		st.twapCount++

		vwap := 0.0
		if st.vwapDen > 0 {
			vwap = st.vwapNum / st.vwapDen
		}
		twap := 0.0
		if st.twapCount > 0 {
			twap = st.twapSum / float64(st.twapCount)
		}

		book := qs.buildBook(newPrice, meta.AssetClass)
		st.book = book

		bid := book.Bids[0].Price
		ask := book.Asks[0].Price
		spreadBps := 0.0
		if newPrice > 0 {
			spreadBps = (ask - bid) / newPrice * 10000
		}

		realVol := realizedVol(st.returns)
		impliedVol := realVol * (1 + 0.05*(qs.rng.Float64()-0.5))

		changePct := 0.0
		if st.open > 0 {
			changePct = (newPrice - st.open) / st.open * 100
		}

		q := Quote{
			Symbol:        sym,
			Bid:           roundPrice(bid),
			Ask:           roundPrice(ask),
			BidSize:       roundSize(book.Bids[0].Size),
			AskSize:       roundSize(book.Asks[0].Size),
			LastPrice:     roundPrice(newPrice),
			LastSize:      roundSize(tradeSize),
			Volume:        roundSize(st.volume),
			VWAP:          roundPrice(vwap),
			TWAP:          roundPrice(twap),
			SpreadBps:     roundPrice(spreadBps),
			ChangePct:     roundPrice(changePct),
			High:          roundPrice(st.high),
			Low:           roundPrice(st.low),
			Open:          roundPrice(st.open),
			RealizedVol:   roundPrice(realVol),
			ImpliedVol:    roundPrice(impliedVol),
			BookImbalance: roundPrice(book.Imbalance),
			TimestampNs:   now,
		}
		st.lastQuote = q

		// push to Redis (best-effort)
		go qs.cacheQuote(ctx, sym, q)

		// publish to Kafka (best-effort)
		go qs.publishQuote(ctx, sym, q)
	}
}

func (qs *QuoteService) cacheQuote(ctx context.Context, sym string, q Quote) {
	b, err := json.Marshal(q)
	if err != nil {
		return
	}
	qs.rdb.Set(ctx, "quote:"+sym, b, 5*time.Second)
}

func (qs *QuoteService) publishQuote(ctx context.Context, sym string, q Quote) {
	b, err := json.Marshal(q)
	if err != nil {
		return
	}
	_ = qs.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(sym),
		Value: b,
	})
}

func realizedVol(returns []float64) float64 {
	n := len(returns)
	if n < 2 {
		return 0
	}
	var sum, sumSq float64
	for _, r := range returns {
		sum += r
		sumSq += r * r
	}
	mean := sum / float64(n)
	variance := sumSq/float64(n) - mean*mean
	if variance < 0 {
		variance = 0
	}
	// annualize: assume 100ms ticks ~ 252 trading days * 6.5h * 3600s * 10 ticks/s
	ticksPerYear := 252.0 * 6.5 * 3600 * 10
	return math.Sqrt(variance * ticksPerYear)
}

func roundPrice(v float64) float64 {
	return math.Round(v*1e6) / 1e6
}

func roundSize(v float64) float64 {
	return math.Round(v*100) / 100
}

// ---------------------------------------------------------------------------
// Market data simulator goroutine
// ---------------------------------------------------------------------------

func (qs *QuoteService) runSimulator(ctx context.Context) {
	interval := time.Duration(qs.cfg.TickIntervalMS) * time.Millisecond
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	logJSON("info", "market data simulator started", "extra", fmt.Sprintf("tick_interval_ms=%d", qs.cfg.TickIntervalMS))

	warningCounter := 0
	for {
		select {
		case <-ctx.Done():
			logJSON("info", "market data simulator stopping")
			return
		case <-ticker.C:
			qs.tick(ctx)
			warningCounter++
			// occasionally emit warnings for realism
			if warningCounter%500 == 0 {
				logJSON("warn", "market data feed delayed", "extra", "latency spike detected on upstream feed")
			}
			if warningCounter%730 == 0 {
				logJSON("warn", "quote cache miss", "extra", "redis GET returned nil for stale key")
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Kafka consumer (market-data -> quote-updates relay)
// ---------------------------------------------------------------------------

func (qs *QuoteService) runConsumer(ctx context.Context) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  qs.cfg.KafkaBrokers,
		Topic:    "market-data",
		GroupID:  "quote-service",
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	defer reader.Close()

	quoteUpdatesWriter := &kafka.Writer{
		Addr:         kafka.TCP(qs.cfg.KafkaBrokers...),
		Topic:        "quote-updates",
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond,
		Async:        true,
	}
	defer quoteUpdatesWriter.Close()

	logJSON("info", "kafka consumer started", "extra", "topic=market-data group=quote-service")

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			logJSON("error", "kafka read error", "extra", err.Error())
			time.Sleep(time.Second)
			continue
		}

		// relay to quote-updates topic
		_ = quoteUpdatesWriter.WriteMessages(ctx, kafka.Message{
			Key:   msg.Key,
			Value: msg.Value,
		})
	}
}

// ---------------------------------------------------------------------------
// HTTP handlers
// ---------------------------------------------------------------------------

func (qs *QuoteService) handleGetQuote(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	symbol := chi.URLParam(r, "symbol")
	traceID := r.Header.Get("X-Trace-ID")

	// try Redis first
	ctx := r.Context()
	cached, err := qs.rdb.Get(ctx, "quote:"+symbol).Bytes()
	if err == nil {
		w.Header().Set("Content-Type", "application/json")
		w.Write(cached)
		latUs := time.Since(start).Microseconds()
		logJSON("info", "quote request served from cache", "symbol", symbol, "latency_us", strconv.FormatInt(latUs, 10), "trace_id", traceID)
		return
	}

	// fallback to in-memory state
	qs.mu.RLock()
	st, ok := qs.states[symbol]
	if !ok {
		qs.mu.RUnlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "symbol not found", "symbol": symbol})
		logJSON("warn", "quote cache miss", "symbol", symbol, "trace_id", traceID)
		return
	}
	q := st.lastQuote
	qs.mu.RUnlock()

	latUs := time.Since(start).Microseconds()
	logJSON("info", "quote request served from memory", "symbol", symbol, "latency_us", strconv.FormatInt(latUs, 10), "trace_id", traceID)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(q)
}

func (qs *QuoteService) handleGetBook(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	symbol := chi.URLParam(r, "symbol")
	traceID := r.Header.Get("X-Trace-ID")

	qs.mu.RLock()
	st, ok := qs.states[symbol]
	if !ok {
		qs.mu.RUnlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "symbol not found", "symbol": symbol})
		return
	}
	book := st.book
	qs.mu.RUnlock()

	latUs := time.Since(start).Microseconds()
	logJSON("info", "book request", "symbol", symbol, "latency_us", strconv.FormatInt(latUs, 10), "trace_id", traceID)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(book)
}

func (qs *QuoteService) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	traceID := r.Header.Get("X-Trace-ID")

	qs.mu.RLock()
	quotes := make([]Quote, 0, len(qs.states))
	for _, st := range qs.states {
		quotes = append(quotes, st.lastQuote)
	}
	qs.mu.RUnlock()

	latUs := time.Since(start).Microseconds()
	logJSON("info", "snapshot request", "latency_us", strconv.FormatInt(latUs, 10), "trace_id", traceID)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(quotes)
}

func (qs *QuoteService) handleHealth(w http.ResponseWriter, r *http.Request) {
	status := map[string]interface{}{
		"status":    "ok",
		"service":   "quote-service",
		"timestamp": time.Now().UTC().Format(time.RFC3339Nano),
	}

	// Redis connectivity
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := qs.rdb.Ping(ctx).Err(); err != nil {
		status["redis"] = "disconnected"
		status["redis_error"] = err.Error()
	} else {
		status["redis"] = "connected"
	}

	// Kafka connectivity: attempt a brief dial
	kafkaStatus := "connected"
	for _, broker := range qs.cfg.KafkaBrokers {
		conn, err := kafka.DialContext(ctx, "tcp", broker)
		if err != nil {
			kafkaStatus = "disconnected"
			status["kafka_error"] = err.Error()
			break
		}
		conn.Close()
	}
	status["kafka"] = kafkaStatus

	if kafkaStatus == "disconnected" || status["redis"] == "disconnected" {
		status["status"] = "degraded"
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

func main() {
	cfg := loadConfig()

	qs := NewQuoteService(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Run simulator
	go qs.runSimulator(ctx)

	// Run Kafka consumer (best-effort; won't crash if Kafka is unavailable)
	go qs.runConsumer(ctx)

	// HTTP server
	r := chi.NewRouter()
	r.Use(middleware.Recoverer)
	r.Use(middleware.RealIP)

	r.Get("/quotes/{symbol}", qs.handleGetQuote)
	r.Get("/quotes/book/{symbol}", qs.handleGetBook)
	r.Get("/quotes/snapshot", qs.handleSnapshot)
	r.Get("/health", qs.handleHealth)

	srv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      r,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	go func() {
		logJSON("info", "quote-service starting", "extra", fmt.Sprintf("port=%s", cfg.Port))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen error: %v", err)
		}
	}()

	// Graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	logJSON("info", "shutting down quote-service")
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	srv.Shutdown(shutdownCtx)
	qs.writer.Close()
	qs.rdb.Close()

	logJSON("info", "quote-service stopped")
}
