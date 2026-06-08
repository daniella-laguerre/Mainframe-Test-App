package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

// ---------------------------------------------------------------------------
// Structured logger
// ---------------------------------------------------------------------------

type Logger struct{}

func (l *Logger) Info(msg string, kv ...any)  { l.log("INFO", msg, kv...) }
func (l *Logger) Warn(msg string, kv ...any)  { l.log("WARN", msg, kv...) }
func (l *Logger) Error(msg string, kv ...any) { l.log("ERROR", msg, kv...) }

func (l *Logger) log(level, msg string, kv ...any) {
	entry := map[string]any{
		"level":     level,
		"msg":       msg,
		"timestamp": time.Now().UTC().Format(time.RFC3339Nano),
		"service":   "market-data-sim",
	}
	for i := 0; i+1 < len(kv); i += 2 {
		entry[fmt.Sprint(kv[i])] = kv[i+1]
	}
	b, _ := json.Marshal(entry)
	fmt.Fprintln(os.Stdout, string(b))
}

var log = &Logger{}

// ---------------------------------------------------------------------------
// Asset classes & instrument definitions
// ---------------------------------------------------------------------------

type AssetClass int

const (
	Equity AssetClass = iota
	FX
	Crypto
	Derivatives
)

type InstrumentDef struct {
	Symbol     string
	BasePrice  float64
	Class      AssetClass
	Sigma      float64
	SpreadMin  float64
	SpreadMax  float64
	TickSize   float64
	BaseLotMin int
	BaseLotMax int
}

func instruments() []InstrumentDef {
	eq := func(sym string, px float64) InstrumentDef {
		return InstrumentDef{sym, px, Equity, 0.20, 0.01, 0.05, 0.01, 100, 1000}
	}
	fx := func(sym string, px float64) InstrumentDef {
		pip := 0.0001
		if strings.Contains(sym, "JPY") {
			pip = 0.01
		}
		return InstrumentDef{sym, px, FX, 0.08, 0.5 * pip, 2.0 * pip, pip, 100000, 5000000}
	}
	cr := func(sym string, px float64) InstrumentDef {
		return InstrumentDef{sym, px, Crypto, 0.60, 5.0, 50.0, 0.01, 1, 10}
	}
	dr := func(sym string, px float64) InstrumentDef {
		return InstrumentDef{sym, px, Derivatives, 0.15, 0.25, 1.0, 0.25, 1, 50}
	}
	return []InstrumentDef{
		eq("AAPL", 185), eq("MSFT", 420), eq("GOOGL", 175), eq("AMZN", 185),
		eq("TSLA", 245), eq("JPM", 195), eq("GS", 450), eq("NVDA", 880),
		eq("META", 500), eq("SPY", 520),
		fx("EUR/USD", 1.0850), fx("GBP/USD", 1.2650), fx("USD/JPY", 149.50),
		cr("BTC-USD", 67500), cr("ETH-USD", 3400),
		dr("ES", 5250), dr("NQ", 18500), dr("CL", 78.5), dr("GC", 2350),
	}
}

// ---------------------------------------------------------------------------
// Per-instrument live state
// ---------------------------------------------------------------------------

type BookLevel struct {
	Price float64 `json:"price"`
	Size  int     `json:"size"`
}

type InstrumentState struct {
	Def InstrumentDef

	Price     float64
	Open      float64
	High      float64
	Low       float64
	Volume    int64
	VWAPNum   float64 // cumulative price*volume
	VWAPDen   float64 // cumulative volume
	TWAPSum   float64
	TWAPCount int64

	// rolling window for realized vol
	PriceHistory []float64
	HistIdx      int
	HistFull     bool

	// event counters
	VolSpikeLeft   int
	SpreadWideLeft int
	FlashCrashLeft int
	FlashCrashBase float64
	FlashCrashDrop float64

	// auction
	AuctionLeft int

	// quote counter
	QuotesThisSec int64
	SecondStart   time.Time
	QuotesPerSec  int64

	mu sync.Mutex
}

func NewInstrumentState(def InstrumentDef) *InstrumentState {
	return &InstrumentState{
		Def:          def,
		Price:        def.BasePrice,
		Open:         def.BasePrice,
		High:         def.BasePrice,
		Low:          def.BasePrice,
		PriceHistory: make([]float64, 100),
		SecondStart:  time.Now(),
	}
}

// ---------------------------------------------------------------------------
// Price generation (geometric Brownian motion)
// ---------------------------------------------------------------------------

func (s *InstrumentState) nextTick(rng *rand.Rand, dtYears float64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	sigma := s.Def.Sigma

	// Volatility spike event
	if s.VolSpikeLeft > 0 {
		sigma *= 3.0
		s.VolSpikeLeft--
	} else if rng.Float64() < 0.001 {
		s.VolSpikeLeft = 10
		sigma *= 3.0
	}

	// Flash crash event
	if s.FlashCrashLeft > 0 {
		s.FlashCrashLeft--
		progress := 1.0 - float64(s.FlashCrashLeft)/20.0
		if progress < 0.25 {
			// sharp drop phase
			s.Price = s.FlashCrashBase * (1.0 - s.FlashCrashDrop*progress/0.25)
		} else {
			// recovery phase
			recovery := (progress - 0.25) / 0.75
			s.Price = s.FlashCrashBase * (1.0 - s.FlashCrashDrop*(1.0-recovery))
		}
	} else if rng.Float64() < 0.0002 {
		s.FlashCrashLeft = 20
		s.FlashCrashBase = s.Price
		s.FlashCrashDrop = 0.02 + rng.Float64()*0.03 // 2-5%
	} else {
		// Normal GBM
		mu := 0.0 // drift neutral for simulator
		z := rng.NormFloat64()
		s.Price = s.Price * math.Exp((mu-0.5*sigma*sigma)*dtYears+sigma*math.Sqrt(dtYears)*z)
	}

	// Round to tick size
	s.Price = math.Round(s.Price/s.Def.TickSize) * s.Def.TickSize

	if s.Price > s.High {
		s.High = s.Price
	}
	if s.Price < s.Low {
		s.Low = s.Price
	}

	// Store for realized vol
	s.PriceHistory[s.HistIdx] = s.Price
	s.HistIdx++
	if s.HistIdx >= len(s.PriceHistory) {
		s.HistIdx = 0
		s.HistFull = true
	}

	// Spread widening event
	if s.SpreadWideLeft > 0 {
		s.SpreadWideLeft--
	} else if rng.Float64() < 0.0005 {
		s.SpreadWideLeft = 5
	}

	// Auction event
	if s.AuctionLeft > 0 {
		s.AuctionLeft--
	} else if rng.Float64() < 0.0003 {
		s.AuctionLeft = 8
	}

	// Quote counter
	now := time.Now()
	if now.Sub(s.SecondStart) >= time.Second {
		s.QuotesPerSec = s.QuotesThisSec
		s.QuotesThisSec = 0
		s.SecondStart = now
	}
	s.QuotesThisSec++
}

func (s *InstrumentState) realizedVol() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	n := s.HistIdx
	if s.HistFull {
		n = len(s.PriceHistory)
	}
	if n < 2 {
		return s.Def.Sigma
	}

	// Compute log-return variance
	var sumSq float64
	count := 0
	hist := s.PriceHistory
	size := len(hist)

	start := 0
	end := s.HistIdx
	if s.HistFull {
		// full ring buffer – iterate in order
		start = s.HistIdx
		end = s.HistIdx + size
	}

	var prev float64
	for i := start; i < end; i++ {
		px := hist[i%size]
		if px <= 0 {
			continue
		}
		if i > start && prev > 0 {
			lr := math.Log(px / prev)
			sumSq += lr * lr
			count++
		}
		prev = px
	}
	if count == 0 {
		return s.Def.Sigma
	}
	// Annualize: assume ~252 trading days, ticks per day varies
	variance := sumSq / float64(count)
	return math.Sqrt(variance * 252 * 6.5 * 3600 * 10) // rough annualization
}

// ---------------------------------------------------------------------------
// Tick message construction
// ---------------------------------------------------------------------------

type TickMessage struct {
	Symbol           string      `json:"symbol"`
	Bid              float64     `json:"bid"`
	Ask              float64     `json:"ask"`
	BidSize          int         `json:"bid_size"`
	AskSize          int         `json:"ask_size"`
	LastPrice        float64     `json:"last_price"`
	LastSize         int         `json:"last_size"`
	Volume           int64       `json:"volume"`
	VWAP             float64     `json:"vwap"`
	TWAP             float64     `json:"twap"`
	SpreadBps        float64     `json:"spread_bps"`
	ChangePct        float64     `json:"change_pct"`
	High             float64     `json:"high"`
	Low              float64     `json:"low"`
	Open             float64     `json:"open"`
	RealizedVol      float64     `json:"realized_vol"`
	ImpliedVol       float64     `json:"implied_vol"`
	BookImbalance    float64     `json:"book_imbalance"`
	BookDepth        BookDepth   `json:"book_depth"`
	QuoteUpdatesPS   int64       `json:"quote_updates_per_sec"`
	TimestampNanos   int64       `json:"timestamp_nanos"`
	EventType        string      `json:"event_type"`
}

type BookDepth struct {
	Bids []BookLevel `json:"bids"`
	Asks []BookLevel `json:"asks"`
}

func buildQuoteTick(s *InstrumentState, rng *rand.Rand) TickMessage {
	s.mu.Lock()
	defer s.mu.Unlock()

	spread := s.Def.SpreadMin + rng.Float64()*(s.Def.SpreadMax-s.Def.SpreadMin)
	if s.SpreadWideLeft > 0 {
		spread *= 5.0
	}

	half := spread / 2.0
	bid := math.Round((s.Price-half)/s.Def.TickSize) * s.Def.TickSize
	ask := math.Round((s.Price+half)/s.Def.TickSize) * s.Def.TickSize
	if ask <= bid {
		ask = bid + s.Def.TickSize
	}

	bidSize := s.Def.BaseLotMin + rng.Intn(s.Def.BaseLotMax-s.Def.BaseLotMin+1)
	askSize := s.Def.BaseLotMin + rng.Intn(s.Def.BaseLotMax-s.Def.BaseLotMin+1)

	// Auction imbalance skew
	if s.AuctionLeft > 0 {
		if rng.Float64() < 0.5 {
			bidSize *= 3
		} else {
			askSize *= 3
		}
	}

	lastSize := s.Def.BaseLotMin + rng.Intn(s.Def.BaseLotMax/2)
	lastPx := bid + rng.Float64()*(ask-bid)
	lastPx = math.Round(lastPx/s.Def.TickSize) * s.Def.TickSize

	// Update volume & VWAP/TWAP
	s.Volume += int64(lastSize)
	s.VWAPNum += lastPx * float64(lastSize)
	s.VWAPDen += float64(lastSize)
	s.TWAPSum += s.Price
	s.TWAPCount++

	vwap := s.Price
	if s.VWAPDen > 0 {
		vwap = s.VWAPNum / s.VWAPDen
	}
	twap := s.Price
	if s.TWAPCount > 0 {
		twap = s.TWAPSum / float64(s.TWAPCount)
	}

	// L2 book depth – 10 levels each side with exponential size decay
	bids := make([]BookLevel, 10)
	asks := make([]BookLevel, 10)
	var totalBidSize, totalAskSize int
	for i := 0; i < 10; i++ {
		decay := math.Exp(-0.3 * float64(i))
		bLevelPx := bid - float64(i)*s.Def.TickSize
		aLevelPx := ask + float64(i)*s.Def.TickSize
		bSz := int(float64(bidSize)*decay*(0.5+rng.Float64())) + 1
		aSz := int(float64(askSize)*decay*(0.5+rng.Float64())) + 1
		bids[i] = BookLevel{Price: round(bLevelPx, 6), Size: bSz}
		asks[i] = BookLevel{Price: round(aLevelPx, 6), Size: aSz}
		totalBidSize += bSz
		totalAskSize += aSz
	}

	imbalance := 0.0
	if totalBidSize+totalAskSize > 0 {
		imbalance = float64(totalBidSize-totalAskSize) / float64(totalBidSize+totalAskSize)
	}

	spreadBps := 0.0
	mid := (bid + ask) / 2.0
	if mid > 0 {
		spreadBps = (ask - bid) / mid * 10000.0
	}

	changePct := 0.0
	if s.Open > 0 {
		changePct = (s.Price - s.Open) / s.Open * 100.0
	}

	rv := s.Def.Sigma // placeholder; real calc outside lock
	iv := rv * (1.0 + 0.05 + rng.Float64()*0.1)

	return TickMessage{
		Symbol:         s.Def.Symbol,
		Bid:            round(bid, 6),
		Ask:            round(ask, 6),
		BidSize:        bidSize,
		AskSize:        askSize,
		LastPrice:      round(lastPx, 6),
		LastSize:       lastSize,
		Volume:         s.Volume,
		VWAP:           round(vwap, 6),
		TWAP:           round(twap, 6),
		SpreadBps:      round(spreadBps, 2),
		ChangePct:      round(changePct, 4),
		High:           round(s.High, 6),
		Low:            round(s.Low, 6),
		Open:           round(s.Open, 6),
		RealizedVol:    round(rv, 4),
		ImpliedVol:     round(iv, 4),
		BookImbalance:  round(imbalance, 4),
		BookDepth:      BookDepth{Bids: bids, Asks: asks},
		QuoteUpdatesPS: s.QuotesPerSec,
		TimestampNanos: time.Now().UnixNano(),
		EventType:      "quote_update",
	}
}

func buildTradePrint(s *InstrumentState, rng *rand.Rand) TickMessage {
	s.mu.Lock()
	defer s.mu.Unlock()

	tradeSize := s.Def.BaseLotMin + rng.Intn(s.Def.BaseLotMax)
	s.Volume += int64(tradeSize)
	s.VWAPNum += s.Price * float64(tradeSize)
	s.VWAPDen += float64(tradeSize)

	vwap := s.Price
	if s.VWAPDen > 0 {
		vwap = s.VWAPNum / s.VWAPDen
	}
	twap := s.Price
	if s.TWAPCount > 0 {
		twap = s.TWAPSum / float64(s.TWAPCount)
	}

	return TickMessage{
		Symbol:         s.Def.Symbol,
		LastPrice:      round(s.Price, 6),
		LastSize:       tradeSize,
		Volume:         s.Volume,
		VWAP:           round(vwap, 6),
		TWAP:           round(twap, 6),
		ChangePct:      round((s.Price-s.Open)/s.Open*100.0, 4),
		High:           round(s.High, 6),
		Low:            round(s.Low, 6),
		Open:           round(s.Open, 6),
		TimestampNanos: time.Now().UnixNano(),
		EventType:      "trade_print",
	}
}

// ---------------------------------------------------------------------------
// Kafka producer wrapper
// ---------------------------------------------------------------------------

type KafkaProducer struct {
	writer       *kafka.Writer
	msgsSent     atomic.Int64
	bytesWritten atomic.Int64
	errors       atomic.Int64
}

func NewKafkaProducer(brokers []string, topic string) *KafkaProducer {
	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    100,
		BatchTimeout: 5 * time.Millisecond,
		Async:        true,
		RequiredAcks: kafka.RequireOne,
	}
	return &KafkaProducer{writer: w}
}

func (p *KafkaProducer) Send(ctx context.Context, key string, value []byte) error {
	err := p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(key),
		Value: value,
	})
	if err != nil {
		p.errors.Add(1)
		return err
	}
	p.msgsSent.Add(1)
	p.bytesWritten.Add(int64(len(value)))
	return nil
}

func (p *KafkaProducer) Close() error {
	return p.writer.Close()
}

// ---------------------------------------------------------------------------
// Redis cache wrapper
// ---------------------------------------------------------------------------

type RedisCache struct {
	client *redis.Client
}

func NewRedisCache(url string) *RedisCache {
	opts, err := redis.ParseURL(url)
	if err != nil {
		log.Warn("redis url parse failed, using defaults", "error", err.Error(), "url", url)
		opts = &redis.Options{Addr: "localhost:6379"}
	}
	return &RedisCache{client: redis.NewClient(opts)}
}

func (r *RedisCache) SetQuote(ctx context.Context, symbol string, data []byte) {
	key := "quote:" + symbol
	r.client.Set(ctx, key, data, 30*time.Second)
}

func (r *RedisCache) Close() error {
	return r.client.Close()
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func round(v float64, decimals int) float64 {
	pow := math.Pow(10, float64(decimals))
	return math.Round(v*pow) / pow
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

func main() {
	log.Info("market data simulator starting",
		"version", "1.0.0",
		"go_version", "1.21",
	)

	// Configuration
	tickIntervalMs, _ := strconv.Atoi(envOr("TICK_INTERVAL_MS", "100"))
	tickInterval := time.Duration(tickIntervalMs) * time.Millisecond
	kafkaBrokers := strings.Split(envOr("KAFKA_BROKERS", "localhost:9092"), ",")
	redisURL := envOr("REDIS_URL", "redis://localhost:6379")

	log.Info("configuration loaded",
		"tick_interval_ms", tickIntervalMs,
		"kafka_brokers", strings.Join(kafkaBrokers, ","),
		"redis_url", redisURL,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize Kafka producer
	producer := NewKafkaProducer(kafkaBrokers, "market-data")
	defer func() {
		if err := producer.Close(); err != nil {
			log.Error("kafka producer close error", "error", err.Error())
		}
	}()

	// Initialize Redis cache
	cache := NewRedisCache(redisURL)
	defer func() {
		if err := cache.Close(); err != nil {
			log.Error("redis close error", "error", err.Error())
		}
	}()

	// Build instrument states
	defs := instruments()
	states := make([]*InstrumentState, len(defs))
	for i, d := range defs {
		states[i] = NewInstrumentState(d)
	}

	log.Info("instruments initialized", "count", len(states))

	// Graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Metrics reporting goroutine
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		reconnectTicker := time.NewTicker(47 * time.Second) // occasional warning
		defer reconnectTicker.Stop()
		delayTicker := time.NewTicker(73 * time.Second)
		defer delayTicker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				log.Info("producer metrics",
					"msgs_sent", producer.msgsSent.Load(),
					"bytes_written", producer.bytesWritten.Load(),
					"errors", producer.errors.Load(),
					"tick_interval_ms", tickIntervalMs,
				)
				// Log per-instrument tick rates
				for _, s := range states {
					s.mu.Lock()
					qps := s.QuotesPerSec
					vol := s.Volume
					s.mu.Unlock()
					if qps > 0 {
						log.Info("instrument tick rate",
							"symbol", s.Def.Symbol,
							"quotes_per_sec", qps,
							"volume", vol,
						)
					}
				}
			case <-reconnectTicker.C:
				log.Warn("market data feed reconnecting",
					"reason", "periodic_health_check",
					"broker", kafkaBrokers[0],
				)
			case <-delayTicker.C:
				log.Warn("tick processing delayed",
					"delay_us", 150+rand.Intn(500),
					"queue_depth", 10+rand.Intn(90),
				)
			}
		}
	}()

	// Main tick loop
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	dtYears := tickInterval.Seconds() / (252.0 * 6.5 * 3600.0) // fraction of trading year

	log.Info("tick loop started", "dt_years", dtYears)

	tickCount := int64(0)
	for {
		select {
		case <-sigCh:
			log.Info("shutdown signal received", "total_ticks", tickCount)
			cancel()
			return
		case <-ticker.C:
			tickCount++

			for _, s := range states {
				s.nextTick(rng, dtYears)

				// Build and publish quote update
				tick := buildQuoteTick(s, rng)

				// Update realized vol outside instrument lock
				tick.RealizedVol = round(s.realizedVol(), 4)
				tick.ImpliedVol = round(tick.RealizedVol*(1.0+0.05+rng.Float64()*0.1), 4)

				data, err := json.Marshal(tick)
				if err != nil {
					log.Error("marshal error", "symbol", s.Def.Symbol, "error", err.Error())
					continue
				}

				if err := producer.Send(ctx, s.Def.Symbol, data); err != nil {
					log.Error("kafka send error", "symbol", s.Def.Symbol, "error", err.Error())
				}

				// Cache latest quote in Redis
				cache.SetQuote(ctx, s.Def.Symbol, data)

				// Random trade prints (~20% of ticks)
				if rng.Float64() < 0.20 {
					trade := buildTradePrint(s, rng)
					tdata, err := json.Marshal(trade)
					if err == nil {
						if err := producer.Send(ctx, s.Def.Symbol+".trade", tdata); err != nil {
							log.Error("kafka trade send error", "symbol", s.Def.Symbol, "error", err.Error())
						}
					}
				}
			}
		}
	}
}
