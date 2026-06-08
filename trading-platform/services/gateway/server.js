const express = require('express');
const cors = require('cors');
const compression = require('compression');
const { WebSocketServer } = require('ws');
const { Kafka } = require('kafkajs');
const crypto = require('crypto');
const pino = require('pino');
const http = require('http');

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------
const PORT = parseInt(process.env.PORT, 10) || 3000;

const ORDER_SERVICE_URL = process.env.ORDER_SERVICE_URL || 'http://order-service:8080';
const QUOTE_SERVICE_URL = process.env.QUOTE_SERVICE_URL || 'http://quote-service:8080';
const ANALYTICS_SERVICE_URL = process.env.ANALYTICS_SERVICE_URL || 'http://analytics-service:8080';
const RISK_ENGINE_URL = process.env.RISK_ENGINE_URL || 'http://risk-engine:8080';
const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || 'kafka:9092').split(',');

const RATE_LIMIT_WINDOW_MS = 60_000;
const RATE_LIMIT_MAX = 1000;

// ---------------------------------------------------------------------------
// Logger
// ---------------------------------------------------------------------------
const logger = pino({
  level: process.env.LOG_LEVEL || 'info',
  timestamp: pino.stdTimeFunctions.isoTime,
});

// ---------------------------------------------------------------------------
// Metrics (in-memory, exposed as Prometheus text)
// ---------------------------------------------------------------------------
const metrics = {
  request_count: 0,
  request_latency_sum: 0,
  request_latency_buckets: { 5: 0, 10: 0, 25: 0, 50: 0, 100: 0, 250: 0, 500: 0, 1000: 0, 2500: 0, 5000: 0, Infinity: 0 },
  error_count: 0,
  active_connections: 0,
  kafka_messages_forwarded: 0,
};

function recordLatency(ms) {
  metrics.request_latency_sum += ms;
  for (const bucket of Object.keys(metrics.request_latency_buckets)) {
    if (ms <= Number(bucket)) {
      metrics.request_latency_buckets[bucket]++;
    }
  }
}

function renderMetrics() {
  const lines = [];
  lines.push('# HELP gateway_request_count Total number of requests handled');
  lines.push('# TYPE gateway_request_count counter');
  lines.push(`gateway_request_count ${metrics.request_count}`);

  lines.push('# HELP gateway_request_latency_ms_sum Sum of request latencies in ms');
  lines.push('# TYPE gateway_request_latency_ms_sum counter');
  lines.push(`gateway_request_latency_ms_sum ${metrics.request_latency_sum.toFixed(2)}`);

  lines.push('# HELP gateway_request_latency_ms Histogram of request latencies');
  lines.push('# TYPE gateway_request_latency_ms histogram');
  let cumulative = 0;
  for (const [le, count] of Object.entries(metrics.request_latency_buckets)) {
    cumulative += count;
    const label = le === 'Infinity' ? '+Inf' : le;
    lines.push(`gateway_request_latency_ms_bucket{le="${label}"} ${cumulative}`);
  }
  lines.push(`gateway_request_latency_ms_count ${metrics.request_count}`);
  lines.push(`gateway_request_latency_ms_sum ${metrics.request_latency_sum.toFixed(2)}`);

  lines.push('# HELP gateway_error_count Total number of error responses');
  lines.push('# TYPE gateway_error_count counter');
  lines.push(`gateway_error_count ${metrics.error_count}`);

  lines.push('# HELP gateway_active_ws_connections Currently connected WebSocket clients');
  lines.push('# TYPE gateway_active_ws_connections gauge');
  lines.push(`gateway_active_ws_connections ${metrics.active_connections}`);

  lines.push('# HELP gateway_kafka_messages_forwarded Total Kafka messages forwarded to WS clients');
  lines.push('# TYPE gateway_kafka_messages_forwarded counter');
  lines.push(`gateway_kafka_messages_forwarded ${metrics.kafka_messages_forwarded}`);

  return lines.join('\n') + '\n';
}

// ---------------------------------------------------------------------------
// Rate limiter (in-memory, per client IP)
// ---------------------------------------------------------------------------
const rateLimitStore = new Map();

function rateLimit(req, res, next) {
  const clientIp = req.ip || req.socket.remoteAddress;
  const now = Date.now();
  let record = rateLimitStore.get(clientIp);
  if (!record || now - record.windowStart > RATE_LIMIT_WINDOW_MS) {
    record = { windowStart: now, count: 0 };
    rateLimitStore.set(clientIp, record);
  }
  record.count++;
  if (record.count > RATE_LIMIT_MAX) {
    metrics.error_count++;
    return res.status(429).json({ error: 'Rate limit exceeded. Max 1000 requests per minute.' });
  }
  next();
}

// Periodically clean stale entries
setInterval(() => {
  const now = Date.now();
  for (const [ip, record] of rateLimitStore) {
    if (now - record.windowStart > RATE_LIMIT_WINDOW_MS * 2) {
      rateLimitStore.delete(ip);
    }
  }
}, 60_000).unref();

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function generateHex(bytes) {
  return crypto.randomBytes(bytes).toString('hex');
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ---------------------------------------------------------------------------
// Express app
// ---------------------------------------------------------------------------
const app = express();
app.set('trust proxy', true);
app.use(cors());
app.use(compression());
app.use(express.json());

// --- Tracing & logging & artificial latency middleware ---
app.use(async (req, res, next) => {
  const start = Date.now();
  const traceId = req.headers['x-trace-id'] || generateHex(16);
  const spanId = generateHex(8);
  req.traceId = traceId;
  req.spanId = spanId;

  // Artificial latency: 95% normal (0-50ms), 5% spike (500-2000ms)
  const spike = Math.random() < 0.05;
  const artificialDelay = spike
    ? 500 + Math.random() * 1500
    : Math.random() * 50;
  await sleep(artificialDelay);

  // Occasional warning logs (1% of requests)
  if (Math.random() < 0.01) {
    const warnings = [
      'high latency detected on downstream call',
      `circuit breaker half-open for ${pickRandomService()}`,
    ];
    logger.warn({ trace_id: traceId, span_id: spanId }, warnings[Math.floor(Math.random() * warnings.length)]);
  }

  res.on('finish', () => {
    const latencyMs = Date.now() - start;
    metrics.request_count++;
    recordLatency(latencyMs);
    if (res.statusCode >= 400) metrics.error_count++;

    logger.info({
      trace_id: traceId,
      span_id: spanId,
      method: req.method,
      path: req.originalUrl,
      status_code: res.statusCode,
      latency_ms: latencyMs,
      client_ip: req.ip || req.socket.remoteAddress,
      user_agent: req.headers['user-agent'] || '',
    });
  });

  next();
});

// Rate limiting
app.use('/api/', rateLimit);

// ---------------------------------------------------------------------------
// Proxy helper using native fetch (Node 20+)
// ---------------------------------------------------------------------------
async function proxyRequest(req, res, baseUrl, path) {
  const url = `${baseUrl}${path}`;
  const headers = {
    'content-type': 'application/json',
    'x-trace-id': req.traceId,
    'x-span-id': req.spanId,
    'x-forwarded-for': req.ip || req.socket.remoteAddress,
  };

  const fetchOptions = {
    method: req.method,
    headers,
    signal: AbortSignal.timeout(10_000),
  };

  if (req.method === 'POST' || req.method === 'PUT' || req.method === 'PATCH') {
    fetchOptions.body = JSON.stringify(req.body);
  }

  try {
    const upstream = await fetch(url, fetchOptions);
    const contentType = upstream.headers.get('content-type') || 'application/json';
    const body = await upstream.text();
    res.status(upstream.status).set('content-type', contentType).send(body);
  } catch (err) {
    logger.error({ trace_id: req.traceId, err: err.message, upstream_url: url }, 'Upstream request failed');
    metrics.error_count++;
    if (err.name === 'TimeoutError') {
      return res.status(504).json({ error: 'Gateway timeout', detail: `Upstream ${url} did not respond in time` });
    }
    return res.status(503).json({
      error: 'Service unavailable',
      detail: `Could not reach upstream service at ${url}`,
      message: err.message,
    });
  }
}

// ---------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------

// Orders
app.post('/api/v1/orders', (req, res) => proxyRequest(req, res, ORDER_SERVICE_URL, '/orders'));
app.get('/api/v1/orders/:id', (req, res) => proxyRequest(req, res, ORDER_SERVICE_URL, `/orders/${req.params.id}`));
app.get('/api/v1/orders', (req, res) => {
  const qs = req.originalUrl.includes('?') ? req.originalUrl.slice(req.originalUrl.indexOf('?')) : '';
  proxyRequest(req, res, ORDER_SERVICE_URL, `/orders${qs}`);
});

// Quotes
app.get('/api/v1/quotes/book/:symbol', (req, res) => proxyRequest(req, res, QUOTE_SERVICE_URL, `/quotes/book/${req.params.symbol}`));
app.get('/api/v1/quotes/:symbol', (req, res) => proxyRequest(req, res, QUOTE_SERVICE_URL, `/quotes/${req.params.symbol}`));

// Analytics
app.get('/api/v1/analytics/kpis', (req, res) => {
  const qs = req.originalUrl.includes('?') ? req.originalUrl.slice(req.originalUrl.indexOf('?')) : '';
  proxyRequest(req, res, ANALYTICS_SERVICE_URL, `/analytics/kpis${qs}`);
});
app.get('/api/v1/analytics/pnl', (req, res) => {
  const qs = req.originalUrl.includes('?') ? req.originalUrl.slice(req.originalUrl.indexOf('?')) : '';
  proxyRequest(req, res, ANALYTICS_SERVICE_URL, `/analytics/pnl${qs}`);
});

// Risk
app.get('/api/v1/risk/portfolio', (req, res) => {
  const qs = req.originalUrl.includes('?') ? req.originalUrl.slice(req.originalUrl.indexOf('?')) : '';
  proxyRequest(req, res, RISK_ENGINE_URL, `/risk/portfolio${qs}`);
});
app.get('/api/v1/risk/:clientId', (req, res) => proxyRequest(req, res, RISK_ENGINE_URL, `/risk/${req.params.clientId}`));

// Health check
app.get('/health', (_req, res) => {
  res.json({
    status: 'ok',
    uptime: process.uptime(),
    timestamp: new Date().toISOString(),
  });
});

// Prometheus-style metrics
app.get('/metrics', (_req, res) => {
  res.set('content-type', 'text/plain; version=0.0.4; charset=utf-8');
  res.send(renderMetrics());
});

// ---------------------------------------------------------------------------
// Helper to pick a random service name for chaos warnings
// ---------------------------------------------------------------------------
function pickRandomService() {
  const services = ['order-service', 'quote-service', 'analytics-service', 'risk-engine'];
  return services[Math.floor(Math.random() * services.length)];
}

// ---------------------------------------------------------------------------
// HTTP server + WebSocket
// ---------------------------------------------------------------------------
const server = http.createServer(app);

const wss = new WebSocketServer({ server, path: '/ws' });

wss.on('connection', (ws, req) => {
  metrics.active_connections++;
  const clientIp = req.headers['x-forwarded-for'] || req.socket.remoteAddress;
  logger.info({ client_ip: clientIp }, 'WebSocket client connected');

  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('close', () => {
    metrics.active_connections--;
    logger.info({ client_ip: clientIp }, 'WebSocket client disconnected');
  });

  ws.on('error', (err) => {
    logger.error({ client_ip: clientIp, err: err.message }, 'WebSocket error');
  });
});

// Heartbeat to detect dead connections
const heartbeatInterval = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, 30_000);

wss.on('close', () => clearInterval(heartbeatInterval));

// ---------------------------------------------------------------------------
// Kafka consumer - forward market-data to WebSocket clients
// ---------------------------------------------------------------------------
async function startKafkaConsumer() {
  const kafka = new Kafka({
    clientId: 'api-gateway',
    brokers: KAFKA_BROKERS,
    retry: { initialRetryTime: 3000, retries: 10 },
  });

  const consumer = kafka.consumer({ groupId: 'gateway-ws-relay' });

  try {
    await consumer.connect();
    logger.info('Kafka consumer connected');

    await consumer.subscribe({ topic: 'market-data', fromBeginning: false });

    await consumer.run({
      eachMessage: async ({ message }) => {
        const payload = message.value.toString();
        let forwarded = 0;
        wss.clients.forEach((client) => {
          if (client.readyState === 1) { // WebSocket.OPEN
            client.send(payload);
            forwarded++;
          }
        });
        metrics.kafka_messages_forwarded += forwarded;
      },
    });
  } catch (err) {
    logger.error({ err: err.message }, 'Kafka consumer failed to start - will operate without live market data relay');
  }
}

// ---------------------------------------------------------------------------
// Start
// ---------------------------------------------------------------------------
server.listen(PORT, () => {
  logger.info({ port: PORT }, 'API Gateway listening');
});

// Start Kafka consumer in background (non-blocking, tolerates Kafka being down)
startKafkaConsumer();

// Graceful shutdown
function shutdown(signal) {
  logger.info({ signal }, 'Shutting down gracefully');
  clearInterval(heartbeatInterval);
  wss.close();
  server.close(() => {
    logger.info('Server closed');
    process.exit(0);
  });
  // Force exit after 10s
  setTimeout(() => process.exit(1), 10_000).unref();
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
