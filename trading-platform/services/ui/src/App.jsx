import React, { useState, useEffect, useRef, useCallback } from 'react';

// ── Color palette ──
const C = {
  bg: '#1a1a2e', panel: '#16213e', panelLight: '#1a2744',
  text: '#e0e0e0', textDim: '#8899aa', green: '#00ff88', red: '#ff4444',
  accent: '#0066ff', border: '#2a3a5e', header: '#0f1629',
  yellowFlash: '#3a3a00',
};

// ── Helpers ──
const fmt = (n, d = 2) => n == null ? '--' : Number(n).toFixed(d);
const fmtPct = (n) => n == null ? '--' : (n >= 0 ? '+' : '') + Number(n).toFixed(2) + '%';
const fmtVol = (n) => n == null ? '--' : n >= 1e6 ? (n / 1e6).toFixed(1) + 'M' : n >= 1e3 ? (n / 1e3).toFixed(1) + 'K' : String(n);
const fmtTime = (t) => t ? new Date(t).toLocaleTimeString('en-US', { hour12: false, fractionalSecondDigits: 3 }) : '--';

async function apiFetch(url) {
  try {
    const r = await fetch(url);
    if (!r.ok) return null;
    return await r.json();
  } catch { return null; }
}

// ── Styles ──
const S = {
  app: { display: 'flex', flexDirection: 'column', height: '100vh', background: C.bg, color: C.text, fontFamily: "'Consolas','Monaco','Courier New',monospace", fontSize: 12 },
  header: { display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '6px 16px', background: C.header, borderBottom: `1px solid ${C.border}`, minHeight: 40 },
  headerTitle: { fontSize: 15, fontWeight: 700, color: '#fff', letterSpacing: 1 },
  headerRight: { display: 'flex', alignItems: 'center', gap: 20, fontSize: 11 },
  dot: (on) => ({ width: 8, height: 8, borderRadius: '50%', background: on ? C.green : C.red, display: 'inline-block', marginRight: 6, boxShadow: on ? `0 0 6px ${C.green}` : 'none' }),
  mainGrid: { display: 'grid', gridTemplateColumns: '2fr 1.5fr 1.5fr', gap: 2, flex: 1, overflow: 'hidden', padding: 2 },
  panel: { background: C.panel, border: `1px solid ${C.border}`, borderRadius: 4, display: 'flex', flexDirection: 'column', overflow: 'hidden' },
  panelHead: { padding: '6px 10px', borderBottom: `1px solid ${C.border}`, fontSize: 11, fontWeight: 700, color: C.accent, textTransform: 'uppercase', letterSpacing: 1, display: 'flex', justifyContent: 'space-between', alignItems: 'center' },
  panelBody: { flex: 1, overflow: 'auto', padding: 4 },
  table: { width: '100%', borderCollapse: 'collapse', fontSize: 11 },
  th: { padding: '4px 6px', textAlign: 'left', color: C.textDim, borderBottom: `1px solid ${C.border}`, fontSize: 10, fontWeight: 600, position: 'sticky', top: 0, background: C.panel, zIndex: 1 },
  td: { padding: '3px 6px', borderBottom: `1px solid ${C.border}22`, whiteSpace: 'nowrap' },
  bottomTabs: { borderTop: `1px solid ${C.border}`, background: C.panel },
  tabBar: { display: 'flex', borderBottom: `1px solid ${C.border}` },
  tab: (active) => ({ padding: '6px 16px', cursor: 'pointer', fontSize: 11, fontWeight: 600, color: active ? C.accent : C.textDim, borderBottom: active ? `2px solid ${C.accent}` : '2px solid transparent', background: 'transparent', border: 'none', fontFamily: 'inherit' }),
  tabContent: { height: 200, overflow: 'auto', padding: 8 },
  btn: (variant) => ({ padding: '6px 12px', border: 'none', borderRadius: 3, cursor: 'pointer', fontWeight: 700, fontSize: 11, fontFamily: 'inherit', color: '#fff', background: variant === 'buy' ? C.green + 'cc' : variant === 'sell' ? C.red + 'cc' : C.accent, transition: 'opacity .15s' }),
  input: { padding: '5px 8px', background: C.bg, border: `1px solid ${C.border}`, borderRadius: 3, color: C.text, fontSize: 11, fontFamily: 'inherit', width: '100%' },
  select: { padding: '5px 8px', background: C.bg, border: `1px solid ${C.border}`, borderRadius: 3, color: C.text, fontSize: 11, fontFamily: 'inherit', width: '100%' },
  kpiCard: { background: C.panelLight, border: `1px solid ${C.border}`, borderRadius: 4, padding: '10px 14px', textAlign: 'center' },
  kpiValue: { fontSize: 20, fontWeight: 700 },
  kpiLabel: { fontSize: 10, color: C.textDim, marginTop: 2 },
  flashRow: { background: C.yellowFlash, transition: 'background 0.4s ease' },
};

export default function App() {
  // ── State ──
  const [time, setTime] = useState(Date.now());
  const [wsConnected, setWsConnected] = useState(false);
  const [msgRate, setMsgRate] = useState(0);
  const [quotes, setQuotes] = useState([]);
  const [flashSymbols, setFlashSymbols] = useState(new Set());
  const [selectedSymbol, setSelectedSymbol] = useState(null);
  const [orderBook, setOrderBook] = useState(null);
  const [bottomTab, setBottomTab] = useState(0);
  const [kpis, setKpis] = useState(null);
  const [risk, setRisk] = useState(null);
  const [trades, setTrades] = useState([]);
  const [health, setHealth] = useState(null);
  const [recentOrders, setRecentOrders] = useState([]);
  const [orderForm, setOrderForm] = useState({ symbol: '', side: 'buy', type: 'market', quantity: '100', price: '', algo: 'DMA' });

  const wsRef = useRef(null);
  const msgCountRef = useRef(0);
  const prevPricesRef = useRef({});

  // ── Clock ──
  useEffect(() => {
    const id = setInterval(() => setTime(Date.now()), 100);
    return () => clearInterval(id);
  }, []);

  // ── WebSocket ──
  useEffect(() => {
    let ws, retryTimer, rateTimer;
    const connect = () => {
      const proto = window.location.protocol === 'https:' ? 'wss' : 'ws';
      const url = window.location.hostname === 'localhost'
        ? `ws://localhost:3000/ws`
        : `${proto}://${window.location.host}/ws`;
      ws = new WebSocket(url);
      wsRef.current = ws;
      ws.onopen = () => setWsConnected(true);
      ws.onclose = () => { setWsConnected(false); retryTimer = setTimeout(connect, 3000); };
      ws.onerror = () => ws.close();
      ws.onmessage = (evt) => {
        msgCountRef.current++;
        try {
          const msg = JSON.parse(evt.data);
          if (msg.type === 'quote' || msg.type === 'market_data' || msg.symbol) {
            setQuotes(prev => {
              const idx = prev.findIndex(q => q.symbol === msg.symbol);
              const updated = [...prev];
              if (idx >= 0) updated[idx] = { ...updated[idx], ...msg };
              else updated.push(msg);
              return updated;
            });
            setFlashSymbols(prev => new Set(prev).add(msg.symbol));
            setTimeout(() => setFlashSymbols(prev => { const s = new Set(prev); s.delete(msg.symbol); return s; }), 400);
          }
        } catch {}
      };
    };
    connect();
    rateTimer = setInterval(() => { setMsgRate(msgCountRef.current); msgCountRef.current = 0; }, 1000);
    return () => { ws && ws.close(); clearTimeout(retryTimer); clearInterval(rateTimer); };
  }, []);

  // ── Polling: quotes snapshot ──
  useEffect(() => {
    const poll = async () => {
      const data = await apiFetch('/api/quotes/snapshot');
      if (data && Array.isArray(data)) {
        const prevMap = prevPricesRef.current;
        const flashSet = new Set();
        data.forEach(q => {
          if (prevMap[q.symbol] && prevMap[q.symbol] !== q.last) flashSet.add(q.symbol);
          prevMap[q.symbol] = q.last;
        });
        if (flashSet.size > 0) {
          setFlashSymbols(prev => new Set([...prev, ...flashSet]));
          setTimeout(() => setFlashSymbols(prev => { const s = new Set(prev); flashSet.forEach(sym => s.delete(sym)); return s; }), 400);
        }
        setQuotes(prev => {
          const map = {};
          prev.forEach(q => map[q.symbol] = q);
          data.forEach(q => map[q.symbol] = { ...map[q.symbol], ...q });
          return Object.values(map);
        });
      }
    };
    poll();
    const id = setInterval(poll, 1000);
    return () => clearInterval(id);
  }, []);

  // ── Polling: order book for selected symbol ──
  useEffect(() => {
    if (!selectedSymbol) return;
    const poll = async () => {
      const data = await apiFetch(`/api/quotes/book/${selectedSymbol}`);
      if (data) setOrderBook(data);
    };
    poll();
    const id = setInterval(poll, 500);
    return () => clearInterval(id);
  }, [selectedSymbol]);

  // ── Polling: bottom tabs (only when visible) ──
  useEffect(() => {
    let id;
    const poll = async () => {
      if (bottomTab === 0) {
        const data = await apiFetch('/api/v1/analytics/kpis');
        if (data) setKpis(data);
      } else if (bottomTab === 1) {
        const data = await apiFetch('/api/v1/risk/portfolio');
        if (data) setRisk(data);
      } else if (bottomTab === 2) {
        const data = await apiFetch('/api/v1/trades?limit=50');
        if (data) setTrades(Array.isArray(data) ? data : data.trades || []);
      } else if (bottomTab === 3) {
        const data = await apiFetch('/health');
        if (data) setHealth(data);
      }
    };
    poll();
    id = setInterval(poll, bottomTab === 0 ? 5000 : 3000);
    return () => clearInterval(id);
  }, [bottomTab]);

  // ── Auto-select first symbol ──
  useEffect(() => {
    if (!selectedSymbol && quotes.length > 0) setSelectedSymbol(quotes[0].symbol);
  }, [quotes, selectedSymbol]);

  // ── Submit order ──
  const submitOrder = useCallback(async () => {
    const sym = orderForm.symbol || selectedSymbol;
    if (!sym) return;
    const body = {
      symbol: sym,
      side: orderForm.side,
      type: orderForm.type,
      quantity: Number(orderForm.quantity) || 100,
      price: orderForm.type === 'market' ? undefined : Number(orderForm.price) || undefined,
      algo_strategy: orderForm.algo,
    };
    try {
      const r = await fetch('/api/v1/orders', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
      const data = await r.json();
      setRecentOrders(prev => [{ ...body, id: data.order_id || data.id || Date.now(), status: data.status || 'submitted', time: new Date().toISOString() }, ...prev].slice(0, 20));
    } catch (e) {
      setRecentOrders(prev => [{ ...body, id: Date.now(), status: 'error', time: new Date().toISOString() }, ...prev].slice(0, 20));
    }
  }, [orderForm, selectedSymbol]);

  // ── Compute total P&L ──
  const totalPnl = quotes.reduce((sum, q) => sum + (q.pnl || 0), 0);

  // ── Render helpers ──
  const colorVal = (v) => v > 0 ? C.green : v < 0 ? C.red : C.text;

  const symbols = quotes.map(q => q.symbol).filter(Boolean);

  return (
    <div style={S.app}>
      {/* ════ HEADER ════ */}
      <div style={S.header}>
        <span style={S.headerTitle}>TRADING PLATFORM - LIVE DASHBOARD</span>
        <div style={S.headerRight}>
          <span><span style={S.dot(wsConnected)} /> {wsConnected ? 'CONNECTED' : 'DISCONNECTED'}</span>
          <span style={{ color: C.textDim }}>{msgRate} msg/s</span>
          <span style={{ color: C.textDim }}>{new Date(time).toLocaleTimeString('en-US', { hour12: false, fractionalSecondDigits: 3 })}</span>
          <span style={{ color: colorVal(totalPnl), fontWeight: 700 }}>P&L: {totalPnl >= 0 ? '+' : ''}{fmt(totalPnl)}</span>
        </div>
      </div>

      {/* ════ MAIN GRID ════ */}
      <div style={S.mainGrid}>
        {/* ── Market Data Panel ── */}
        <div style={S.panel}>
          <div style={S.panelHead}>
            <span>Market Data</span>
            <span style={{ fontWeight: 400, color: C.textDim }}>{quotes.length} instruments</span>
          </div>
          <div style={S.panelBody}>
            <table style={S.table}>
              <thead>
                <tr>
                  {['Symbol', 'Bid', 'Ask', 'Last', 'Chg%', 'Volume', 'Sprd(bps)', 'Imbal'].map(h => (
                    <th key={h} style={S.th}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {quotes.map(q => {
                  const isFlash = flashSymbols.has(q.symbol);
                  const isSelected = q.symbol === selectedSymbol;
                  return (
                    <tr key={q.symbol}
                      onClick={() => setSelectedSymbol(q.symbol)}
                      style={{
                        cursor: 'pointer',
                        background: isFlash ? C.yellowFlash : isSelected ? C.border + '44' : 'transparent',
                        transition: 'background 0.3s',
                      }}>
                      <td style={{ ...S.td, color: C.accent, fontWeight: 700 }}>{q.symbol}</td>
                      <td style={{ ...S.td, color: C.green }}>{fmt(q.bid, q.bid > 1000 ? 2 : 4)}</td>
                      <td style={{ ...S.td, color: C.red }}>{fmt(q.ask, q.ask > 1000 ? 2 : 4)}</td>
                      <td style={{ ...S.td, fontWeight: 700 }}>{fmt(q.last, q.last > 1000 ? 2 : 4)}</td>
                      <td style={{ ...S.td, color: colorVal(q.change_pct) }}>{fmtPct(q.change_pct)}</td>
                      <td style={S.td}>{fmtVol(q.volume)}</td>
                      <td style={S.td}>{fmt(q.spread_bps, 1)}</td>
                      <td style={{ ...S.td, color: colorVal(q.imbalance) }}>{fmt(q.imbalance, 2)}</td>
                    </tr>
                  );
                })}
                {quotes.length === 0 && (
                  <tr><td colSpan={8} style={{ ...S.td, textAlign: 'center', color: C.textDim, padding: 20 }}>Waiting for market data...</td></tr>
                )}
              </tbody>
            </table>
          </div>
        </div>

        {/* ── Order Book Panel ── */}
        <div style={S.panel}>
          <div style={S.panelHead}>
            <span>Order Book {selectedSymbol ? `- ${selectedSymbol}` : ''}</span>
          </div>
          <div style={S.panelBody}>
            {orderBook ? (
              <OrderBookView book={orderBook} />
            ) : (
              <div style={{ padding: 20, textAlign: 'center', color: C.textDim }}>
                {selectedSymbol ? 'Loading order book...' : 'Select a symbol'}
              </div>
            )}
          </div>
        </div>

        {/* ── Order Entry Panel ── */}
        <div style={S.panel}>
          <div style={S.panelHead}><span>Order Entry</span></div>
          <div style={S.panelBody}>
            <div style={{ padding: 8, display: 'flex', flexDirection: 'column', gap: 6 }}>
              <select style={S.select} value={orderForm.symbol || selectedSymbol || ''} onChange={e => setOrderForm(f => ({ ...f, symbol: e.target.value }))}>
                <option value="">Select Symbol</option>
                {symbols.map(s => <option key={s} value={s}>{s}</option>)}
              </select>

              <div style={{ display: 'flex', gap: 4 }}>
                <button style={{ ...S.btn('buy'), flex: 1, opacity: orderForm.side === 'buy' ? 1 : 0.4 }} onClick={() => setOrderForm(f => ({ ...f, side: 'buy' }))}>BUY</button>
                <button style={{ ...S.btn('sell'), flex: 1, opacity: orderForm.side === 'sell' ? 1 : 0.4 }} onClick={() => setOrderForm(f => ({ ...f, side: 'sell' }))}>SELL</button>
              </div>

              <div style={{ display: 'flex', gap: 4 }}>
                {['market', 'limit', 'stop'].map(t => (
                  <button key={t} style={{ ...S.btn(null), flex: 1, opacity: orderForm.type === t ? 1 : 0.4, fontSize: 10, padding: '4px 6px' }} onClick={() => setOrderForm(f => ({ ...f, type: t }))}>{t.toUpperCase()}</button>
                ))}
              </div>

              <div style={{ display: 'flex', gap: 4 }}>
                <div style={{ flex: 1 }}>
                  <label style={{ fontSize: 9, color: C.textDim }}>Quantity</label>
                  <input style={S.input} type="number" value={orderForm.quantity} onChange={e => setOrderForm(f => ({ ...f, quantity: e.target.value }))} />
                </div>
                {orderForm.type !== 'market' && (
                  <div style={{ flex: 1 }}>
                    <label style={{ fontSize: 9, color: C.textDim }}>Price</label>
                    <input style={S.input} type="number" step="0.01" value={orderForm.price} onChange={e => setOrderForm(f => ({ ...f, price: e.target.value }))} />
                  </div>
                )}
              </div>

              <div>
                <label style={{ fontSize: 9, color: C.textDim }}>Algo Strategy</label>
                <select style={S.select} value={orderForm.algo} onChange={e => setOrderForm(f => ({ ...f, algo: e.target.value }))}>
                  {['DMA', 'VWAP', 'TWAP', 'POV', 'Iceberg', 'Sniper'].map(a => <option key={a} value={a}>{a}</option>)}
                </select>
              </div>

              <button style={{ ...S.btn(orderForm.side), width: '100%', padding: '8px', fontSize: 13 }} onClick={submitOrder}>
                SUBMIT {orderForm.side.toUpperCase()} ORDER
              </button>
            </div>

            {/* Recent Orders */}
            <div style={{ borderTop: `1px solid ${C.border}`, marginTop: 6 }}>
              <div style={{ padding: '4px 8px', fontSize: 10, color: C.textDim, fontWeight: 600 }}>RECENT ORDERS ({recentOrders.length})</div>
              <div style={{ maxHeight: 200, overflow: 'auto' }}>
                {recentOrders.map((o, i) => (
                  <div key={o.id || i} style={{ padding: '3px 8px', borderBottom: `1px solid ${C.border}22`, fontSize: 10, display: 'flex', justifyContent: 'space-between' }}>
                    <span style={{ color: o.side === 'buy' ? C.green : C.red }}>{o.side?.toUpperCase()} {o.quantity} {o.symbol}</span>
                    <span style={{ color: o.status === 'error' ? C.red : C.textDim }}>{o.status}</span>
                  </div>
                ))}
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* ════ BOTTOM TABS ════ */}
      <div style={S.bottomTabs}>
        <div style={S.tabBar}>
          {['Business KPIs', 'Risk Dashboard', 'Recent Trades', 'System Health'].map((label, i) => (
            <button key={label} style={S.tab(bottomTab === i)} onClick={() => setBottomTab(i)}>{label}</button>
          ))}
        </div>
        <div style={S.tabContent}>
          {bottomTab === 0 && <KPIsTab kpis={kpis} />}
          {bottomTab === 1 && <RiskTab risk={risk} />}
          {bottomTab === 2 && <TradesTab trades={trades} />}
          {bottomTab === 3 && <HealthTab health={health} />}
        </div>
      </div>
    </div>
  );
}

// ════════════════════════════════════════════
// Sub-components
// ════════════════════════════════════════════

function OrderBookView({ book }) {
  const bids = book.bids || [];
  const asks = (book.asks || []).slice().reverse();
  const maxSize = Math.max(...[...bids, ...asks].map(l => l.size || l.quantity || l[1] || 0), 1);

  const renderLevel = (level, side) => {
    const price = level.price || level[0] || 0;
    const size = level.size || level.quantity || level[1] || 0;
    const pct = (size / maxSize) * 100;
    const color = side === 'bid' ? C.green : C.red;
    return (
      <div key={`${side}-${price}`} style={{ display: 'flex', alignItems: 'center', fontSize: 11, padding: '1px 6px', position: 'relative' }}>
        <div style={{ position: 'absolute', [side === 'bid' ? 'right' : 'left']: 0, top: 0, bottom: 0, width: `${pct}%`, background: color + '18', transition: 'width 0.3s' }} />
        <span style={{ flex: 1, textAlign: 'right', color, zIndex: 1 }}>{fmt(price, price > 1000 ? 2 : 4)}</span>
        <span style={{ flex: 1, textAlign: 'right', color: C.textDim, zIndex: 1 }}>{fmtVol(size)}</span>
      </div>
    );
  };

  const bestBid = bids[0] ? (bids[0].price || bids[0][0]) : 0;
  const bestAsk = asks.length ? (asks[asks.length - 1]?.price || asks[asks.length - 1]?.[0]) : 0;
  const spread = bestAsk && bestBid ? bestAsk - bestBid : 0;
  const spreadBps = bestBid ? (spread / bestBid * 10000).toFixed(1) : '--';

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', padding: '2px 6px', fontSize: 9, color: C.textDim, borderBottom: `1px solid ${C.border}` }}>
        <span>PRICE</span><span>SIZE</span>
      </div>
      <div style={{ flex: 1, overflow: 'auto' }}>
        {asks.map(l => renderLevel(l, 'ask'))}
      </div>
      <div style={{ padding: '4px 8px', textAlign: 'center', background: C.bg, fontSize: 11, fontWeight: 700, borderTop: `1px solid ${C.border}`, borderBottom: `1px solid ${C.border}` }}>
        Spread: {fmt(spread, 4)} ({spreadBps} bps)
      </div>
      <div style={{ flex: 1, overflow: 'auto' }}>
        {bids.map(l => renderLevel(l, 'bid'))}
      </div>
    </div>
  );
}

function KPIsTab({ kpis }) {
  if (!kpis) return <div style={{ color: C.textDim, padding: 20, textAlign: 'center' }}>Loading KPIs...</div>;
  const cards = [
    { label: 'Fill Rate', value: fmtPct(kpis.fill_rate).replace('+', ''), color: C.green },
    { label: 'Avg Slippage', value: fmt(kpis.avg_slippage, 3) + ' bps', color: kpis.avg_slippage > 1 ? C.red : C.green },
    { label: 'Reject Rate', value: fmtPct(kpis.reject_rate).replace('+', ''), color: kpis.reject_rate > 5 ? C.red : C.green },
    { label: 'Total Volume', value: fmtVol(kpis.total_volume), color: C.accent },
    { label: 'Revenue/min', value: '$' + fmt(kpis.revenue_per_min || kpis.revenue_per_minute, 0), color: C.green },
    { label: 'P99 Latency', value: fmt(kpis.p99_latency || kpis.p99_latency_ms, 1) + ' ms', color: (kpis.p99_latency || kpis.p99_latency_ms) > 100 ? C.red : C.green },
  ];
  return (
    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(6, 1fr)', gap: 8 }}>
      {cards.map(c => (
        <div key={c.label} style={S.kpiCard}>
          <div style={{ ...S.kpiValue, color: c.color }}>{c.value}</div>
          <div style={S.kpiLabel}>{c.label}</div>
        </div>
      ))}
    </div>
  );
}

function RiskTab({ risk }) {
  if (!risk) return <div style={{ color: C.textDim, padding: 20, textAlign: 'center' }}>Loading risk data...</div>;
  return (
    <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: 12 }}>
      <div>
        <div style={{ fontSize: 11, fontWeight: 700, color: C.accent, marginBottom: 6 }}>PORTFOLIO RISK</div>
        <div style={{ fontSize: 10, lineHeight: 1.8 }}>
          <div>VaR (95%): <span style={{ color: C.red, fontWeight: 700 }}>${fmt(risk.var_95 || risk.var, 0)}</span></div>
          <div>VaR (99%): <span style={{ color: C.red, fontWeight: 700 }}>${fmt(risk.var_99, 0)}</span></div>
          <div>Expected Shortfall: <span style={{ color: C.red }}>${fmt(risk.expected_shortfall || risk.es, 0)}</span></div>
          <div>Sharpe Ratio: <span style={{ color: C.green }}>{fmt(risk.sharpe_ratio, 2)}</span></div>
          <div>Max Drawdown: <span style={{ color: C.red }}>{fmtPct(risk.max_drawdown)}</span></div>
        </div>
      </div>
      <div>
        <div style={{ fontSize: 11, fontWeight: 700, color: C.accent, marginBottom: 6 }}>EXPOSURE BREAKDOWN</div>
        {(risk.exposures || risk.exposure_breakdown || []).map((e, i) => (
          <div key={i} style={{ display: 'flex', justifyContent: 'space-between', fontSize: 10, padding: '2px 0', borderBottom: `1px solid ${C.border}22` }}>
            <span>{e.asset_class || e.name || e.sector}</span>
            <span style={{ color: (e.exposure || e.value) >= 0 ? C.green : C.red }}>${fmtVol(Math.abs(e.exposure || e.value || e.net))}</span>
          </div>
        ))}
      </div>
      <div>
        <div style={{ fontSize: 11, fontWeight: 700, color: C.accent, marginBottom: 6 }}>TOP CONCENTRATIONS</div>
        {(risk.top_concentrations || risk.concentrations || []).slice(0, 8).map((c, i) => (
          <div key={i} style={{ display: 'flex', justifyContent: 'space-between', fontSize: 10, padding: '2px 0', borderBottom: `1px solid ${C.border}22` }}>
            <span style={{ color: C.accent }}>{c.symbol || c.name}</span>
            <span>{fmtPct(c.weight || c.pct)}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

function TradesTab({ trades }) {
  if (!trades.length) return <div style={{ color: C.textDim, padding: 20, textAlign: 'center' }}>No recent trades</div>;
  return (
    <table style={S.table}>
      <thead>
        <tr>
          {['Time', 'Symbol', 'Side', 'Qty', 'Price', 'Venue', 'Slippage', 'Fees'].map(h => (
            <th key={h} style={S.th}>{h}</th>
          ))}
        </tr>
      </thead>
      <tbody>
        {trades.map((t, i) => (
          <tr key={t.id || i}>
            <td style={{ ...S.td, color: C.textDim }}>{fmtTime(t.timestamp || t.time || t.executed_at)}</td>
            <td style={{ ...S.td, color: C.accent }}>{t.symbol}</td>
            <td style={{ ...S.td, color: t.side === 'buy' ? C.green : C.red }}>{t.side?.toUpperCase()}</td>
            <td style={S.td}>{fmtVol(t.quantity || t.qty)}</td>
            <td style={S.td}>{fmt(t.price, t.price > 1000 ? 2 : 4)}</td>
            <td style={{ ...S.td, color: C.textDim }}>{t.venue || '--'}</td>
            <td style={{ ...S.td, color: (t.slippage || 0) > 0.5 ? C.red : C.green }}>{fmt(t.slippage, 3)} bps</td>
            <td style={S.td}>${fmt(t.fees || t.commission, 2)}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function HealthTab({ health }) {
  if (!health) return <div style={{ color: C.textDim, padding: 20, textAlign: 'center' }}>Loading health data...</div>;
  const status = health.status || health.state;
  const isHealthy = status === 'healthy' || status === 'ok' || status === 'up';
  return (
    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 12 }}>
      <div style={S.kpiCard}>
        <div style={{ ...S.kpiValue, color: isHealthy ? C.green : C.red }}>{isHealthy ? 'HEALTHY' : 'DEGRADED'}</div>
        <div style={S.kpiLabel}>Gateway Status</div>
      </div>
      <div style={S.kpiCard}>
        <div style={{ ...S.kpiValue, color: C.accent }}>{fmt(health.latency_ms || health.avg_latency || health.p50_latency, 1)} ms</div>
        <div style={S.kpiLabel}>Avg Latency</div>
      </div>
      <div style={S.kpiCard}>
        <div style={{ ...S.kpiValue, color: (health.error_rate || 0) > 1 ? C.red : C.green }}>{fmtPct(health.error_rate)}</div>
        <div style={S.kpiLabel}>Error Rate</div>
      </div>
      <div style={S.kpiCard}>
        <div style={{ ...S.kpiValue, color: C.accent }}>{health.active_connections || health.connections || '--'}</div>
        <div style={S.kpiLabel}>Active Connections</div>
      </div>
      {health.services && (
        <div style={{ gridColumn: '1 / -1' }}>
          <div style={{ fontSize: 11, fontWeight: 700, color: C.accent, marginBottom: 6 }}>SERVICE STATUS</div>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(180px, 1fr))', gap: 6 }}>
            {Object.entries(health.services).map(([name, svc]) => {
              const svcOk = typeof svc === 'string' ? svc === 'ok' || svc === 'healthy' : svc?.status === 'ok' || svc?.status === 'healthy';
              return (
                <div key={name} style={{ ...S.kpiCard, display: 'flex', alignItems: 'center', gap: 8, padding: '6px 10px', justifyContent: 'flex-start' }}>
                  <span style={S.dot(svcOk)} />
                  <span style={{ fontSize: 10 }}>{name}</span>
                </div>
              );
            })}
          </div>
        </div>
      )}
      {health.uptime && (
        <div style={{ gridColumn: '1 / -1', fontSize: 10, color: C.textDim }}>
          Uptime: {typeof health.uptime === 'number' ? (health.uptime / 3600).toFixed(1) + ' hours' : health.uptime}
        </div>
      )}
    </div>
  );
}
