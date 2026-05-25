# argus-polymarket

A Rust client library for connecting to an Argus server — a high-performance Polymarket market data and order routing server. Provides real-time order book streaming, account event handling, and full order management over a persistent TCP connection.

## Overview

`argus-polymarket` sits between your algo/strategy code and the Argus server process. The server handles the Polymarket WebSocket connection, order signing, and market data fan-out; this crate handles the TCP wire protocol, background I/O threads, and exposes a clean synchronous API for your strategy.

```
Your code  ←→  argus-polymarket  ←→  Argus server  ←→  Polymarket
```

The library connects over raw TCP and manages two background threads internally:
- **Reading thread** — reads bytes from the socket, frames packets by delimiter, and sends them to the processing thread
- **Processing thread** — decodes packets and routes them to either the live order book map or the system messages buffer

## Quick Start

```toml
[dependencies]
argus-polymarket = { git = "https://github.com/the-sal/argus-polymarket" }
event-listener = "5"
```

```rust
use argus_polymarket::{MarketDataConnection, PlaceOrder, OrderSide};

// Connect to Argus (typically running locally)
let mut conn = MarketDataConnection::new("localhost:9972");
conn.read_data_forever(); // spawns background I/O threads

// Check latency
let rtt_ms = conn.rtt_to_server().unwrap();
println!("RTT to server: {}ms", rtt_ms);

// Search for markets and subscribe
let markets = conn.search_for_markets("btc-updown-5m", Some(50)).unwrap();
let subscription = conn.subscribe_to_event(&markets[0]).unwrap();

// Get the CLOB token IDs from the subscription
let up_token = &subscription.subscribed[0];
let down_token = &subscription.subscribed[1];

// Resolve orderbook symbols for each token
let up_info = conn.fetch_clob_id_information(up_token).unwrap();
let down_info = conn.fetch_clob_id_information(down_token).unwrap();

// The order book map is updated by the background thread
let books = conn.get_order_book();
let market_event = conn.get_order_book_event();

// Wait for next update and read the book
let listener = market_event.listen();
listener.wait();
let snapshot = books.read().unwrap();
if let Some(book) = snapshot.get(&up_info.aot_p2_symbol) {
    book.print_orderbook();
}
```

## Architecture

### Protocols

The Argus server uses two wire protocols over the same TCP connection.

**Protocol 1** — JSON request/response and server push events.

```
~NNNN|{...json...}
```

- Header: `~` byte (`0x7E`) followed by a 4-digit ASCII length
- Separator: `|` (`0x7C`)
- Body: JSON object, terminated with `}` (`0x7D`)
- Large responses (e.g. market search results) may be zlib-compressed and base64-encoded; the library decompresses them transparently

**Protocol 2** — compact binary order book snapshots, emitted continuously by the server.

```
~NNNN<sym_len>|<symbol><bid_price>,<bid_qty>,...,<ask_price>,<ask_qty>,...,<remote_ts>,<argus_ts>L
```

- Terminated with `L` (`0x4C`)
- Values are ASCII floats separated by commas
- Layout: `N` bid (price, qty) pairs, then `N` ask (price, qty) pairs, then exchange timestamp, then Argus timestamp
- Default book depth is 10 levels

### Threading model

`read_data_forever()` spawns two threads and returns the reading thread's `JoinHandle`. Both threads run for the lifetime of the connection.

```
TCP socket
    │
    ▼
Reading thread ──(crossbeam channel)──▶ Processing thread
                                              │
                    ┌─────────────────────────┴──────────────────────┐
                    ▼                                                 ▼
         Protocol 1 JSON message                         Protocol 2 orderbook
                    │                                                 │
          ┌─────────┴──────────┐                      RwLock<HashMap<String, OrderBook>>
          ▼                    ▼                               + notify market_event
  Control messages       System messages
  (matched by           (notification,
  correlation_id)       account_update,
                        fatal_error)
                        → SystemMessagesPushed
```

All shared state is behind `Arc<RwLock<...>>` or `Arc<Mutex<...>>`. The reading thread holds the write lock on the `TcpStream` only during an active `read()` call.

## Argus Server Configuration

The Argus sharding system controls how many Polymarket WebSocket connections back the server. Recommended settings:

| `POLYMARKET_MAX_ASSETS_PER_WS` | `POLYMARKET_MAX_SHARDS` | Notes |
|---|---|---|
| `4` | `10` | Minimum viable |
| `2` | `15` | Best performance |

## API Reference

### `MarketDataConnection`

#### Connecting

```rust
let mut conn = MarketDataConnection::new("localhost:9972");
// This blocks until the TCP connection is established.
conn.read_data_forever(); // must be called before any other method
```

#### Accessing live market data

```rust
// Arc<RwLock<HashMap<String, OrderBook>>> — updated on every Protocol 2 packet
let books: Arc<RwLock<HashMap<String, OrderBook>>> = conn.get_order_book();

// Arc<Event> — notified on every orderbook update
let event: Arc<Event> = conn.get_order_book_event();

// Arc<RwLock<SystemMessagesPushed>> — account_update, notification, fatal_error
let sys: Arc<RwLock<SystemMessagesPushed>> = conn.get_sys_msgs();
```

These return clones of internal `Arc`s so they can be moved into other threads. The typical pattern in a hot loop:

```rust
loop {
    let listener = event.listen(); // register BEFORE reading state to avoid races
    let snapshot = books.read().unwrap();
    // ... use snapshot ...
    drop(snapshot);

    // Drain any account updates
    let msgs = sys.write().unwrap().drain();
    // ... handle msgs ...

    listener.wait(); // block until next update
}
```

#### Market discovery

```rust
// Search for market tickers matching a query string
// Returns a Vec<String> of ticker names
let tickers = conn.search_for_markets("btc-updown-15m", Some(200)).unwrap();

// Get the current price-to-beat (strike price) for a market
let ptb: f64 = conn.get_price_to_beat("btc-updown-15m-1746000000").unwrap();
```

#### Subscriptions

Subscribing tells the Argus server to route Protocol 2 order book packets for the relevant tokens to this connection.

```rust
// Subscribe to all tokens in a market event (identified by ticker)
// Returns subscribed token IDs (index 0 = Up, index 1 = Down for binary markets)
let sub: SubscriptionResponse = conn.subscribe_to_event("btc-updown-5m-1746000000").unwrap();
let up_token_id = &sub.subscribed[0];
let down_token_id = &sub.subscribed[1];

// Subscribe to a specific token by its CLOB token ID
let sub: SubscriptionResponse = conn.subscribe_to_instrument(up_token_id).unwrap();
```

#### Token metadata

```rust
// Resolve a CLOB token ID to its full metadata
let info: CLOBInfo = conn.fetch_clob_id_information(up_token_id).unwrap();
// info.aot_p2_symbol is the key used in the orderbook HashMap
// info.outcome is "Yes" or "No" / "Up" or "Down"
// info.event_name, info.market_name, info.ticker, info.market_slug are human-readable identifiers
```

`CLOBInfo.aot_p2_symbol` is the key you use to look up order books:

```rust
let books = conn.get_order_book();
let snapshot = books.read().unwrap();
let book: &OrderBook = snapshot.get(&info.aot_p2_symbol).unwrap();
```

#### Account queries

```rust
let balance: f64 = conn.get_balance().unwrap();              // USDC balance
let token_bal: f64 = conn.get_token_balance(token_id).unwrap(); // outcome token balance
let orders: Vec<PolyMarketOrder> = conn.get_orders().unwrap(); // all open orders
let order: PolyMarketOrder = conn.get_order_status(order_id).unwrap();
```

#### Order management

```rust
// Build an order
let order = PlaceOrder::new(
    token_id,      // CLOB token ID (from CLOBInfo or subscription)
    0.45,          // price (0.01–0.99 cents on the dollar)
    10.0,          // size in shares
    OrderSide::Buy,
    None,          // order_type: defaults to "GTC"
);

// Validate the order is large enough to be submitted ($1 minimum notional)
order.is_marketable().unwrap();

// Place a single order
let response: OrderPlacedMsg = conn.place_order(order).unwrap();
println!("Order ID: {}", response.order_id);

// Place multiple orders atomically
let responses: Vec<OrderPlacedMsg> = conn.place_multiple_orders(vec![order1, order2]).unwrap();

// Cancel
let cancelled: OrderCancelled = conn.cancel_order(&order_id).unwrap();
let result: CancelledMultipleOrdersResponse = conn.cancel_multiple_orders(order_ids).unwrap();
```

#### Diagnostics

```rust
let client_to_server_ms: u128 = conn.rtt_to_server().unwrap();
let server_to_exchange_ms: u128 = conn.rtt_to_exchange_from_server().unwrap();
```

### `OrderBook`

```rust
pub struct OrderBook {
    pub symbol: String,           // aot_p2_symbol key
    pub bids: Vec<Order>,         // sorted best-first
    pub asks: Vec<Order>,         // sorted best-first
    pub remote_timestamp: f64,    // exchange timestamp (ms)
    pub argus_timestamp: f64,     // Argus server timestamp (seconds, multiply by 1000 for ms)
}

pub struct Order {
    pub price: f64,
    pub quantity: f64,
}
```

Print a formatted book:
```rust
book.print_orderbook();
```

Compute latency from an order book:
```rust
let now_ms = chrono::Utc::now().timestamp_millis() as f64;
let ms_since_argus = now_ms - (book.argus_timestamp * 1000.0);
let ms_since_exchange = now_ms - book.remote_timestamp;
```

### `PlaceOrder`

```rust
pub struct PlaceOrder {
    pub token_id: String,
    pub price: f64,
    pub size: f64,
    pub side: String,       // "buy" or "sell"
    pub order_type: String, // "GTC", "GTD", "FOK", "FAK"
}

impl PlaceOrder {
    pub fn new(token_id: &str, price: f64, size: f64, side: OrderSide, order_type: Option<&str>) -> Self;

    // Returns Err if price * size <= 1.0 (Polymarket's minimum notional)
    pub fn is_marketable(&self) -> Result<(), String>;
}
```

### `PolymarketEvent`

Account update messages from the server are deserialized into this tagged enum:

```rust
pub enum PolymarketEvent {
    Order(OrderEvent),
    Trade(TradeEvent),
}
```

Deserialize from a system message:

```rust
let msgs = conn.get_sys_msgs().write().unwrap().drain();
for msg in msgs {
    if msg.action == "account_update" {
        let event: PolymarketEvent = serde_json::from_value(msg.data).unwrap();
        match event {
            PolymarketEvent::Order(o) => {
                // o.order_id, o.status (Live/Matched/Cancelled/...), o.matched_size, ...
            }
            PolymarketEvent::Trade(t) => {
                // t.order_id (taker), t.maker_orders, t.price, t.size, ...
            }
        }
    }
}
```

Key fields on `OrderEvent`:
- `order_id` — hex order ID used to match against placed orders
- `status` — `OrderStatus::{Live, Matched, Delayed, Cancelled, Mined, Confirmed, Failed}`
- `matched_size` — shares matched so far (cumulative)
- `subtype` — `"PLACEMENT"`, `"UPDATE"`, or `"CANCELLATION"`

Key fields on `TradeEvent`:
- `order_id` — the taker's order ID
- `maker_orders` — `Vec<MakerOrder>` with individual maker fills
- `price`, `size` — fill price and size
- `trader_side` — `TraderSide::{Maker, Taker}`

### `SystemMessagesPushed`

Holds control messages that were pushed by the server asynchronously (not in response to a specific request).

```rust
let sys = conn.get_sys_msgs();
let msgs: Vec<InBoundMessage> = sys.write().unwrap().drain();
```

`drain()` takes all buffered messages and clears the internal buffer. A warning is printed if more than 5 messages accumulate without being drained.

### `ProtocolFns` (low-level)

These are used internally but are public for testing:

```rust
ProtocolFns::analyse_bytes(&bytes)       // → Result<ProtocolKind, String>
ProtocolFns::protocol_1_encoder(&msg)    // → Vec<u8>
ProtocolFns::protocol_1_decoder(&bytes)  // → InBoundMessage
ProtocolFns::maybe_decompress_p1(&mut msg) // in-place zlib decompression if compressed=true
ProtocolFns::protocol_2_decoder(&bytes)  // → Result<Protocol2IR, String>
ProtocolFns::bytes_to_orderbook(&bytes, depth) // → OrderBook
```

### Deserializer helpers

Polymarket's API returns many numeric fields as JSON strings. These public helpers handle the conversion and are re-exported for use in downstream types:

```rust
deserialize_f64_from_string        // accepts "0.45" or 0.45
deserialize_optional_f64_from_string  // accepts "0.45", 0.45, null, or ""
deserialize_u64_from_string        // accepts "1746000000" or 1746000000
```

## Full Example: Subscribe and Stream Order Books

This is essentially what bt1560 does during startup:

```rust
use argus_polymarket::{MarketDataConnection, CLOBInfo};
use event_listener::Listener;

fn main() {
    let mut conn = MarketDataConnection::new("localhost:9972");
    conn.read_data_forever();

    println!("RTT: {}ms", conn.rtt_to_server().unwrap());

    // Find the current epoch-based 5-minute BTC market
    let query = format!("btc-updown-5m-{}", now_secs());
    let markets = conn.search_for_markets(&query, Some(200)).unwrap();
    let ticker = markets
        .iter()
        .filter_map(|m| parse_epoch(m, "btc-updown-5m").map(|t| (m, t)))
        .filter(|(_, t)| *t <= now_secs())
        .max_by_key(|(_, t)| *t)
        .map(|(m, _)| m.clone())
        .expect("no active market found");

    // Subscribe and map tokens to orderbook symbols
    let sub = conn.subscribe_to_event(&ticker).unwrap();
    let up_sym = conn.fetch_clob_id_information(&sub.subscribed[0]).unwrap().aot_p2_symbol;
    let down_sym = conn.fetch_clob_id_information(&sub.subscribed[1]).unwrap().aot_p2_symbol;

    let ptb = conn.get_price_to_beat(&ticker).unwrap();
    println!("Price to beat: {}", ptb);

    // Stream order books
    let books = conn.get_order_book();
    let event = conn.get_order_book_event();
    let sys = conn.get_sys_msgs();

    loop {
        let listener = event.listen();

        {
            let snapshot = books.read().unwrap();
            if let (Some(up_book), Some(down_book)) = (snapshot.get(&up_sym), snapshot.get(&down_sym)) {
                let best_ask_up = up_book.asks.first().map(|o| o.price).unwrap_or(0.0);
                let best_ask_down = down_book.asks.first().map(|o| o.price).unwrap_or(0.0);
                println!("sum of asks: {:.4} (ptb: {})", best_ask_up + best_ask_down, ptb);
            }
        }

        // Drain account updates
        for msg in sys.write().unwrap().drain() {
            eprintln!("system message: {} {:?}", msg.action, msg.error);
        }

        listener.wait();
    }
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn parse_epoch(ticker: &str, prefix: &str) -> Option<u64> {
    let search = format!("{}-", prefix);
    ticker.rfind(&search)
        .and_then(|pos| ticker[pos + search.len()..].parse().ok())
}
```

## Key Concepts

**Market vs. Token vs. Symbol**

- **Ticker** (market): e.g. `btc-updown-5m-1746000000`. Identifies a Polymarket *event* containing two outcome tokens.
- **Token ID** (CLOB ID): a hex string identifying a specific outcome token (Up or Down). This is what you use when placing orders.
- **Symbol** (`aot_p2_symbol`): a compound string (`ticker-slug-token_id`) that the Argus server uses as the key for Protocol 2 order book packets. Look up order books by this key.

**Correlation IDs**

Every outbound request gets a UUID correlation ID. `get_my_packet_with_verification()` blocks until the server returns a matching response or the timeout (default 10 seconds) elapses. This makes the API fully synchronous from the caller's perspective.

**Ordering guarantee**

The server does not guarantee order of Protocol 1 responses if multiple requests are in flight simultaneously. `get_my_packet_with_verification()` scans the entire response buffer for a matching correlation ID, so interleaved responses are handled correctly. However, concurrent calls from multiple threads on the same connection are safe (protected by `Arc<Mutex<TcpStream>>`).

**Minimum order size**

Polymarket rejects orders with a notional value (`price × size`) below $1. Use `PlaceOrder::is_marketable()` to validate before submitting.
