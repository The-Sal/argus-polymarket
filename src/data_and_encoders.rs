use uuid::Uuid;
use std::io::Read;
use serde_json::Value;
use flate2::read::ZlibDecoder;
use std::collections::HashMap;
use serde::{Deserialize, Deserializer, Serialize};
use base64::{Engine, engine::general_purpose::STANDARD as BASE64};


/// Deserializes a `u64` from either a JSON number or a quoted numeric string.
///
/// Polymarket's API inconsistently returns integer fields as bare numbers in some responses
/// and as quoted strings in others. Use this as `#[serde(deserialize_with = "deserialize_u64_from_string")]`
/// on any `u64` field that may arrive as either form.
pub fn deserialize_u64_from_string<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    let value: Value = Deserialize::deserialize(deserializer)?;
    match value {
        Value::Number(n) => n
            .as_u64()
            .ok_or_else(|| serde::de::Error::custom("expected u64")),
        Value::String(s) => s.parse::<u64>().map_err(|e| {
            serde::de::Error::custom(format!("failed to parse u64 from string: {}", e))
        }),
        _ => Err(serde::de::Error::custom(
            "expected string or number for u64 field",
        )),
    }
}

/// Deserializes a `f64` from either a JSON number or a quoted numeric string.
///
/// Polymarket's API returns price and size fields as quoted strings (e.g. `"0.45"`) in
/// most contexts. Use this as `#[serde(deserialize_with = "deserialize_f64_from_string")]`
/// on any `f64` field that may arrive as either form.
pub fn deserialize_f64_from_string<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: Deserializer<'de>,
{
    let value: Value = Deserialize::deserialize(deserializer)?;
    match value {
        Value::Number(n) => n
            .as_f64()
            .ok_or_else(|| serde::de::Error::custom("expected f64")),
        Value::String(s) => s.parse::<f64>().map_err(|e| {
            serde::de::Error::custom(format!("failed to parse f64 from string: {}", e))
        }),
        _ => Err(serde::de::Error::custom(
            "expected string or number for f64 field",
        )),
    }
}

/// Deserializes an `Option<f64>` from a JSON number, quoted numeric string, `null`, or empty string.
///
/// Handles the four forms Polymarket uses for optional numeric fields: a bare number, a quoted
/// number string, JSON `null`, or an empty string `""` (all of which map to `None` except the
/// numeric forms). Use as `#[serde(deserialize_with = "deserialize_optional_f64_from_string")]`.
pub fn deserialize_optional_f64_from_string<'de, D>(
    deserializer: D,
) -> Result<Option<f64>, D::Error>
where
    D: Deserializer<'de>,
{
    let value: Value = Deserialize::deserialize(deserializer)?;
    match value {
        Value::Null => Ok(None),
        Value::Number(n) => n
            .as_f64()
            .map(Some)
            .ok_or_else(|| serde::de::Error::custom("expected f64")),
        Value::String(s) if s.is_empty() => Ok(None),
        Value::String(s) => s.parse::<f64>().map(Some).map_err(|e| {
            serde::de::Error::custom(format!("failed to parse f64 from string: {}", e))
        }),
        _ => Err(serde::de::Error::custom(
            "expected string, number, or null for optional f64 field",
        )),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TraderSide {
    Maker,
    Taker,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event_type", rename_all = "snake_case")]
pub enum PolymarketEvent {
    Order(OrderEvent),
    Trade(TradeEvent),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OrderStatus {
    Live,
    Matched,
    Delayed,
    Unmatched,
    #[serde(alias = "CANCELED")]
    Cancelled,
    Mined,
    Confirmed,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderType {
    GTC,
    GTD,
    FOK,
    FAK,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderEvent {
    #[serde(rename = "id")]
    pub order_id: String,
    pub status: OrderStatus,
    pub side: OrderSide,
    pub order_type: OrderType,
    pub market: String,
    pub asset_id: String,
    pub outcome: String,
    #[serde(deserialize_with = "deserialize_f64_from_string")]
    pub price: f64,
    #[serde(deserialize_with = "deserialize_f64_from_string")]
    pub original_size: f64,
    #[serde(default)]
    #[serde(deserialize_with = "deserialize_optional_f64_from_string")]
    pub remaining_size: Option<f64>,
    #[serde(default)]
    #[serde(deserialize_with = "deserialize_optional_f64_from_string")]
    #[serde(rename = "size_matched")]
    pub matched_size: Option<f64>,
    #[serde(default)]
    #[serde(deserialize_with = "deserialize_u64_from_string")]
    pub expiration: u64,
    #[serde(deserialize_with = "deserialize_u64_from_string")]
    pub created_at: u64,
    #[serde(deserialize_with = "deserialize_u64_from_string")]
    pub timestamp: u64,
    #[serde(default)]
    #[serde(deserialize_with = "deserialize_u64_from_string")]
    pub last_update: u64,
    pub owner: String,
    pub maker_address: String,
    #[serde(rename = "type")]
    pub subtype: String,
    #[serde(default)]
    pub associate_trades: Vec<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeEvent {
    #[serde(rename = "id")]
    pub trade_id: String,
    pub asset_id: String,
    pub side: OrderSide,
    #[serde(deserialize_with = "deserialize_f64_from_string")]
    pub price: f64,
    #[serde(deserialize_with = "deserialize_f64_from_string")]
    pub size: f64,
    #[serde(deserialize_with = "deserialize_u64_from_string")]
    pub timestamp: u64,
    #[serde(rename = "taker_order_id")]
    pub order_id: String,
    pub market: String,
    pub outcome: String,
    pub transaction_hash: String,
    pub trade_owner: String,
    pub maker_orders: Vec<MakerOrder>,
    pub status: OrderStatus,
    pub trader_side: TraderSide,
    #[serde(deserialize_with = "deserialize_u64_from_string")]
    pub match_time: u64,
    #[serde(deserialize_with = "deserialize_u64_from_string")]
    pub last_update: u64,
    pub bucket_index: u64,
    pub fee_rate_bps: String,
    pub role: Option<TraderSide>,
    #[serde(default)]
    #[serde(deserialize_with = "deserialize_optional_f64_from_string")]
    pub fee: Option<f64>,
    pub counterparty_profile_id: Option<String>,
    pub owner: String,
    pub maker_address: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MakerOrder {
    pub order_id: String,
    pub owner: String,
    pub maker_address: String,
    #[serde(deserialize_with = "deserialize_f64_from_string")]
    pub matched_amount: f64,
    #[serde(deserialize_with = "deserialize_f64_from_string")]
    pub price: f64,
    pub fee_rate_bps: String,
    pub asset_id: String,
    pub outcome: String,
    pub outcome_index: u64,
    pub side: OrderSide,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OrderSide {
    Buy,
    Sell,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PlaceOrder {
    pub token_id: String,
    pub price: f64,
    pub size: f64,
    pub side: String,
    pub order_type: String,
}

impl PlaceOrder {
    /// Constructs a new `PlaceOrder`.
    ///
    /// `token_id` is the CLOB outcome token ID (from [`SubscriptionResponse`] or [`CLOBInfo`]).
    /// `price` is in cents on the dollar (0.01–0.99). `size` is the number of shares.
    /// `order_type` defaults to `"GTC"` (Good-Till-Cancelled) if `None`; also accepts `"GTD"`,
    /// `"FOK"` (Fill-Or-Kill), and `"FAK"` (Fill-And-Kill).
    pub fn new(
        token_id: &str,
        price: f64,
        size: f64,
        side: OrderSide,
        order_type: Option<&str>,
    ) -> Self {
        PlaceOrder {
            token_id: token_id.to_string(),
            price,
            size,
            side: match side {
                OrderSide::Buy => "buy".to_string(),
                OrderSide::Sell => "sell".to_string(),
            },
            order_type: order_type.unwrap_or("GTC").to_string(),
        }
    }

    /// Checks whether the order meets Polymarket's minimum notional value of $1.
    ///
    /// Returns `Ok(())` if `price * size > 1.0`, or `Err` with a descriptive message if not.
    /// Call this before submitting to avoid a server-side rejection.
    pub fn is_marketable(&self) -> Result<(), String> {
        let order_value = self.price * self.size;
        if order_value > 1f64 {
            Ok(())
        } else {
            Err(format!(
                "Order value is too small: {}. Order value must be greater than 1. Order={:?}",
                order_value, self
            ))
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct OrderPlacedMsg {
    pub error_msg: String,
    #[serde(rename = "orderID")]
    pub order_id: String,
    pub taking_amount: String,
    pub making_amount: String,
    pub status: String,
    pub success: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PlaceMultipleOrdersResponse {
    pub success: Vec<OrderPlacedMsg>,
    pub failed: Vec<Value>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CancelledMultipleOrdersResponse {
    pub not_canceled: HashMap<String, String>,
    pub canceled: Vec<Value>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PolyMarketOrder {
    pub id: String,
    pub status: String,
    pub owner: String,
    pub maker_address: String,
    pub market: String,
    pub asset_id: String,
    pub side: String,
    pub original_size: String,
    pub size_matched: String,
    pub price: String,
    pub outcome: String,
    pub expiration: String,
    pub order_type: String,
    #[serde(default)]
    pub associate_trades: Vec<Value>,
    pub created_at: i64,
}

#[derive(Debug, Clone)]
pub struct OrderCancelled {
    pub not_canceled: Value,
    pub canceled: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct Order {
    pub price: f64,
    pub quantity: f64,
}

#[derive(Debug, Clone)]
pub struct OrderBook {
    pub symbol: String,
    pub bids: Vec<Order>,
    pub asks: Vec<Order>,
    pub remote_timestamp: f64,
    pub argus_timestamp: f64,
}

impl OrderBook {
    /// Prints a formatted side-by-side view of the order book to stdout.
    ///
    /// Bids are shown on the left and asks on the right, both sorted best-first. Column widths are
    /// computed from the actual data so the output aligns cleanly regardless of price/quantity
    /// magnitude. Prints nothing but the header if both sides are empty.
    pub fn print_orderbook(&self) {
        use std::fmt::Write;
        let mut out = String::with_capacity(4096);

        let _ = writeln!(out, "Order Book for symbol: {}", self.symbol);

        if self.bids.is_empty() && self.asks.is_empty() {
            print!("{out}");
            return;
        }

        let bid_index_width = self.bids.len().to_string().len();
        let ask_index_width = self.asks.len().to_string().len();

        let mut bids = Vec::with_capacity(self.bids.len());
        let mut asks = Vec::with_capacity(self.asks.len());

        let mut max_bid_price = 0;
        let mut max_bid_qty = 0;
        let mut max_ask_price = 0;
        let mut max_ask_qty = 0;

        for o in &self.bids {
            let p = format!("{:.2}", o.price);
            let q = format!("{:.2}", o.quantity);
            max_bid_price = max_bid_price.max(p.len());
            max_bid_qty = max_bid_qty.max(q.len());
            bids.push((p, q));
        }

        for o in &self.asks {
            let p = format!("{:.2}", o.price);
            let q = format!("{:.2}", o.quantity);
            max_ask_price = max_ask_price.max(p.len());
            max_ask_qty = max_ask_qty.max(q.len());
            asks.push((p, q));
        }

        let bid_empty_len = "Bid ".len()
            + bid_index_width
            + ": Price: ".len()
            + max_bid_price
            + ", Quantity: ".len()
            + max_bid_qty;

        let ask_empty_len = "Ask ".len()
            + ask_index_width
            + ": Price: ".len()
            + max_ask_price
            + ", Quantity: ".len()
            + max_ask_qty;

        let bid_empty = " ".repeat(bid_empty_len);
        let ask_empty = " ".repeat(ask_empty_len);

        let rows = bids.len().max(asks.len());
        for i in 0..rows {
            if let Some((p, q)) = bids.get(i) {
                let _ = write!(
                    out,
                    "Bid {:>idx$}: Price: {:>pw$}, Quantity: {:>qw$}",
                    i + 1,
                    p,
                    q,
                    idx = bid_index_width,
                    pw = max_bid_price,
                    qw = max_bid_qty,
                );
            } else {
                out.push_str(&bid_empty);
            }

            out.push_str(" | ");

            if let Some((p, q)) = asks.get(i) {
                let _ = write!(
                    out,
                    "Ask {:>idx$}: Price: {:>pw$}, Quantity: {:>qw$}",
                    i + 1,
                    p,
                    q,
                    idx = ask_index_width,
                    pw = max_ask_price,
                    qw = max_ask_qty,
                );
            } else {
                out.push_str(&ask_empty);
            }

            out.push('\n');
        }

        print!("{out}");
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct OutBoundMessage {
    pub action: String,
    pub data: Value,
    pub correlation_id: String,
}

impl OutBoundMessage {
    /// Constructs an outbound message with a randomly generated correlation ID if one is not provided.
    ///
    /// `action` is the server command name (e.g. `"subscribe"`, `"place_order"`). `data` is the
    /// command payload as a `serde_json::Value`. `correlation_id` can be supplied to reuse a
    /// known ID; if `None` a UUID v4 is generated. The correlation ID is used by
    /// `get_my_packet_with_verification` to match the server's response to this specific request.
    pub fn new(action: String, data: Value, correlation_id: Option<String>) -> Self {
        OutBoundMessage {
            action,
            data,
            correlation_id: correlation_id.unwrap_or(Uuid::new_v4().to_string()),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct InBoundMessage {
    pub action: String,
    pub data: Value,
    pub error: Option<String>,
    pub compressed: Option<bool>,
    pub correlation_id: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SubscriptionResponse {
    pub subscribed: Vec<String>,
    pub failed: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct UnsubscriptionResponse {
    pub subscribed: Vec<String>,
    pub failed: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CLOBInfo {
    pub event_name: String,
    pub market_name: String,
    pub outcome: String,
    pub ticker: String,
    pub market_slug: String,
    pub aot_p2_symbol: String,
}

#[derive(Debug)]
pub enum ProtocolKind {
    Protocol1,
    Protocol2,
}

#[derive(Debug)]
pub struct Protocol2IR {
    pub symbol: String,
    pub values: Vec<f64>,
}

pub struct ProtocolFns;

impl ProtocolFns {
    /// Inspects a raw byte slice and determines which wire protocol it belongs to.
    ///
    /// Returns `Ok(ProtocolKind::Protocol1)` for JSON message packets (terminated with `}`)
    /// or `Ok(ProtocolKind::Protocol2)` for binary order book packets (terminated with `L`).
    /// Returns `Err` if the packet is too short, has an invalid header byte, or fails the
    /// per-protocol structural checks. This is called by the reading thread on each candidate
    /// packet before it is forwarded for decoding.
    pub fn analyse_bytes(bytes: &[u8]) -> Result<ProtocolKind, String> {
        if bytes.len() < 6 {
            return Err(format!(
                "Invalid packet: expected length of at least 6, got {}",
                bytes.len()
            ));
        }

        let raw_kind;
        let header = &bytes[0..5];

        if header[0] != 0x7E {
            return Err(format!(
                "Invalid header: expected first byte to be 0x7E, got 0x{:02X}",
                header[0]
            ));
        }

        let length_bytes = &header[1..5];
        let length_real = (length_bytes[0] - 0x30) as usize * 1000
            + (length_bytes[1] - 0x30) as usize * 100
            + (length_bytes[2] - 0x30) as usize * 10
            + (length_bytes[3] - 0x30) as usize;

        if bytes.len() < 5 + length_real {
            return Err(format!(
                "Invalid packet: expected length of at least {}, got {}",
                5 + length_real,
                bytes.len()
            ));
        }

        let maybe_pipe_char = bytes[5];

        if maybe_pipe_char == 0x7C {
            raw_kind = ProtocolKind::Protocol1;
        } else {
            raw_kind = ProtocolKind::Protocol2;
        }

        match raw_kind {
            ProtocolKind::Protocol1 => {
                if bytes[6] != 0x7B {
                    return Err(format!(
                        "Invalid packet for protocol 1: expected byte after pipe to be 0x7B, got 0x{:02X}",
                        bytes[6]
                    ));
                }

                if bytes[bytes.len() - 1] != 0x7D {
                    return Err(format!(
                        "Invalid packet for protocol 1: expected last byte to be 0x7D, got 0x{:02X}",
                        bytes[bytes.len() - 1]
                    ));
                }

                Ok(ProtocolKind::Protocol1)
            }
            ProtocolKind::Protocol2 => {
                if bytes[9] != 0x7C {
                    return Err(format!(
                        "Invalid packet for protocol 2: expected byte after symbol length to be 0x7C, got 0x{:02X}",
                        bytes[9]
                    ));
                }

                if bytes[bytes.len() - 1] != 0x4C {
                    return Err(format!(
                        "Invalid packet for protocol 2: expected last byte to be 0x4C, got 0x{:02X}",
                        bytes[bytes.len() - 1]
                    ));
                }

                Ok(ProtocolKind::Protocol2)
            }
        }
    }

    /// Serializes an [`OutBoundMessage`] into Protocol 1 wire bytes.
    ///
    /// Produces an owned `Vec<u8>` in the form `~NNNN|{...json...}` where `NNNN` is the
    /// zero-padded length of the JSON body. Pass the result directly to the TCP write stream.
    pub fn protocol_1_encoder(message: &OutBoundMessage) -> Vec<u8> {
        let json_string = serde_json::to_string(message).expect("Failed to serialize message");
        let length_of_message = json_string.len();
        let protocol_message = format!("~{:04}|{}", length_of_message, json_string);
        protocol_message.as_bytes().to_vec()
    }

    /// Deserializes a Protocol 1 packet into an [`InBoundMessage`].
    ///
    /// Strips the `~NNNN` header and the `|` separator, then JSON-parses the body. Returns an
    /// owned `InBoundMessage`. Panics if the packet is malformed or the JSON cannot be parsed —
    /// caller is expected to only pass bytes that have already passed [`analyse_bytes`].
    pub fn protocol_1_decoder(message: &[u8]) -> InBoundMessage {
        let message_str = String::from_utf8_lossy(message);
        let parts: Vec<&str> = message_str.splitn(2, '|').collect();
        if parts.len() != 2 {
            panic!("Invalid message format");
        }
        let json_part = parts[1];
        let inbound_message: InBoundMessage =
            serde_json::from_str(json_part).expect("Failed to deserialize message");
        inbound_message
    }

    /// Decompresses a Protocol 1 message in place if it is marked as compressed.
    ///
    /// When the server sets `compressed: true` on a response (used for large payloads such as
    /// market search results), the `data` field is a base64-encoded zlib-compressed JSON string.
    /// This function decodes and decompresses it, replacing `data` with the parsed `Value` and
    /// setting `compressed` to `false`. Is a no-op if `compressed` is not `Some(true)`.
    pub fn maybe_decompress_p1(msg: &mut InBoundMessage) {
        if msg.compressed != Some(true) {
            return;
        }

        let data_str = match msg.data.as_str() {
            Some(s) => s,
            None => return,
        };

        let compressed_bytes = match BASE64.decode(data_str) {
            Ok(b) => b,
            Err(_) => return,
        };

        let mut decoder = ZlibDecoder::new(&compressed_bytes[..]);
        let mut decompressed = String::new();
        if decoder.read_to_string(&mut decompressed).is_err() {
            return;
        }

        if let Ok(json_value) = serde_json::from_str(&decompressed) {
            msg.data = json_value;
            msg.compressed = Some(false);
        }
    }

    /// Decodes a Protocol 2 packet into its intermediate representation.
    ///
    /// Strips framing bytes, extracts the symbol string and the trailing comma-separated float
    /// values (bids, asks, timestamps) using `fast_float` for performance. Returns an owned
    /// [`Protocol2IR`] with the raw `Vec<f64>`. The caller must know the value layout to
    /// interpret the slice; prefer [`bytes_to_orderbook`] which handles the layout automatically.
    /// Uses `unsafe` `from_utf8_unchecked` on the inner content — safe because Argus only
    /// encodes ASCII.
    pub fn protocol_2_decoder(message_bytes: &[u8]) -> Result<Protocol2IR, String> {
        if message_bytes[message_bytes.len() - 1] != 0x4C {
            Err(format!(
                "Invalid packet for protocol 2: expected last byte to be 0x4C, got 0x{:02X}",
                message_bytes[message_bytes.len() - 1]
            ))?;
        }

        let inner_content = &message_bytes[5..message_bytes.len() - 1];
        let inner_content_str: &str;

        // SAFETY: Argus only encodes ASCII
        unsafe {
            inner_content_str = std::str::from_utf8_unchecked(&inner_content);
        }

        let symbol_length: usize = inner_content_str[0..4]
            .parse()
            .expect("Failed to parse symbol length");
        let symbol = &inner_content_str[5..5 + symbol_length];
        let message_body = &inner_content_str[5 + symbol_length..];

        let mut working_buff = String::new();
        let mut parsed_values: Vec<f64> = Vec::new();

        for c in message_body.chars() {
            if c == ',' {
                let value: f64 = fast_float::parse(working_buff.as_str())
                    .expect(format!("Failed to parse value: {}", working_buff).as_str());
                parsed_values.push(value);
                working_buff.clear();
            } else {
                working_buff.push(c);
            }
        }

        if !working_buff.is_empty() {
            let value: f64 = fast_float::parse(working_buff.as_str())
                .expect(format!("Failed to parse value: {}", working_buff).as_str());
            parsed_values.push(value);
        }

        Ok(Protocol2IR {
            symbol: symbol.to_string(),
            values: parsed_values,
        })
    }

    /// Decodes a Protocol 2 packet directly into an [`OrderBook`].
    ///
    /// Calls [`protocol_2_decoder`] and interprets the resulting float slice as interleaved
    /// (price, quantity) pairs: the first half is bids, the second half is asks, with the last
    /// two values being the exchange timestamp (`remote_timestamp`) and the Argus timestamp
    /// (`argus_timestamp`). `depth` controls how many levels to read from each side; defaults
    /// to 10 if `None`. Panics if the packet contains fewer values than `depth * 2` per side.
    /// Returns an owned `OrderBook` — this is a point-in-time snapshot, not a live handle.
    pub fn bytes_to_orderbook(message_bytes: &[u8], depth: Option<usize>) -> OrderBook {
        let depth = depth.unwrap_or(10);
        let ir = ProtocolFns::protocol_2_decoder(message_bytes)
            .expect("Failed to decode protocol 2 message");

        let argus_timestamp = ir.values[ir.values.len() - 1];
        let remote_timestamp = ir.values[ir.values.len() - 2];
        let values_without_timestamps = &ir.values[0..ir.values.len() - 2];

        let mut bids: Vec<Order> = Vec::new();
        let mut asks: Vec<Order> = Vec::new();

        let split_index = values_without_timestamps.len() / 2;

        if split_index < depth {
            panic!(
                "Not enough values in the message to fill the orderbook depth, expected at least {}, got {}",
                depth * 2,
                ir.values.len()
            );
        }

        let bid_values = &values_without_timestamps[0..split_index];
        let ask_values = &values_without_timestamps[split_index..];

        for i in 0..depth {
            let bid_price = bid_values[i * 2];
            let bid_quantity = bid_values[i * 2 + 1];
            let ask_price = ask_values[i * 2];
            let ask_quantity = ask_values[i * 2 + 1];

            bids.push(Order {
                price: bid_price,
                quantity: bid_quantity,
            });

            asks.push(Order {
                price: ask_price,
                quantity: ask_quantity,
            });
        }

        OrderBook {
            symbol: ir.symbol,
            bids,
            asks,
            remote_timestamp,
            argus_timestamp,
        }
    }
}
