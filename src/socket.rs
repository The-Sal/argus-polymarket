/*

This module was designed around the `cf5e438128ab5d5f8b2d10b7dbd741957b93a287` commit.
This maps roughly to Argus version 0.1.0.

Update: This supports Argus 0.2.6 (and is not backward compatible) and uses the prod/runner-ie-bt1560 branch
that cuts releases for BT1560. Last tested with ec170453a9dd273bfb58009f2b94b1e53da4316f
It uses implicitly expects the Argus Sharding system to run underneath. Best config is
`POLYMARKET_MAX_ASSETS_PER_WS`=4 (minimum) 2 (best perf)
`POLYMARKET_MAX_SHARDS`=10 (for 4) 15 (for 2)

*/

use serde_json::Value;
use std::net::TcpStream;
use std::io::{Read, Write};
use std::collections::HashMap;
use crate::data_and_encoders::*;
use std::sync::{Arc, Mutex, RwLock};
use std::thread::{spawn, JoinHandle};
use event_listener::{Event, Listener};
use crossbeam::channel::{unbounded, Receiver, Sender};

const CONTROL_MESSAGES: &[&str] = &["notification", "fatal_error", "account_update"];

#[derive(Debug)]
pub struct SystemMessagesPushed {
    buffer: Vec<InBoundMessage>,
}

impl SystemMessagesPushed {
    fn new() -> Self {
        Self { buffer: Vec::new() }
    }

    /// Drains all messages that have been pushed but not yet read.
    pub fn drain(&mut self) -> Vec<InBoundMessage> {
        std::mem::take(&mut self.buffer)
    }

    fn add_message(&mut self, msg: InBoundMessage) {
        self.buffer.push(msg);
        if self.buffer.len() > 5 {
            println!(
                "[WARNING] There are {} messages in the system message \
            buffer that have not yet been read by the algo. This maybe a \
            sign that the algo is not keeping up with the incoming messages, \
            or that there is some other issue. Here are the messages in the \
            buffer: {:?}",
                self.buffer.len(),
                self.buffer
            );
        }
    }
}

#[derive(Debug)]
pub struct MarketDataConnection {
    pub read_stream_handle: Arc<RwLock<TcpStream>>,
    pub write_stream_handle: Arc<Mutex<TcpStream>>,
    order_books: Arc<RwLock<HashMap<String, OrderBook>>>,
    market_event: Arc<Event>,
    system_messages_pushed: Arc<RwLock<SystemMessagesPushed>>,
    control_msg_match_buf: Arc<RwLock<Vec<InBoundMessage>>>,
    control_msg_match_event: Arc<Event>,
}

impl MarketDataConnection {
    /// Opens a blocking TCP connection to the Argus server at `address` (e.g. `"localhost:9972"`).
    ///
    /// Panics if the connection cannot be established. Call [`read_data_forever`] immediately
    /// after construction to start the background I/O threads before issuing any requests.
    pub fn new(address: &str) -> Self {
        println!("Connecting to server at {}", address);
        let stream = TcpStream::connect(address).expect("Could not connect to server");
        println!("Successfully connected to server at {}", address);
        MarketDataConnection {
            read_stream_handle: Arc::new(RwLock::new(
                stream.try_clone().expect("Failed to clone stream"),
            )),
            write_stream_handle: Arc::new(Mutex::new(stream)),
            order_books: Arc::new(RwLock::new(HashMap::new())),
            control_msg_match_buf: Arc::new(Default::default()),
            control_msg_match_event: Arc::new(Event::new()),
            market_event: Arc::new(Event::new()),
            system_messages_pushed: Arc::new(RwLock::new(SystemMessagesPushed::new())),
        }
    }

    /// Returns a shared handle to the live order book map.
    ///
    /// The returned `Arc<RwLock<HashMap<String, OrderBook>>>` is backed by the same allocation
    /// that the background processing thread writes to. Every Protocol 2 packet received from
    /// the server overwrites the entry for that symbol in place, so a read lock taken at any
    /// point will see the most recent snapshot available. The map key is the `aot_p2_symbol`
    /// string from [`CLOBInfo`], obtained via [`fetch_clob_id_information`].
    pub fn get_order_book(&self) -> Arc<RwLock<HashMap<String, OrderBook>>> {
        self.order_books.clone()
    }

    /// Returns a shared handle to the market-data notification event.
    ///
    /// The background processing thread calls `notify(usize::MAX)` on this [`Event`] every time
    /// a Protocol 2 order book packet is processed. Register a [`Listener`] *before* reading the
    /// order book map to avoid missing updates between the read and the wait:
    ///
    /// ```
    /// let listener = event.listen(); // register first
    /// let snapshot = books.read().unwrap(); // then read
    /// // ... use snapshot ...
    /// drop(snapshot);
    /// listener.wait(); // block until the next update arrives
    /// ```
    pub fn get_order_book_event(&self) -> Arc<Event> {
        self.market_event.clone()
    }

    /// Returns a shared handle to the system messages buffer.
    ///
    /// The background processing thread pushes `account_update`, `notification`, and
    /// `fatal_error` messages from the server into this buffer as they arrive. Call
    /// [`SystemMessagesPushed::drain`] on the write-locked guard to take all pending messages
    /// and clear the buffer in one operation. A warning is printed to stdout if more than 5
    /// messages accumulate without being drained.
    pub fn get_sys_msgs(&self) -> Arc<RwLock<SystemMessagesPushed>> {
        self.system_messages_pushed.clone()
    }

    /// Spawns the background reading and processing threads and returns the reading thread's handle.
    ///
    /// Must be called once before any other method. Two threads are started:
    /// - **Reading thread** — holds the TCP read lock, reads bytes into a rolling buffer, detects
    ///   complete packets by their terminator byte, validates them with [`ProtocolFns::analyse_bytes`],
    ///   and forwards them to the processing thread via a crossbeam channel.
    /// - **Processing thread** — decodes each packet. Protocol 2 packets update the order book map
    ///   and fire the market event. Protocol 1 packets are routed to either the system messages
    ///   buffer (for push events) or the control message buffer (for request responses).
    ///
    /// The returned `JoinHandle` belongs to the reading thread. The processing thread is detached.
    /// Both threads run indefinitely; the reading thread panics if the server closes the connection.
    pub fn read_data_forever(&mut self) -> JoinHandle<()> {
        let read_handle = Arc::clone(&self.read_stream_handle);
        let orderbooks_handle = Arc::clone(&self.order_books);
        let control_messages_matching_buffer_handle = Arc::clone(&self.control_msg_match_buf);
        let control_messages_matching_event_handle = Arc::clone(&self.control_msg_match_event);
        let market_event_handle = Arc::clone(&self.market_event);
        let system_messages_handle = Arc::clone(&self.system_messages_pushed);

        let (sender, receiver): (
            Sender<(
                Vec<u8>,
                ProtocolKind,
                Arc<RwLock<Vec<InBoundMessage>>>,
                Arc<Event>,
            )>,
            Receiver<(
                Vec<u8>,
                ProtocolKind,
                Arc<RwLock<Vec<InBoundMessage>>>,
                Arc<Event>,
            )>,
        ) = unbounded();

        // reading thread
        let handle = spawn(move || {
            let mut buffer = [0; 9999];
            let mut full_packet_buffer: Vec<u8> = Vec::new();

            loop {
                let mut stream = read_handle.write().unwrap();
                match stream.read(&mut buffer) {
                    Ok(bytes_read) => {
                        if bytes_read > 0 {
                            for char in buffer[..bytes_read].iter() {
                                full_packet_buffer.push(*char);
                                if char == &0x4C || char == &0x7D {
                                    let is_valid_packet =
                                        ProtocolFns::analyse_bytes(&full_packet_buffer);
                                    match is_valid_packet {
                                        Ok(protocol_kind) => {
                                            sender
                                                .send((
                                                    full_packet_buffer.clone(),
                                                    protocol_kind,
                                                    Arc::clone(
                                                        &control_messages_matching_buffer_handle,
                                                    ),
                                                    Arc::clone(&control_messages_matching_event_handle),
                                                ))
                                                .expect("Failed to send data to processing thread");
                                            full_packet_buffer.clear();
                                        }
                                        Err(_) => {}
                                    }
                                }
                            }
                        } else {
                            panic!("Stream closed by server");
                        }
                    }
                    Err(e) => {
                        eprintln!("Error reading from stream: {}", e);
                        break;
                    }
                }
            }
        });

        // processing thread
        spawn(move || {
            while let Ok(buffer_and_friends) = receiver.recv() {
                Self::process_data_static(
                    &buffer_and_friends.0,
                    buffer_and_friends.1,
                    Arc::clone(&orderbooks_handle),
                    Arc::clone(&buffer_and_friends.2),
                    Arc::clone(&buffer_and_friends.3),
                    Arc::clone(&market_event_handle),
                    Arc::clone(&system_messages_handle),
                );
            }
        });

        handle
    }

    fn process_data_static(
        buffer: &[u8],
        protocol_kind: ProtocolKind,
        orderbook_handle: Arc<RwLock<HashMap<String, OrderBook>>>,
        matching_message_handle: Arc<RwLock<Vec<InBoundMessage>>>,
        matching_message_event: Arc<Event>,
        market_event: Arc<Event>,
        system_messages_handle: Arc<RwLock<SystemMessagesPushed>>,
    ) {
        match protocol_kind {
            ProtocolKind::Protocol1 => {
                let mut decoded = ProtocolFns::protocol_1_decoder(&buffer);
                ProtocolFns::maybe_decompress_p1(&mut decoded);

                if CONTROL_MESSAGES.contains(&decoded.action.as_str()) {
                    system_messages_handle
                        .write()
                        .expect("Failed to lock system messages buffer for writing")
                        .add_message(decoded.clone());
                } else {
                    matching_message_handle
                        .write()
                        .expect("Failed to lock control message buffer for writing")
                        .push(decoded);
                }
                matching_message_event.notify(usize::MAX);
            }
            ProtocolKind::Protocol2 => {
                let ob = ProtocolFns::bytes_to_orderbook(&buffer, None);
                {
                    let mut orderbooks = orderbook_handle
                        .write()
                        .expect("Failed to lock orderbooks for writing");
                    orderbooks.insert(ob.symbol.clone(), ob);
                }
                market_event.notify(usize::MAX);
            }
        }
    }

    fn send_message(&self, message: &Vec<u8>) {
        let mut stream = self
            .write_stream_handle
            .lock()
            .expect("Failed to lock the write stream");
        match stream.write_all(message) {
            Ok(_) => {}
            Err(e) => eprintln!("Error sending message: {}", e),
        }
    }

    fn get_my_packet_with_verification(
        &self,
        outbound_msg: &OutBoundMessage,
        timeout: Option<u64>,
    ) -> Result<InBoundMessage, String> {
        let start_time = std::time::Instant::now();
        let timeout = timeout.unwrap_or(10000);

        let outbound_corr = &outbound_msg.correlation_id;
        let outbound_action = &outbound_msg.action;

        fn drop_at_index(handle: &Arc<RwLock<Vec<InBoundMessage>>>, index: usize) {
            handle
                .write()
                .expect("Failed to lock control message buffer for writing")
                .remove(index);
        }

        loop {
            let listener = self.control_msg_match_event.listen();
            if listener
                .wait_timeout(std::time::Duration::from_secs(timeout))
                .is_none()
            {
                return Err(format!(
                    "Timeout on .wait_timeout for message with action {}",
                    outbound_action
                ));
            }
            let time_delta = start_time.elapsed().as_secs();
            if time_delta >= timeout {
                return Err(format!(
                    "Timeout waiting for message with action {}",
                    outbound_action
                ));
            }

            let cntl_buff_handle = self.control_msg_match_buf.clone();

            let matching_msg: Option<(usize, InBoundMessage)> = {
                let buf = cntl_buff_handle
                    .read()
                    .expect("Failed to lock control message buffer for reading");

                buf.iter().enumerate().find_map(|(index, msg)| {
                    let optional_corr = msg.correlation_id.clone();
                    let action_msg = msg.action.clone();

                    match optional_corr {
                        Some(corr) => {
                            if corr == *outbound_corr {
                                Some((index, msg.clone()))
                            } else {
                                None
                            }
                        }
                        _ => {
                            eprintln!(
                                "Expected correlation id {} but got {}, msg_action={:?}",
                                outbound_corr,
                                optional_corr.unwrap_or("None".to_string()),
                                msg.action
                            );

                            if action_msg == *outbound_action {
                                Some((index, msg.clone()))
                            } else {
                                None
                            }
                        }
                    }
                })
            };

            if let Some((index, msg)) = matching_msg {
                drop_at_index(&self.control_msg_match_buf, index);
                return Ok(msg);
            }
        }
    }
}

impl MarketDataConnection {
    /// Subscribes to a single outcome token by its CLOB token ID.
    ///
    /// Sends a `subscribe` request to the server and blocks until the confirmation arrives
    /// (up to 10 seconds). Returns a [`SubscriptionResponse`] whose `subscribed` vec contains
    /// the token IDs that were successfully registered, and `failed` contains any that were not.
    /// After a successful subscription the server will begin streaming Protocol 2 order book
    /// packets for this token; look them up in the map from [`get_order_book`] using the
    /// `aot_p2_symbol` from [`fetch_clob_id_information`].
    pub fn subscribe_to_instrument(
        &self,
        instrument: &str,
    ) -> Result<SubscriptionResponse, String> {
        let msg = OutBoundMessage::new(
            "subscribe".to_string(),
            serde_json::to_value(vec![instrument.to_string()]).unwrap(),
            None,
        );

        let packet = ProtocolFns::protocol_1_encoder(&msg);
        self.send_message(&packet);
        let response = self
            .get_my_packet_with_verification(&msg, None)
            .map_err(|e| format!("Failed to get subscription confirmation: {}", e))?;

        if response.error.is_some() {
            return Err(format!(
                "Server error when subscribing to instrument: {:?}",
                response.error
            ));
        }

        let subscription: SubscriptionResponse = serde_json::from_value(response.data)
            .map_err(|e| format!("Failed to parse subscription response: {}", e))?;

        Ok(subscription)
    }

    /// Subscribes to all outcome tokens belonging to a market event (identified by ticker).
    ///
    /// Sends a `subscribe_to_market_by_ticker` request and blocks until confirmation. Returns a
    /// [`SubscriptionResponse`] where `subscribed` contains the CLOB token IDs for all outcomes
    /// in the market — for binary (Up/Down) markets index 0 is the Up token and index 1 is the
    /// Down token. Pass these token IDs to [`fetch_clob_id_information`] to resolve the
    /// `aot_p2_symbol` needed to look up order books, and to [`place_order`] / [`cancel_order`].
    pub fn subscribe_to_event(
        &self,
        market_ticker: &str,
    ) -> Result<SubscriptionResponse, String> {
        let msg = OutBoundMessage::new(
            "subscribe_to_market_by_ticker".to_string(),
            serde_json::to_value(vec![market_ticker.to_string()]).unwrap(),
            None,
        );
        let packet = ProtocolFns::protocol_1_encoder(&msg);
        self.send_message(&packet);
        let response = self
            .get_my_packet_with_verification(&msg, None)
            .map_err(|e| format!("Failed to get subscription confirmation: {}", e))?;

        if response.error.is_some() {
            return Err(format!(
                "Server error when subscribing to event: {:?}",
                response.error
            ));
        }

        let subscription: SubscriptionResponse = serde_json::from_value(response.data)
            .map_err(|e| format!("Failed to parse subscription response: {}", e))?;

        Ok(subscription)
    }


    /// Sends an `unsubscribe` request and blocks until confirmation. Returns
    /// an [`UnsubscriptionResponse`] confirming which tokens were successfully unsubscribed. After
    /// a successful unsubscription, the server will stop streaming Protocol 2 order book packets
    /// for the outcome tokens in this market. Any pending order book updates in the map from
    /// [`get_order_book`] will not be updated going forward.
    pub fn unsubscribe_from_instrument(&self, instrument: &str) -> Result<UnsubscriptionResponse, String> {
        let msg = OutBoundMessage::new(
            "unsubscribe".to_string(),
            serde_json::to_value(vec![instrument.to_string()]).unwrap(),
            None,
        );
        let packet = ProtocolFns::protocol_1_encoder(&msg);
        self.send_message(&packet);
        let response = self
            .get_my_packet_with_verification(&msg, None);

        if response.is_err() {
            return Err(format!("Failed to get unsubscription confirmation: {}", response.err().unwrap()));
        }

        let unsubscription: UnsubscriptionResponse = serde_json::from_value(response.unwrap().data)
            .map_err(|e| format!("Failed to parse unsubscription response: {}", e))?;

        Ok(unsubscription)

    }

    /// Unsubscribes from all outcome tokens belonging to a market event (identified by ticker).
    /// Sends an `unsubscribe_from_market_by_ticker` request and blocks until confirmation. Returns
    /// an [`UnsubscriptionResponse`] confirming which tokens were successfully unsubscribed. After
    /// a successful unsubscription, the server will stop streaming Protocol 2 order book packets
    /// for the outcome tokens in this market. Any pending order book updates in the map from
    /// [`get_order_book`] will not be updated going forward.
    pub fn unsubscribe_from_event(&self, market_ticker: &str) -> Result<UnsubscriptionResponse, String> {
        let msg = OutBoundMessage::new(
            "unsubscribe_from_market_by_ticker".to_string(),
            serde_json::to_value(vec![market_ticker.to_string()]).unwrap(),
            None,
        );
        let packet = ProtocolFns::protocol_1_encoder(&msg);
        self.send_message(&packet);
        let response = self
            .get_my_packet_with_verification(&msg, None);

        if response.is_err() {
            return Err(format!("Failed to get unsubscription confirmation: {}", response.err().unwrap()));
        }

        let unsubscription: UnsubscriptionResponse = serde_json::from_value(response.unwrap().data)
            .map_err(|e| format!("Failed to parse unsubscription response: {}", e))?;

        Ok(unsubscription)

    }

    /// Searches for market tickers whose names contain `query`.
    ///
    /// Sends a `search_markets` request to the server and blocks until the response arrives.
    /// Returns an owned `Vec<String>` of matching ticker strings (e.g. `"btc-updown-5m-1746000000"`).
    /// `limit` caps the number of results; defaults to 10 if `None`. Pass a larger limit (e.g. 200)
    /// when searching for epoch-based markets where many windows may match the prefix.
    pub fn search_for_markets(
        &self,
        query: &str,
        limit: Option<usize>,
    ) -> Result<Vec<String>, String> {
        let limit_to_use = limit.unwrap_or(10);
        let msg = OutBoundMessage::new(
            "search_markets".to_string(),
            serde_json::to_value(vec![query.to_string(), limit_to_use.to_string()]).unwrap(),
            None,
        );

        let packet = ProtocolFns::protocol_1_encoder(&msg);
        self.send_message(&packet);
        let response = self
            .get_my_packet_with_verification(&msg, None)
            .map_err(|e| format!("Failed to get search results: {}", e))?;

        if response.error.is_some() {
            return Err(format!(
                "Server error when searching for markets: {:?}",
                response.error
            ));
        }

        let markets: Vec<String> = serde_json::from_value(response.data)
            .map_err(|e| format!("Failed to parse market search results: {}", e))?;

        Ok(markets)
    }


    /// Returns the current strike price (price-to-beat) for a market.
    ///
    /// Sends a `get_price_to_beat` request and blocks until the response arrives. The returned
    /// `f64` is the fixed reference price for the market — the asset price at the time the
    /// contract opened. It does not change over the life of the contract.
    pub fn get_price_to_beat(&self, market_ticker: &str) -> Result<f64, String> {
        let msg = OutBoundMessage::new(
            "get_price_to_beat".to_string(),
            serde_json::to_value(vec![market_ticker.to_string()]).unwrap(),
            None,
        );
        let packet = ProtocolFns::protocol_1_encoder(&msg);
        self.send_message(&packet);
        let response = self
            .get_my_packet_with_verification(&msg, None)
            .map_err(|e| format!("Failed to get price to beat: {}", e))?;

        if response.error.is_some() {
            return Err(format!(
                "Server error when getting price to beat: {:?}",
                response.error
            ));
        }

        let price: f64 = serde_json::from_value(response.data)
            .map_err(|e| format!("Failed to parse price to beat: {}", e))?;

        Ok(price)
    }

    /// Measures the round-trip latency from this client to the Argus server in milliseconds.
    ///
    /// Sends a `ping` request, records the wall-clock time before and after, and returns the
    /// difference. The returned `u128` is the total elapsed milliseconds for the request/response
    /// cycle over the local TCP connection to Argus, not the latency to the Polymarket exchange.
    /// Use [`rtt_to_exchange_from_server`] for exchange latency.
    pub fn rtt_to_server(&self) -> Result<u128, String> {
        let time_now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis();

        let msg = OutBoundMessage::new(
            "ping".to_string(),
            serde_json::to_value(Vec::<String>::new()).unwrap(),
            None,
        );

        let packet = ProtocolFns::protocol_1_encoder(&msg);
        self.send_message(&packet);
        let recv = self
            .get_my_packet_with_verification(&msg, None)
            .expect("Failed to get pong response");

        if recv.error.is_some() {
            return Err(format!("Error from server when pinging: {:?}", recv.error));
        }

        let time_after = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis();

        Ok(time_after - time_now)
    }

    /// Returns the Argus server's measured round-trip latency to the Polymarket exchange in milliseconds.
    ///
    /// Sends an `rtt_to_exchange` request; the server pings the exchange and reports back its own
    /// measured latency. The returned `u128` is that server-measured value converted to whole
    /// milliseconds. This reflects the network distance between the Argus host and Polymarket's
    /// infrastructure, not the latency from this client.
    pub fn rtt_to_exchange_from_server(&self) -> Result<u128, String> {
        let msg = OutBoundMessage::new(
            "rtt_to_exchange".to_string(),
            serde_json::to_value(Vec::<String>::new()).unwrap(),
            None,
        );

        let packet = ProtocolFns::protocol_1_encoder(&msg);
        self.send_message(&packet);
        let response = self
            .get_my_packet_with_verification(&msg, None)
            .expect("Failed to get RTT to exchange");

        if response.error.is_some() {
            return Err(format!(
                "Error from server when getting RTT to exchange: {:?}",
                response.error
            ));
        }

        let rtt_to_exchange: f64 = serde_json::from_value(response.data)
            .expect("Failed to parse RTT to exchange from response data");

        Ok((rtt_to_exchange * 1000.0) as u128)
    }

    /// Fetches metadata for a CLOB token ID.
    ///
    /// Sends a `fetch_clob_id_information` request and blocks until the response arrives. Returns
    /// an owned [`CLOBInfo`] containing the human-readable identifiers for the token: event name,
    /// market name, outcome label (e.g. `"Yes"` / `"Up"`), ticker, market slug, and — critically —
    /// `aot_p2_symbol`, which is the key used to look up this token's order book in the map
    /// returned by [`get_order_book`].
    pub fn fetch_clob_id_information(&self, clob_id: &str) -> Result<CLOBInfo, String> {
        let msg = OutBoundMessage::new(
            "fetch_clob_id_information".to_string(),
            serde_json::to_value(vec![clob_id.to_string()]).unwrap(),
            None,
        );

        let packet = ProtocolFns::protocol_1_encoder(&msg);
        self.send_message(&packet);
        let response = self
            .get_my_packet_with_verification(&msg, None)
            .expect("Failed to get clob id information");

        if response.error.is_some() {
            return Err(format!(
                "Error from server when fetching clob id information: {:?}",
                response.error
            ));
        }

        let clob_info = serde_json::from_value(response.data);

        if clob_info.is_ok() {
            Ok(clob_info.unwrap())
        } else {
            Err(format!(
                "Failed to parse CLOB info from response data: {:?}",
                clob_info.err()
            ))
        }
    }

    /// Returns the account's current USDC cash balance.
    ///
    /// Sends a `get_balance` request and blocks until the response arrives. The returned `f64`
    /// is the total liquid USDC balance — it does not account for capital already committed to
    /// open orders. Query at startup to determine trading capital; do not poll this in a hot loop.
    pub fn get_balance(&self) -> Result<f64, String> {
        let msg = OutBoundMessage::new(
            "get_balance".to_string(),
            serde_json::to_value(Vec::<String>::new()).unwrap(),
            None,
        );

        let packet = ProtocolFns::protocol_1_encoder(&msg);
        self.send_message(&packet);
        let response = self
            .get_my_packet_with_verification(&msg, None)
            .expect("Failed to get balance");

        if response.error.is_some() {
            return Err(format!(
                "Error from server when getting balance: {:?}",
                response.error
            ));
        }

        let balance: f64 = serde_json::from_value(response.data)
            .expect("Failed to parse balance from response data");

        Ok(balance)
    }

    /// Places a single order and returns the exchange's acknowledgement.
    ///
    /// Sends a `place_order` request and blocks until the server returns the placement response.
    /// Returns an owned [`OrderPlacedMsg`] containing the assigned `order_id`, initial `status`,
    /// and fill amounts (`taking_amount`, `making_amount`). An `Err` is returned if the server
    /// reports an error; check [`OrderPlacedMsg::success`] for application-level failures.
    /// For placing two legs atomically, prefer [`place_multiple_orders`].
    pub fn place_order(&self, order: PlaceOrder) -> Result<OrderPlacedMsg, String> {
        let msg = OutBoundMessage::new(
            "place_order".to_string(),
            serde_json::to_value(order).unwrap(),
            None,
        );

        let packet = ProtocolFns::protocol_1_encoder(&msg);
        self.send_message(&packet);
        let response = self
            .get_my_packet_with_verification(&msg, None)
            .expect("Failed to get order confirmation");

        if response.error.is_some() {
            Err(format!(
                "Error from server when placing order: {:?}",
                response.error
            ))
        } else {
            Ok(serde_json::from_value(response.data).unwrap())
        }
    }

    /// Places multiple orders in a single request and returns the acknowledgements for all successes.
    ///
    /// Sends a `place_multiple_orders` request and blocks until the server responds. Returns an
    /// owned `Vec<OrderPlacedMsg>` containing one entry per successfully placed order. Orders that
    /// failed on the server side are silently dropped from the success list (the server returns
    /// them in a `failed` field that this method discards). Inspect each [`OrderPlacedMsg`]'s
    /// `error_msg` field for per-order application-level failures.
    pub fn place_multiple_orders(
        &self,
        orders: Vec<PlaceOrder>,
    ) -> Result<Vec<OrderPlacedMsg>, String> {
        let raw_msg =
            HashMap::from([("orders".to_string(), serde_json::to_value(orders).unwrap())]);

        let msg = OutBoundMessage::new(
            "place_multiple_orders".to_string(),
            serde_json::to_value(raw_msg).unwrap(),
            None,
        );

        let packet = ProtocolFns::protocol_1_encoder(&msg);
        self.send_message(&packet);
        let response = self
            .get_my_packet_with_verification(&msg, None)
            .expect("Failed to get order confirmation");

        if response.error.is_some() {
            Err(format!(
                "Error from server when placing multiple orders: {:?}",
                response.error
            ))
        } else {
            let parsed: PlaceMultipleOrdersResponse =
                serde_json::from_value(response.data).expect("Failed to parse order confirmation");
            Ok(parsed.success)
        }
    }

    /// Cancels a single open order by its order ID.
    ///
    /// Sends a `cancel_order` request and blocks until the response arrives. Returns an owned
    /// [`OrderCancelled`] whose `canceled` vec contains the IDs that were successfully cancelled
    /// and `not_canceled` contains any that were not (e.g. already filled). An `Err` is returned
    /// if the server reports a transport-level error. For cancelling several orders at once,
    /// prefer [`cancel_multiple_orders`] to avoid serial round-trips.
    pub fn cancel_order(&self, order_id: &str) -> Result<OrderCancelled, String> {
        let mut order_dict: HashMap<String, String> = HashMap::new();
        order_dict.insert("order_id".to_string(), order_id.to_string());
        let msg = OutBoundMessage::new(
            "cancel_order".to_string(),
            serde_json::to_value(order_dict).unwrap(),
            None,
        );
        let send_packet = ProtocolFns::protocol_1_encoder(&msg);
        self.send_message(&send_packet);
        let response = self.get_my_packet_with_verification(&msg, None);

        match response {
            Ok(response) => {
                if response.error.is_some() {
                    return Err(format!(
                        "Error from server when cancelling order: {:?}",
                        response.error
                    ));
                }

                println!("{:?}", response.data);

                let data: HashMap<String, Value> = serde_json::from_value(response.data)
                    .expect("Failed to parse order cancellation response data");

                let not_canceled = data
                    .get("not_canceled")
                    .expect("Failed to get `not_canceled` key from order cancellation response data")
                    .clone();

                let raw_cancelled = data
                    .get("canceled")
                    .expect("Unable to get `canceled` key from order cancellation response data")
                    .clone();
                let canceled: Vec<String> = serde_json::from_value::<Vec<String>>(raw_cancelled)
                    .unwrap()
                    .clone();

                Ok(OrderCancelled {
                    not_canceled,
                    canceled,
                })
            }
            Err(e) => Err(e),
        }
    }

    /// Cancels multiple open orders in a single request.
    ///
    /// Sends a `cancel_multiple_orders` request and blocks until the response arrives. Returns
    /// an owned [`CancelledMultipleOrdersResponse`] whose `canceled` vec contains the successfully
    /// cancelled order IDs and `not_canceled` is a map of order ID → reason for any that could
    /// not be cancelled (e.g. already matched).
    pub fn cancel_multiple_orders(&self, order_ids: Vec<String>) -> Result<CancelledMultipleOrdersResponse, String> {
        let mut order_dict: HashMap<String, Vec<String>> = HashMap::new();
        order_dict.insert("order_ids".to_string(), order_ids);

        let msg = OutBoundMessage::new(
            "cancel_multiple_orders".to_string(),
            serde_json::to_value(order_dict).unwrap(),
            None,
        );

        let packet = ProtocolFns::protocol_1_encoder(&msg);
        self.send_message(&packet);
        let response = self.get_my_packet_with_verification(&msg, None)
            .expect("Failed to get order cancellation response");

        if response.error.is_some() {
            Err(format!("Error from server when cancelling multiple orders: {:?}", response.error))
        } else {
            let data = response.data;
            let obj: CancelledMultipleOrdersResponse = serde_json::from_value(data)
                .expect("Failed to parse order cancellation response data");
            Ok(obj)
        }
    }

    /// Returns all currently open orders on the account.
    ///
    /// Sends a `get_orders` request and blocks until the response arrives. Returns an owned
    /// `Vec<PolyMarketOrder>`, each representing one live resting order. The list reflects the
    /// exchange's current state at the moment of the request; it is not a live-updating handle.
    /// For real-time fill/cancel notifications use the system messages buffer from [`get_sys_msgs`].
    pub fn get_orders(&self) -> Result<Vec<PolyMarketOrder>, String> {
        let msg = OutBoundMessage::new(
            "get_orders".to_string(),
            serde_json::to_value(Vec::<String>::new()).unwrap(),
            None,
        );
        let packet = ProtocolFns::protocol_1_encoder(&msg);
        self.send_message(&packet);
        let response = self
            .get_my_packet_with_verification(&msg, None)
            .map_err(|e| format!("Failed to get orders: {}", e))?;

        if response.error.is_some() {
            return Err(format!("Server error when fetching orders: {:?}", response.error));
        }

        serde_json::from_value(response.data)
            .map_err(|e| format!("Failed to parse orders response: {}", e))
    }

    /// Fetches the current exchange-side status of a single order.
    ///
    /// Sends a `get_order_status` request and blocks until the response arrives. Returns an owned
    /// [`PolyMarketOrder`] snapshot from the exchange at the time of the request. Use this as a
    /// REST fallback when the WebSocket `account_update` stream may have been delayed — for
    /// example, immediately after placement to confirm the order was received, or to reconcile
    /// fill amounts that the local book state has not yet reflected.
    pub fn get_order_status(&self, order_id: &str) -> Result<PolyMarketOrder, String> {
        let mut order_dict: HashMap<String, String> = HashMap::new();
        order_dict.insert("order_id".to_string(), order_id.to_string());
        let msg = OutBoundMessage::new(
            "get_order_status".to_string(),
            serde_json::to_value(order_dict).unwrap(),
            None,
        );
        let packet = ProtocolFns::protocol_1_encoder(&msg);
        self.send_message(&packet);
        let response = self
            .get_my_packet_with_verification(&msg, None)
            .map_err(|e| format!("Failed to get order status: {}", e))?;

        if response.error.is_some() {
            return Err(format!("Server error when fetching order status: {:?}", response.error));
        }

        serde_json::from_value(response.data)
            .map_err(|e| format!("Failed to parse order status response: {}", e))
    }

    /// Returns the account's current balance for a specific outcome token.
    ///
    /// Sends a `get_token_balance` request and blocks until the response arrives. The returned
    /// `f64` is the number of shares of the given outcome token held in the account. Useful for
    /// reconciling local position tracking against the exchange after fills, especially when
    /// fee deductions push the actual balance slightly below the expected filled amount.
    pub fn get_token_balance(&self, token_id: &str) -> Result<f64, String> {
        let mut args: HashMap<String, String> = HashMap::new();
        args.insert("token_id".to_string(), token_id.to_string());
        let msg = OutBoundMessage::new(
            "get_token_balance".to_string(),
            serde_json::to_value(args).unwrap(),
            None,
        );
        let packet = ProtocolFns::protocol_1_encoder(&msg);
        self.send_message(&packet);
        let response = self
            .get_my_packet_with_verification(&msg, None)
            .map_err(|e| format!("Failed to get token balance: {}", e))?;

        if response.error.is_some() {
            return Err(format!(
                "Server error when fetching token balance: {:?}",
                response.error
            ));
        }

        serde_json::from_value(response.data)
            .map_err(|e| format!("Failed to parse token balance response: {}", e))
    }
}
