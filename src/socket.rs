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

    pub fn get_order_book(&self) -> Arc<RwLock<HashMap<String, OrderBook>>> {
        self.order_books.clone()
    }

    pub fn get_order_book_event(&self) -> Arc<Event> {
        self.market_event.clone()
    }

    pub fn get_sys_msgs(&self) -> Arc<RwLock<SystemMessagesPushed>> {
        self.system_messages_pushed.clone()
    }

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

#[allow(dead_code)]
impl MarketDataConnection {
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
            return Err(format!("Error from server when cancelling multiple orders: {:?}", response.error));
        } else {
            let data = response.data;
            let obj: CancelledMultipleOrdersResponse = serde_json::from_value(data)
                .expect("Failed to parse order cancellation response data");
            Ok(obj)
        }
    }

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
