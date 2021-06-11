#[allow(dead_code)]
pub mod node {

    use json_patch::merge;
    use serde::{Deserialize, Serialize};
    use serde_json::{json, Value};
    use std::cell::Cell;
    use std::io::{self, Write};
    use std::sync::mpsc::{Receiver, Sender, channel};
    use std::thread;
    use std::time::{ Duration, Instant};
    use std::collections::HashMap;

    const MAX_SYNC_WAIT : u128 = 5000;
    const REPLY_TIMEOUT : u128 = 500;

    #[derive(Deserialize)]
    pub struct Init {
        #[serde(rename = "type")]
        #[allow(dead_code)]
        type_: String,
        msg_id: u64,
        node_id: String,
        node_ids: Vec<String>,
    }

    enum Messages {
        Outbound(Message, u8, Sender<Message>),
        Reply(Message),
    }

    impl Init {
        pub fn response(&self) -> Value {
            return json!({ "type" : "init_ok", "in_reply_to": self.msg_id });
        }
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct Message {
        pub src: String,
        pub dest: String,
        pub body: Value,
    }
    
    pub struct UnackedMessage {
        msg : Message,
        response_handler_tx : Sender<Message>,
        last_try : Option<Instant>,
        attempts : u8
    }

    #[derive(Debug)]
    pub struct Node {
        msg_id: Cell<u64>,
        pub node_ids: Vec<String>,
        pub node_id: String,
        server_reply_tx: Sender<Message>,
        reply_rx: Receiver<Message>,
        acks_tx: Sender<Messages>
    }

    pub trait Server {
        fn start(&mut self, node : Node);
        fn get_node_ref(&self) -> &Node;
        fn process_message(&mut self, msg : Message);
        fn process_reply(&mut self, msg : Message){}
        fn notify(&mut self);
    
        fn run(&mut self) {
            let (server_input_tx, server_input_rx) = channel();
            let (server_reply_tx, server_reply_rx) = channel();
            let (reply_tx, reply_rx) = channel();
            let command_line_tx = server_input_tx.clone();
            thread::spawn(move || {
                loop {
                    let mut buffer = String::new();
                    match io::stdin().read_line(&mut buffer) {
                        Ok(_n) => {
                            match serde_json::from_str::<Message>(buffer.as_str()) {
                                Ok(msg) => {
                                    debug(format!("Received message on the wire- {:?}",msg));
                                    match msg.body.get("in_reply_to") {
                                        Some(_msg_id) => {
                                            reply_tx
                                            .send(msg)
                                            .expect("Failed to send message read from command line");                                        
                                        },
                                        None => {
                                            command_line_tx
                                            .send(msg)
                                            .expect("Failed to send message read from command line");
                                        }
                                    }
                                },
                                Err(error) => {
                                    debug(format!("Invalid JSON {} {}", buffer, error))
                                }
                            }
                        },
                        Err(_error) => panic!("Failed to read from stdin")
                    }
                }
            }); 
            
            loop {
                if let Ok(msg) = server_input_rx.try_recv() {
                    if let Some("init") = msg.body["type"].as_str() {
                        let init : Init = serde_json::from_value(msg.body.clone()).unwrap();
                        let node : Node = Node::new(&init, reply_rx, server_reply_tx.clone());
                        node.send_reply(init.response(), &msg);
                        self.start(node);                        
                        break;
                    }
                };
            }
            
            loop {                    
                self.get_node_ref().process_reply();
                if let Ok(msg) = server_reply_rx.try_recv() {
                    debug(format!("Received Reply Message - {:?}",msg));
                    self.process_reply(msg);
                }
                if let Ok(msg) = server_input_rx.try_recv() {
                    debug(format!("Received Server Message - {:?}",msg));
                    self.process_message(msg);
                };
                self.notify();
            }
        }
    }

    fn send_message(resp: &Message) -> () {
        debug(format!("Sending {:?}", resp));
        io::stdout()
            .write(format!("{}\n", serde_json::to_string(resp).unwrap()).as_bytes())
            .expect("Failed to send response on stdout");
    }

    pub fn debug(msg: String) {
        io::stderr().write(format!("\n{}",msg).as_bytes()).expect("Failed to write debug");
    }

    impl Node {
        pub fn new(init: &Init, reply_rx: Receiver<Message>, server_reply_tx: Sender<Message>) -> Node {
            let (acks_tx, acks_rx) = channel();
            
            // retry thread for "reliable" messages
            thread::spawn(move || {
                let mut unacked: HashMap<u64, UnackedMessage> = HashMap::new();
                loop {
                    loop {
                        match acks_rx.try_recv() {
                            Ok(Messages::Outbound(m, attempts, response_handler_tx)) => {
                                let msg_id = m.body["msg_id"].as_u64().expect("Msg Id is not a u64");
                                unacked.insert(msg_id, UnackedMessage { msg: m, last_try: None, attempts: attempts, response_handler_tx: response_handler_tx });
                            },
                            Ok(Messages::Reply(m)) => {
                                debug(format!("Got reply {:?}", m));
                                let msg_id = m.body["in_reply_to"].as_u64().expect("In reply to is not a u64");
                                if let Some(unacked_msg) = unacked.get(&msg_id) {
                                    debug(format!("Handling response for {}", &msg_id));
                                    unacked_msg.response_handler_tx.send(m).expect("Failed to send message");                                    
                                }; 
                                unacked.remove(&msg_id);
                            }
                            Err(_e) => break,
                        }
                    }
                    if unacked.len() > 0 {
                        let mut expired_entries = Vec::new();
                        for (key, unacked) in unacked.iter_mut() {
                            if unacked.last_try.is_none() || unacked.last_try.unwrap().elapsed().as_millis() > REPLY_TIMEOUT {
                                if unacked.attempts > 0 {
                                    // debug(format!("Initial Send? {} - Sending {:?}", unacked.last_try.is_none(), &unacked.msg));
                                    send_message(&unacked.msg);
                                    unacked.last_try = Some(Instant::now());
                                    unacked.attempts -= 1;
                                } else {
                                    expired_entries.push(key.clone());
                                }
                            }
                        }
                        // drop all expired entries
                        for msg_id in expired_entries {
                            debug(format!("Expiring entry {:?}",msg_id));
                            unacked.remove(&msg_id);
                        }
                    }
                    thread::sleep(Duration::from_millis(10));
                }
            });

            let node = Node {
                msg_id: Cell::new(0),
                node_id: init.node_id.clone(),
                node_ids: init.node_ids.clone(),
                server_reply_tx,
                acks_tx,
                reply_rx
            };
            node
        }

        pub fn other_node_ids(&self) -> Vec<String> {
            self.node_ids.clone().into_iter().filter(|id| id != &self.node_id).collect()
        }
        
        pub fn broadcast(&self, resp: Value) {
            for node_id in self.other_node_ids().iter() {
                send_message(&self.create_message_to_send(&resp, node_id.clone()));
            }
        }

        pub fn send_reply(&self, resp: Value, msg: &Message) {
            let mut reply = json!({ "in_reply_to" : msg.body["msg_id"].as_u64().unwrap() });
            merge(&mut reply, &resp);
            send_message(&self.create_message_to_send(&reply, msg.src.to_string()));
        }

        pub fn send_to_node_noack(&self, resp: Value, dest: String) {
            send_message(&self.create_message_to_send(&resp, dest));
        }

        pub fn broadcast_acked(&self, resp: Value) {
            for node_id in self.other_node_ids().iter() {
                self.send_to_node_acked(&resp, node_id.clone());
            }
        }

        pub fn send_to_node_acked(&self, resp: &Value, dest: String) -> u64 {
            let msg = self.create_message_to_send(resp, dest);
            let msg_id = msg.body["msg_id"].as_u64().unwrap();
            self.acks_tx
                .send(Messages::Outbound(msg, 1, self.server_reply_tx.clone()))
                .expect("Tx send failed");
            msg_id
        }

        pub fn send_to_node_sync(&self, req: Value, dest: String) -> Value {
            // create dedicated channel for the reply for this request
            let (tx, rx) = channel();
            self.acks_tx
                .send(Messages::Outbound(self.create_message_to_send(&req, dest), 3, tx))
                .expect("Tx send failed");
            let start = Instant::now();
            while start.elapsed().as_millis() < MAX_SYNC_WAIT {
                match rx.try_recv() {
                    Ok(msg) => return msg.body,
                    _ => self.process_reply()
                }
            }
            // return an error
            json!({ 
                "type" : "error",
                "code" : 0,
                "text" : "Timed out waiting for reply"
            })
        }

        pub fn process_reply(&self) {
            if let Ok(msg) = self.reply_rx.try_recv() {
                debug(format!("Processing reply {:?}", msg));
                self.acks_tx
                .send(Messages::Reply(msg))
                .expect("Acks tx send failed");
            }
        }

        fn create_message_to_send(&self, resp: &Value, dest: String) -> Message {
            self.msg_id.set(self.msg_id.get() + 1);
            let mut body = resp.clone();
            let msg_properties = json!({ "msg_id": self.msg_id });
            merge(&mut body, &msg_properties);
            Message {
                src: self.node_id.clone(),
                dest: dest,
                body: body,
            }
        }
    }
}
