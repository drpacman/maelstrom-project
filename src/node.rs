pub mod node {

    use json_patch::merge;
    use serde::{Deserialize, Serialize};
    use serde_json::{json, Value};
    use std::cell::Cell;
    use std::io::{self, Write};
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;
    use std::collections::HashMap;

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
        SyncOutbound((Message, mpsc::Sender<Value>)),
        Outbound(Message),
        Reply(Message),
    }

    impl Init {
        pub fn response(&self) -> Value {
            return json!({ "type" : "init_ok", "in_reply_to": self.msg_id });
        }
    }

    #[derive(Serialize, Deserialize, Debug)]
    pub struct Message {
        pub src: String,
        dest: String,
        pub body: Value,
    }

    pub struct Node {
        msg_id: Cell<u64>,
        pub node_ids: Vec<String>,
        pub node_id: String,
        tx: std::sync::mpsc::Sender<Messages>,
        reply_rx: std::sync::mpsc::Receiver<Message>
    }

    pub trait Server {
        fn process_reply(&self);
        fn start(&mut self, node : Node);
        fn process_message(&mut self, msg : Message);
        fn notify(&mut self);
    }

    pub fn run<T : Server>(mut server : T) {
        let (input_tx, input_rx) = mpsc::channel();
        let (reply_tx, reply_rx) = mpsc::channel();
        thread::spawn(move || {
            loop {
                let mut buffer = String::new();
                match io::stdin().read_line(&mut buffer) {
                    Ok(_n) => {
                        match serde_json::from_str::<Message>(buffer.as_str()) {
                            Ok(msg) => {
                                match msg.body.get("in_reply_to") {
                                    Some(_msg_id) => {
                                        reply_tx
                                        .send(msg)
                                        .expect("Failed to send message read from command line");                                        
                                    },
                                    None => {
                                        input_tx
                                        .send(msg)
                                        .expect("Failed to send message read from command line");
                                    }
                                }
                            },
                            Err(error) => debug(format!("Invalid JSON {} {}", buffer, error))
                        }
                    },
                    Err(_error) => panic!("Failed to read from stdin")
                }
            }
        }); 
           
        let mut init = None;
        let mut src = String::new();
        while init.is_none() {
            if let Ok(msg) = input_rx.try_recv() {
                if let Some("init") = msg.body["type"].as_str() {
                    init = Some(serde_json::from_value(msg.body.clone()).unwrap());
                    src = msg.src.to_string();
                }
            };
        }
        let node : Node = Node::new(&init.unwrap(), src, reply_rx);
        server.start(node);                        

        loop {                    
            server.process_reply();
            if let Ok(msg) = input_rx.try_recv() {
                debug(format!("\nReceived Server Message - {:?}",msg));
                server.process_message(msg);
            };
            server.notify();
        }
    }

    fn send_message(resp: &Message) -> () {
        io::stdout()
            .write(format!("{}\n", serde_json::to_string(resp).unwrap()).as_bytes())
            .expect("Failed to send response on stdout");
    }

    impl Node {
        pub fn new(init: &Init, src: String, reply_rx: std::sync::mpsc::Receiver<Message>) -> Node {
            let (tx, rx) = mpsc::channel();

            thread::spawn(move || {
                let mut unacked: HashMap<u64, (Message, Option<mpsc::Sender<Value>>)> = HashMap::new();
                loop {
                    loop {
                        match rx.try_recv() {
                            Ok(Messages::Outbound(m)) => {
                                let msg_id = m.body["msg_id"].as_u64().expect("Msg Id is not a u64");
                                unacked.insert(msg_id, (m, None));
                            },
                            Ok(Messages::SyncOutbound((m, sender))) => {
                                let msg_id = m.body["msg_id"].as_u64().expect("Msg Id is not a u64");
                                unacked.insert(msg_id, (m, Some(sender)));
                            },
                            Ok(Messages::Reply(m)) => {
                                let msg_id = m.body["in_reply_to"].as_u64().expect("In reply to is not a u64");
                                if let Some((_m, Some(sender))) = unacked.get(&msg_id) {
                                    sender
                                    .send(m.body)
                                    .expect("Failed to send message body");                                    
                                };
                                unacked.remove(&msg_id);
                            }
                            Err(_e) => break,
                        }
                    }
                    if unacked.len() > 0 {
                        for (_m,(m, _sender)) in unacked.iter() {
                            debug(format!("\nSending {:?}", &m));
                            send_message(&m);
                        }
                    }
                    thread::sleep(Duration::from_millis(10));
                }
            });

            let node = Node {
                msg_id: Cell::new(0),
                node_id: init.node_id.clone(),
                node_ids: init.node_ids.clone(),
                tx: tx,
                reply_rx: reply_rx
            };
            let msg = node.create_message_to_send(init.response(), src);
            send_message(&msg);
            node
        }

        pub fn send(&self, resp: Value, msg: &Message) {
            send_message(&self.create_message_to_send(resp, msg.src.to_string()));
        }

        pub fn send_to_node_noack(&self, resp: Value, dest: String) {
            send_message(&self.create_message_to_send(resp, dest));
        }

        pub fn send_to_node_async(&self, resp: Value, dest: String) {
            self.tx
                .send(Messages::Outbound(self.create_message_to_send(resp, dest)))
                .expect("Tx send failed");
        }

        pub fn send_to_node_sync(&self, resp: Value, dest: String) -> Value {
            let (tx, rx) = mpsc::channel();
            self.tx
                .send(Messages::SyncOutbound((self.create_message_to_send(resp, dest), tx)))
                .expect("Tx send failed");
            loop {
                match rx.try_recv() {
                    Ok(resp) => return resp,
                    _ => self.process_reply()
                }
            }
        }

        pub fn acked(&self, m : Message) {
            self.tx
                .send(Messages::Reply(m))
                .expect("Tx send failed");
        }

        fn create_message_to_send(&self, resp: Value, dest: String) -> Message {
            self.msg_id.set(self.msg_id.get() + 1);
            let mut body = json!({ "msg_id": self.msg_id });
            merge(&mut body, &resp);
            Message {
                src: self.node_id.clone(),
                dest: dest,
                body: body,
            }
        }

        pub fn process_reply(&self) {
            if let Ok(msg) = self.reply_rx.try_recv() {
                &self.acked(msg);
            }
        }
    }

    pub fn debug(msg: String) {
        io::stderr().write(msg.as_bytes()).expect("Failed to write debug");
    }
}
