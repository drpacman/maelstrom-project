pub mod node {

    use json_patch::merge;
    use serde::{Deserialize, Serialize};
    use serde_json::{json, Value};
    use std::cell::Cell;
    use std::io::{self, Write};
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

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
        Outbound(Message),
        Reply(u64),
    }

    impl Init {
        pub fn response(self) -> Value {
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
    }

    fn send_message(resp: &Message) -> () {
        debug(format!("Sending {:?}\n", resp));
        io::stdout()
            .write(format!("{}\n", serde_json::to_string(resp).unwrap()).as_bytes())
            .expect("Failed to send response on stdout");
    }

    impl Node {
        pub fn new(init: &Init) -> Node {
            let (tx, rx) = mpsc::channel();

            thread::spawn(move || {
                let mut unacked: Vec<Message> = Vec::new();
                loop {
                    loop {
                        match rx.try_recv() {
                            Ok(Messages::Outbound(m)) => unacked.push(m),
                            Ok(Messages::Reply(msg_id)) => {
                                debug(format!("\nThere are {} unacked messages remaining - received ack for message Id {}", unacked.len(), msg_id));
                                unacked.retain(|m| &m.body["msg_id"].as_u64().unwrap() != &msg_id);
                                debug(format!("\nUnacked remaining {}", unacked.len()));
                            }
                            Err(_e) => break,
                        }
                    }
                    if unacked.len() > 0 {
                        debug(format!("Sending {} messages\n", unacked.len()));
                        for m in unacked.iter() {
                            send_message(&m);
                        }
                    }
                    thread::sleep(Duration::from_millis(50));
                }
            });

            Node {
                msg_id: Cell::new(0),
                node_id: init.node_id.clone(),
                node_ids: init.node_ids.clone(),
                tx: tx,
            }
        }

        pub fn send(&self, resp: Value, msg: &Message) {
            send_message(&self.create_message_to_send(resp, msg.src.as_str()));
        }

        pub fn send_to_node(&self, resp: Value, dest: &str) {
            self.tx
                .send(Messages::Outbound(self.create_message_to_send(resp, dest)))
                .expect("Tx send failed");
        }

        pub fn send_to_node_noack(&self, resp: Value, dest: &str) {
            send_message(&self.create_message_to_send(resp, dest));
        }

        pub fn acked(&self, msg_id: u64) {
            self.tx
                .send(Messages::Reply(msg_id))
                .expect("Tx send failed");
        }

        fn create_message_to_send(&self, resp: Value, dest: &str) -> Message {
            self.msg_id.set(self.msg_id.get() + 1);
            let mut body = json!({ "msg_id": self.msg_id });
            merge(&mut body, &resp);
            Message {
                src: self.node_id.clone(),
                dest: dest.to_string(),
                body: body,
            }
        }
    }

    fn debug(msg: String) {
        io::stderr()
            .write(msg.as_bytes())
            .expect("Failed to write debug");
    }
}
