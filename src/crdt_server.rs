    
pub mod crdt_server {
    use std::io::{self, Write};
    use serde::{Deserialize};
    use serde_json::{Value, json};
    use std::thread;
    use std::time::{ Instant, Duration };
    use std::sync::mpsc;
    use crate::node::node::{Node, Server, Message};                          
    
    #[derive(Deserialize)]
    struct Replicate {
        #[serde(rename="type")]
        #[allow(dead_code)]
        type_ : String,
        value: Value,
        #[allow(dead_code)]
        msg_id: u32
    }

    pub fn debug(msg : String) {
        io::stderr().write(format!("{}\n", msg).as_bytes()).expect("Failed to write debug");
    }

    pub trait CRDT {
        fn to_json(&self) -> Value;
        fn from_json(json : Value) -> Self;
        fn read(&self) -> Value;    
        fn merge(&mut self, other : Self);
    }

    #[derive(Deserialize)]
    struct Read {
        #[serde(rename="type")]
        #[allow(dead_code)]
        type_ : String,
        msg_id: u32
    }

    impl Read {
        fn response(self, contents : Value) -> Value {
            return json!({"type" : "read_ok", "in_reply_to": self.msg_id, "value": contents})
        }
    }

    pub struct CRDTServer<T : CRDT >{
        node : Option<Node>,
        crdt : T,
        handler: fn(&String, &mut T, &Message) -> Value,
        last_replication: Instant
    }

    impl<T : CRDT>  CRDTServer<T>  {
        pub fn new(crdt : T, handler: fn(&String, &mut T, &Message) -> Value ) -> CRDTServer<T> {
            CRDTServer {
                node : None,
                crdt : crdt,
                handler : handler,
                last_replication: Instant::now()
            }
        }

        fn replicate(&self) {
            debug("Replicating".to_string());
            let node_ref = self.node.as_ref().unwrap();
            for neighbour in node_ref.node_ids.iter() {
                if neighbour.to_string() !=  node_ref.node_id {
                    node_ref.send_to_node_noack(
                        json!({ "type" : "replicate", "value": &self.crdt.to_json() }
                    ), &neighbour);
                }
            }
        }        
    }

    impl<T: CRDT + Send + 'static>  Server for CRDTServer<T>  {
    
        fn ack(&mut self, msg_id : u64) {}
        
        fn start(&mut self, node : Node) {
            self.node = Some(node);
            self.last_replication = Instant::now();
        }

        fn notify(&mut self) {
            let now = Instant::now();
            if now.duration_since(self.last_replication).as_secs() > 5 {
                self.last_replication = now;
                self.replicate();
            };
        }    

        fn process_message(&mut self, msg : Message ) {
            let n = self.node.as_ref().unwrap();
            match msg.body.get("in_reply_to") {
                Some(msg_id) => {
                    n.acked(msg_id.as_u64().unwrap());
                },
                _ => {
                    match msg.body["type"].as_str() {
                        Some("replicate") => {
                            let repl : Replicate = serde_json::from_value(msg.body.clone()).unwrap();
                            self.crdt.merge(T::from_json(repl.value));
                        },
                        Some("read") => {
                            let read : Read = serde_json::from_value(msg.body.clone()).unwrap();
                            n.send(read.response(self.crdt.read()), &msg );
                        }
                        _ => {
                            let resp = (self.handler)(&n.node_id, &mut self.crdt, &msg);
                            n.send(resp, &msg);
                        }
                    }
                }            
            }
        }
    }
}