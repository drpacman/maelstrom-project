use std::io::{self, Write};
use serde::{Deserialize};
use serde_json::{Value, Map, json};
mod node;
use crate::node::node::{Node, Init, Message};
                                
#[derive(Deserialize)]
struct Topology {
    #[serde(rename="type")]
    #[allow(dead_code)]
    type_ : String,
    topology: Map<String, Value>,
    msg_id: u32
}

impl Topology {
    fn response(self) -> Value {
        return json!({ "type" : "topology_ok", "in_reply_to": self.msg_id })
    }
}

#[derive(Deserialize)]
struct Broadcast {
    #[serde(rename="type")]
    #[allow(dead_code)]
    type_ : String,
    message: u32,
    msg_id: u32
}

impl Broadcast {
    fn response(self) -> Value {
        return json!({ "type" : "broadcast_ok", "in_reply_to": self.msg_id })
    }
}

#[derive(Deserialize)]
struct Read {
    #[serde(rename="type")]
    #[allow(dead_code)]
    type_ : String,
    msg_id: u32
}

impl Read {
    fn response(self, messages : &Vec<u32>) -> Value {
        return json!({"type" : "read_ok", "in_reply_to": self.msg_id, "messages": messages})
    }
}

fn debug(msg : String) {
    io::stderr().write(msg.as_bytes()).expect("Failed to write debug");
}

fn main() -> io::Result<()> {
    let mut server : Option<Box<Node>> = None;
    let mut msgs : Vec<u32> = Vec::new();
    let mut neighbours : Vec<Value> = Vec::new();
    loop {
        let mut buffer = String::new();
        match io::stdin().read_line(&mut buffer) {
            Ok(_n) => {
                match serde_json::from_str::<Message>(buffer.as_str()) {
                    Ok(msg) => {
                        debug(format!("Received {}\n", msg.body));
                        match msg.body["type"].as_str() {
                            Some("init") => {
                                let init : Init = serde_json::from_value(msg.body.clone()).unwrap();
                                let s : Node = Node::new(&init);
                                s.send( init.response(), &msg );
                                server = Some(Box::new(s))                                
                            },
                            Some("topology") => {
                                let topology : Topology = serde_json::from_value(msg.body.clone()).unwrap();
                                match &server {
                                    Some(s) => {
                                        neighbours = topology.topology[&s.node_id].as_array().unwrap().clone();
                                        s.send( topology.response(), &msg );
                                    },
                                    None => panic!("Missing server")
                                }
                            },
                            Some("broadcast") => {
                                let broadcast : Broadcast = serde_json::from_value(msg.body.clone()).unwrap();
                                match &server {
                                    Some(s) => {
                                        let m = broadcast.message.clone();
                                        if !msgs.contains(&m) {
                                            msgs.push(m);
                                            msgs.sort();
                                            debug(format!("Messages at node {} is {:?}\n", &s.node_id, msgs));
                                            for neighbour in neighbours.iter() {
                                                if neighbour.as_str().unwrap() != msg.src {
                                                    s.send_to_node(json!({ "type" : "broadcast", "message": m.clone() }), &neighbour.as_str().unwrap());
                                                }
                                            }
                                        }
                                        s.send( broadcast.response(), &msg )
                                    },
                                    None => panic!("Missing server")
                                }
                            },
                            Some("read") => {
                                let read : Read = serde_json::from_value(msg.body.clone()).unwrap();
                                match &server {
                                    Some(s) => s.send( read.response(&msgs), &msg ),
                                    None => panic!("Missing server")
                                }
                            }
                            _ => {
                                match msg.body.get("in_reply_to") {
                                    Some(msg_id) => {
                                        match &server {
                                            Some(s) => s.acked(msg_id.as_u64().unwrap()),
                                            None => panic!("Missing server")
                                        }
                                    },
                                    _ => {}
                                }
                            }
                        }
                    },
                    Err(error) => {
                        debug(format!("Invalid JSON {} {}\n", buffer, error));
                    }
                };
                ()
            },
            Err(error) => return Err(error)
        }
    }
}