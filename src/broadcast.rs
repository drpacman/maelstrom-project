use std::io::{self, Write};
use serde::{Deserialize};
use serde_json::{Value, Map, json};
mod node;
use crate::node::node::{Node, Server, Message};
                                
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

struct BroadcastServer {
    node : Option<Node>,
    msgs : Vec<u32>,
    neighbours : Vec<Value>
    
}

impl Server for BroadcastServer {
    fn process_reply(&self) {}
    fn start(&mut self, node : Node) {
        self.node = Some(node);
    }
    fn process_message(&mut self, msg : Message) {
        let node = self.node.as_ref().unwrap();
        match msg.body["type"].as_str() {
            Some("topology") => {
                let topology : Topology = serde_json::from_value(msg.body.clone()).unwrap();
                self.neighbours = topology.topology[&node.node_id].as_array().unwrap().clone();
                node.send( topology.response(), &msg );
            },
            Some("broadcast") => {
                let broadcast : Broadcast = serde_json::from_value(msg.body.clone()).unwrap();
                let m = broadcast.message.clone();
                if !self.msgs.contains(&m) {
                    self.msgs.push(m);
                    self.msgs.sort();
                    debug(format!("Messages at node {} is {:?}\n", &node.node_id, &self.msgs));
                    for neighbour in self.neighbours.iter() {
                        if neighbour.as_str().unwrap() != msg.src {
                            node.send_to_node_async(json!({ "type" : "broadcast", "message": m.clone() }), neighbour.to_string());
                        }
                    }
                }
                node.send( broadcast.response(), &msg )
            },
            Some("read") => {
                let read : Read = serde_json::from_value(msg.body.clone()).unwrap();
                node.send( read.response(&self.msgs), &msg )
            },
            _ => {}
        }            
    }
    fn notify(&mut self) {}
}

fn main() -> io::Result<()> {
    let server = BroadcastServer { 
        node: None,
        msgs : Vec::new(),
        neighbours: Vec::new()
    };
    node::node::run(server);
    Ok(())
}

