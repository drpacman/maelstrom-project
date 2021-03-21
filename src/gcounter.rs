use std::io::Result;
use serde::{Deserialize};
use serde_json::{Value, Number, json};
use std::collections::HashMap;

mod node; 
use crate::node::node::Message;   
mod crdt_server;                       
use crate::crdt_server::crdt_server::{CRDT, Server, debug};

#[derive(Deserialize)]
struct Add {
    #[serde(rename="type")]
    #[allow(dead_code)]
    type_ : String,
    delta: Number,
    msg_id: u32
}

impl Add {
    fn response(&self) -> Value {
        return json!({ "type" : "add_ok", "in_reply_to": self.msg_id })
    }
}

struct GCounter {
    counters : HashMap<String, i64>
}

impl GCounter {
    fn new() -> GCounter {
        GCounter {
            counters : HashMap::new()
        }
    }
    fn add(&mut self, node_id : String, value : i64){
        let current : i64 = *self.counters.get(&node_id).unwrap_or(&0);
        self.counters.insert(node_id, current + value);
    }    
}

struct PNCounter {
    inc : GCounter,
    dec : GCounter
}

impl PNCounter {
    fn new() -> PNCounter {
        PNCounter {
            inc : GCounter::new(),
            dec : GCounter::new()
        }
    }

    fn add(&mut self, node_id : String, value : i64){
        if value > 0 {
            self.inc.add(node_id, value)
        } else {
            self.dec.add(node_id, -value)
        };
    }
}

impl CRDT for PNCounter {
    fn to_json(&self) -> Value {
        json!({ "inc" : &self.inc.to_json(), "dec" : &self.dec.to_json() })
    }

    fn from_json(json : Value) -> PNCounter {
        PNCounter { inc : GCounter::from_json(json["inc"].clone()), dec : GCounter::from_json(json["dec"].clone())}
    }

    fn read(&self) -> Value { 
        json!(self.inc.read().as_i64().unwrap() - self.dec.read().as_i64().unwrap())
    }

    fn merge(&mut self, other : PNCounter){
        self.inc.merge( other.inc );
        self.dec.merge( other.dec );
    }
}

impl CRDT for GCounter {

    fn to_json(&self) -> Value {
        json!(&self.counters)
    }

    fn from_json(json : Value) -> GCounter {
        let mut counters : HashMap<String, i64> = HashMap::new();
        let object = json.as_object().expect("Should be an object");
        for (k,v) in object {
            counters.insert(k.clone(), v.as_i64().expect("Should be a number"));
        }
        GCounter { counters : counters }
    }

    fn read(&self) -> Value { 
        let mut sum = 0;
        for (_k, v) in &self.counters {
            sum = sum + v;
        };
        debug(format!("Counters {:?}, Sum {}", self.counters, sum));
        return json!(sum)
    }

    fn merge(&mut self, other : GCounter){
        let mut merged_map : HashMap<String, i64> = HashMap::new();
        for (k,v1) in  &self.counters {
            match other.counters.get(k) {
                Some(v2) => merged_map.insert(k.clone(), std::cmp::max(*v1,*v2)),
                None => merged_map.insert(k.clone(), *v1)
            };
        };
        for (k,v) in  &other.counters {
            if !merged_map.contains_key(k) {
                merged_map.insert(k.clone(), *v);
            }
        };
        debug(format!("This {:?}, Other {:?}, Merged {:?}", self.counters, other.counters, merged_map));
        self.counters = merged_map;
    }
}

fn process_message(node_id : &String, crdt : &mut PNCounter, msg : &Message) -> Value {
    match msg.body["type"].as_str() {
        Some("add") => {
            let add : Add = serde_json::from_value(msg.body.clone()).unwrap();
            let resp = add.response();
            let value = add.delta.as_i64().expect(format!("Expected a number, got {:?}", add.delta).as_str());
            crdt.add(node_id.clone(), value);
            resp
        },                               
        _ => panic!("Unexpected message")
    }
}

fn main() -> Result<()> {
    let crdt = PNCounter::new();
    let server : Server<PNCounter> = Server::new(crdt, process_message);   
    server.run();
    Ok(())
}