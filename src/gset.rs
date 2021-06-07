use std::io::Result;
use serde::{Deserialize};
use serde_json::{Value, Number, json};
use std::collections::HashSet;

mod node; 
use crate::node::node::{ Server, Message };
mod crdt_server;                       
use crate::crdt_server::crdt_server::{CRDT, CRDTServer};

#[derive(Deserialize)]
struct Add {
    element: Number,
}

impl Add {
    fn response(&self) -> Value {
        return json!({ "type" : "add_ok" })
    }
}

struct GSet {
    set : std::collections::HashSet<u64>
}

impl GSet {
    fn add(&mut self, value : u64){
        self.set.insert(value);
    }
}

impl CRDT for GSet {

    fn to_json(&self) -> Value {
        Value::Array(self.set.clone().into_iter().map( |i| json!(i) ).collect())
    }

    fn from_json(json : Value) -> GSet {
        let mut set : HashSet<u64> = HashSet::new();
        for entry in json.as_array().unwrap().iter() {
            set.insert(entry.as_u64().unwrap());
        }
        GSet { set : set }
    }

    fn read(&self) -> Value { 
        self.to_json()
    }

    fn merge(&mut self, other : GSet){
        let mut merged_set : HashSet<u64> = HashSet::new();
        for &elem in self.set.iter() {
            merged_set.insert(elem.clone());
        }
        for elem in other.set.into_iter() {
            merged_set.insert(elem.clone());
        }
        self.set = merged_set;
    }
}

fn process_message(_node_id : &String, crdt : &mut GSet, msg : &Message) -> Value {
    match msg.body["type"].as_str() {    
        Some("add") => {
            let add : Add = serde_json::from_value(msg.body.clone()).unwrap();
            let value = add.element.as_u64().expect(format!("Expected a number, got {:?}", add.element).as_str());
            crdt.add(value);
            add.response()
        },
        _ => panic!("Unexpected message")
    }
}

fn main() -> Result<()> {
    let crdt = GSet{ set : HashSet::new() };
    let mut server = CRDTServer::new(crdt, process_message);   
    server.run();
    Ok(())
}