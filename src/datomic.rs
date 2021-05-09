
mod node;
use crate::node::node::{Node, Server, Message}; 
use std::{thread, time, fmt};
use serde::{
    ser::{Serialize, Serializer},
    Deserialize, Deserializer,
    de::{Error, Visitor, Unexpected}
};
use serde_json::{Value, json};
use std::collections::{ HashMap };   

const SVC : &str = "lin-kv";

#[derive(Deserialize)]
struct Txn {
    #[serde(rename="type")]
    #[allow(dead_code)]
    type_ : String,
    txn: Vec<Value>,
    msg_id: u32
}

impl Txn {
    fn response(&self, values: &Vec<Value>) -> Value {
        return json!({ "type" : "txn_ok", "in_reply_to": self.msg_id, "txn" : values })
    }

    fn fail(&self, err: &String) -> Value {
        return json!({ "type" : "error", "in_reply_to": self.msg_id, "code" : 30, "text": err })
    }
}

#[derive(Clone)]
struct Thunk {
    id : String,
    value : Option<Value>,
    saved: bool
}

impl Thunk {
    fn value_as_vec(&mut self, node: &Node) -> Vec<i64>{
        serde_json::from_value(self.resolve_value(node)).expect("Should be a vec of i64s")
    }
        
    fn value_as_map(&mut self, node: &Node) -> HashMap<u64, Thunk> {
        serde_json::from_value(self.resolve_value(node)).expect("Should be a map of thunks")
    }

    fn resolve_value(&mut self, node: &Node) -> Value {
        while self.value.is_none() {
            let resp = node.send_to_node_sync(json!({ "type": "read", "key": self.id}), SVC.to_string());
            if resp["type"] == "read_ok" {
                self.value = Some(resp["value"]);
            } else {
                thread::sleep(time::Duration::from_millis(10));
            }
        }
        self.value.as_ref().unwrap().clone()
    }

    fn save(&mut self, node: &Node) {
        while self.value.is_some() && self.saved == false {
            let resp = node.send_to_node_sync(json!({ "type": "write", "key": self.id, "value": self.value.as_ref().unwrap()}), SVC.to_string());
            if resp["type"] == "write_ok" {
                self.saved = true
            } else {
                thread::sleep(time::Duration::from_millis(10));
            }
        }
    }
}

impl Serialize for Thunk {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.id.as_str())
    }
}

struct ThunkVisitor;
impl<'de> Visitor<'de> for ThunkVisitor {
    type Value = Thunk;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "Thunk should be represented by a string")
    }

    fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(Thunk { id: s.to_owned(), value: None, saved: false })
    }
}

impl<'de> Deserialize<'de> for Thunk {
    fn deserialize<D>(deserializer: D) -> Result<Thunk, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(ThunkVisitor)
    }
}

#[derive(Deserialize)]
struct ReadMap {
    #[serde(rename="type")]
    #[allow(dead_code)]
    type_ : String,
    value: HashMap<u64, Thunk>,
    #[allow(dead_code)]
    in_reply_to: u32
}

#[derive(Deserialize)]
struct ReadThunk {
    #[serde(rename="type")]
    #[allow(dead_code)]
    type_ : String,
    value: Vec<i64>,
    #[allow(dead_code)]
    in_reply_to: u32
}


struct DatomicServer {
    state : HashMap<u64, Thunk>,
    node : Option<Node>,
    generator : IdGenerator
}

#[derive(Clone)]
struct IdGenerator {
    seed : i64,
    node_id : String
}

impl IdGenerator {
    fn gen_id(&self) -> (String, IdGenerator) {
        let id = format!("#{}-{}", self.node_id, self.seed);
        (id, IdGenerator{ seed: self.seed + 1, node_id : self.node_id.clone() })
    }    
}

impl DatomicServer {
    fn new () -> DatomicServer {
        DatomicServer {
            state : HashMap::new(),
            node : None,
            generator : IdGenerator{ seed:0, node_id : "".to_string() }                
        }
    }

    fn transact(&mut self, txn : &Vec<Value>) -> std::result::Result<Vec<Value>, &str> {
        let node = self.node.as_ref().unwrap();
        let resp = node.send_to_node_sync(json!({ "type": "read", "key": "root"}), SVC.to_string());
        if resp["type"] == "read_ok" {
            node::node::debug(format!("\nReceived read map response {:?}", resp));
            let read_map : ReadMap = serde_json::from_value(resp).expect("Not a suitable Read Map response");
            self.state = read_map.value;
        }    
        let (result, mut updated_state, generator) = self.apply_transaction(txn, self.generator.clone());
        self.generator = generator;
        // save contents of updated state
        for thunk in updated_state.values_mut() {
            thunk.save(node); 
        };
        let cas_resp = node.send_to_node_sync(json!({ 
            "type": "cas", 
            "key": "root", 
            "from": self.state,
            "to" : updated_state,
            "create_if_not_exists" : true
        }), SVC.to_string());
        node::node::debug(format!("\nReceived CAS response {:?}", cas_resp));
        if cas_resp["type"].as_str().unwrap() != "cas_ok" {
            return Err("Failed to CAS");
        }
        node::node::debug(format!("\nTxn complete {:?}", txn));     
        Ok(result)
    }

    fn apply_transaction(&self, txn : &Vec<Value>, id_gen : IdGenerator) -> (Vec<Value>, HashMap<u64, Thunk>, IdGenerator) {
        let mut result = Vec::new();
        let mut new_state = self.state.clone();
        let node = self.node.as_ref().unwrap();   
        let mut generator = id_gen.clone();                            
        for entry in txn.into_iter() {
            if let [ f, k, v ] = entry.as_array().expect("Entry should be an array").as_slice() {
                let action = f.as_str().expect("Action should be a string");
                let key = k.as_u64().expect("Key should be a u64");
                let entry = new_state.get_mut(&key);
                match action {
                    "r" => {
                        match entry {
                            Some(thunk) => result.push(json!([ action, key, *thunk.value_as_vec(node) ])),
                            None => result.push(json!([ action, key, Value::Null ]))
                        }
                    },
                    "append" => {
                        let value = v.as_i64().expect("Append value should be a signed int");
                        result.push(json!([ action, key, v ]));                        
                        let mut updated_entry : Vec<i64> = match entry {
                            Some(thunk) => thunk.value_as_vec(node).clone(),
                            None => Vec::new()
                        };
                        updated_entry.push(value);
                        let (id, new_generator) = generator.gen_id();
                        new_state.insert(key, Thunk { id: id, value: Some(json!(updated_entry) ), saved: false });
                        generator = new_generator;
                    },
                    _ => {
                        panic!(format!("unexpected action of ${:?}", action));
                    }
                }
            }
        }
        (result, new_state, generator)
    }
}

impl Server for DatomicServer {
    fn process_reply(&self) {
        self.node.as_ref().unwrap().process_reply();
    }

    fn start(&mut self, node : Node){
        self.generator = IdGenerator{ seed:0, node_id : node.node_id.clone() };
        self.node = Some(node);
    }

    fn process_message(&mut self, msg : Message ) {
       let response = match msg.body["type"].as_str() {
            Some("txn") => {
                let txn : Txn = serde_json::from_value(msg.body.clone()).unwrap();
                let transaction_output = self.transact(&txn.txn);
                match transaction_output {
                    Ok(values) => txn.response(&values),
                    Err(text) => txn.fail(&text.to_string())
                }
            },
            _ => {
                panic!(format!("Unexpected message {:?}", msg));
            }
        };                
        self.node.as_ref().unwrap().send(response, &msg)
    }
    fn notify(&mut self){}
}

fn main() -> std::io::Result<()> {
    let server = DatomicServer::new();   
    node::node::run(server);
    Ok(())
}