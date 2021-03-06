
mod node;
use crate::node::{debug, Node, Server, Message}; 
use std::{thread, time, fmt};
use serde::{
    ser::{Serialize, Serializer},
    Deserialize, Deserializer,
    de::{Error, Visitor, DeserializeOwned}
};
use serde_json::{Value, json};
use std::collections::{ HashMap };   
use std::marker::PhantomData;

const SVC : &str = "lww-kv";
const ROOT : &str = "root";

#[derive(Deserialize)]
struct Txn {
    txn: Vec<Value>,
}

impl Txn {
    fn response(&self, values: &[Value]) -> Value {
        return json!({ "type" : "txn_ok", "txn" : values })
    }

    fn fail(&self, err: &str) -> Value {
        return json!({ "type" : "error",  "code" : 30, "text": err })
    }
}

#[derive(Clone, Debug)]
struct Thunk<T> {
    id : String,
    value : Option<T>,
    saved: bool
}
type VecThunk = Thunk<Vec<i64>>;
type MapThunk = Thunk<HashMap<u64, VecThunk>>;

impl<T : Serialize + DeserializeOwned + Clone> Thunk<T> {
    fn value(&mut self, node: &Node) -> T {
        while self.value.is_none() {
            let resp = node.send_to_node_sync(json!({ "type": "read", "key": self.id}), SVC.to_string());
            if resp["type"] == "read_ok" {
                debug(format!("Read {:?} - received {}", self.id, resp));
                let value : T = serde_json::from_value(resp["value"].clone()).expect("Failed to unpack JSON for thunk");
                self.value = Some(value);
            } else {
                debug(format!("Failed to read {:?} - received {}", self.id, resp));
                thread::sleep(time::Duration::from_millis(10));
            }
        }
        self.value.as_ref().unwrap().clone()
    }

    fn save(&mut self, node: &Node) {
        while self.value.is_some() && !self.saved {
            let resp = node.send_to_node_sync(json!({ "type": "write", "key": self.id, "value": self.value.as_ref().unwrap()}), SVC.to_string());
            if resp["type"] == "write_ok" {
                debug(format!("Saved {:?}", self.id));
                self.saved = true
            } else {
                thread::sleep(time::Duration::from_millis(10));
            }
        }
    }
}

impl<T> Serialize for Thunk<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.id.as_str())
    }
}

struct ThunkVisitor<T> {
    phantom: std::marker::PhantomData<T>
}

impl<'de, T> Visitor<'de> for ThunkVisitor<T> {
    type Value = Thunk<T>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "Thunk should be represented by a string")
    }

    fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(Thunk::<T>{ id: s.to_owned(), value: None, saved: false })
    }
}

impl<'de, T> Deserialize<'de> for Thunk<T> {
    fn deserialize<D>(deserializer: D) -> Result<Thunk<T>, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(ThunkVisitor::<T>{ phantom : PhantomData })
    }
}

struct DatomicServer {
    state : ThunkCache<HashMap<u64, VecThunk>>,
    entries : ThunkCache<Vec<i64>>,
    node : Option<Node>,
    generator : IdGenerator,
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

struct ThunkCache<T> {
    cache : HashMap<String, Thunk<T>>
}

impl<T: Clone> ThunkCache<T> {
    fn clear_cache_entry(&mut self, id: String) {
        self.cache.remove(&id);
    }
    
    fn read_value_no_cache(id : String, node : &Node) -> Thunk<T>  {
        loop {
            let resp = node.send_to_node_sync(json!({ "type": "read", "key": id}), SVC.to_string());
            if resp["type"] == "read_ok" {
                let v : Thunk<T> = serde_json::from_value(resp["value"].clone()).unwrap_or_else (|_| panic!("Not a suitable Thunk response {} for key {}", resp["value"], id));
                return v
            } else {
                thread::sleep(std::time::Duration::from_millis(50));
            }
        }
    }

    fn read_value(&mut self, id : String, node: &Node) -> Thunk<T> {
        let key = id.clone();
        let entry = self.cache.entry(id).or_insert_with(|| {
            ThunkCache::read_value_no_cache(key, node)
        });
        entry.clone()
    }

    fn contains(&mut self, id : String) -> bool {
        self.cache.contains_key(&id)
    }

    fn insert_value(&mut self, t : Thunk<T>) {
        self.cache.insert(t.id.clone(), t);
    }
}

impl DatomicServer {
    fn new () -> DatomicServer {
        DatomicServer {
            state : ThunkCache::<HashMap<u64, VecThunk>>{ cache: HashMap::new() },
            entries : ThunkCache::<Vec<i64>>{ cache: HashMap::new() },
            node : None,
            generator : IdGenerator{ seed:0, node_id : "".to_string() }                
        }
    }

    fn transact(&mut self, txn : &[Value]) -> std::result::Result<Vec<Value>, &str> {
        debug(format!("Txn start {:?}", txn));     
        let node = self.node.as_ref().unwrap();
        let mut current_state_thunk : MapThunk = ThunkCache::read_value_no_cache(ROOT.to_string(), node);
        let mut current_state = current_state_thunk.value(node);
        // replace any cached vec thunks
        for (_, val) in current_state.iter_mut() {
            if self.entries.contains(val.id.clone()) {
                *val = self.entries.read_value(val.id.clone(), node);
            }
        }
        let (result, mut updated_state, generator) = self.apply_transaction(txn, current_state, self.generator.clone());
        self.generator = generator;
        // save contents of updated state entries and cache them
        for thunk in updated_state.value(node).values_mut() {
            thunk.save(node); 
            self.entries.insert_value(thunk.clone());
        };
        // save the map
        updated_state.save(node);
        // update id of map pointed to by ROOT
        let cas_resp = node.send_to_node_sync(json!({ 
            "type": "cas", 
            "key": ROOT, 
            "from": current_state_thunk.id,
            "to" : updated_state.id,
            "create_if_not_exists" : true
        }), SVC.to_string());
        if cas_resp["type"].as_str().unwrap() != "cas_ok" {
            debug(format!("Failed to CAS response {:?}", cas_resp));
            self.state.clear_cache_entry(ROOT.to_string());
            // std::thread::sleep_ms(10);
            // return self.transact(txn);
            return Err("Failed to CAS");
        }
        debug(format!("Txn complete {:?}", txn));     
        Ok(result)
    }

    fn apply_transaction(&self, txn : &[Value], current_state : HashMap<u64, VecThunk>, id_gen : IdGenerator) -> (Vec<Value>, MapThunk, IdGenerator) {
        let mut result = Vec::new();
        let node = self.node.as_ref().unwrap();  
        let mut new_state = current_state;
        let mut generator = id_gen;                            
        for entry in txn.iter() {
            if let [ f, k, v ] = entry.as_array().expect("Entry should be an array").as_slice() {
                let action = f.as_str().expect("Action should be a string");
                let key = k.as_u64().expect("Key should be a u64");
                let entry = new_state.get_mut(&key);
                match action {
                    "r" => {
                        match entry {
                            Some(thunk) => {
                                debug(format!("Reading thunk {} for key {}", thunk.id, key));     
                                result.push(json!([ action, key, *thunk.value(node) ]))
                            },
                            None => result.push(json!([ action, key, Value::Null ]))
                        }
                    },
                    "append" => {
                        let value = v.as_i64().expect("Append value should be a signed int");
                        result.push(json!([ action, key, v ]));                        
                        let mut updated_entry : Vec<i64> = match entry {
                            Some(thunk) => thunk.value(node).clone(),
                            None => Vec::new()
                        };
                        updated_entry.push(value);
                        let (id, new_generator) = generator.gen_id();
                        new_state.insert(key, VecThunk { id, value: Some(updated_entry), saved: false });
                        generator = new_generator;
                    },
                    _ => {
                        panic!("unexpected action of ${:?}", action);
                    }
                }
            }
        }
        let (id, new_generator) = generator.gen_id();                        
        (result, MapThunk { id, value: Some(new_state), saved: false } , new_generator)
    }
}

impl Server for DatomicServer {
    fn get_node_ref(&self) -> &Node {
        self.node.as_ref().unwrap()
    }    

    fn start(&mut self, node : Node){
        let (id, generator) = IdGenerator{ seed:0, node_id : node.node_id.clone() }.gen_id();
        if node.node_ids[0] == node.node_id {
            let mut root_thunk = MapThunk { id: id.clone(), value: Some(HashMap::new()), saved: false };
            root_thunk.save(&node);

            loop {
                let resp = node.send_to_node_sync(json!({ "type": "write", "key": ROOT, "value": &id }), SVC.to_string());
                if resp["type"] == "write_ok" {
                    debug(format!("Saved initial root node {:?}", id));     
                    break;
                } else {
                    thread::sleep(time::Duration::from_millis(10));
                }
            }
        }
        self.generator = generator;
        self.node = Some(node);        
    }

    fn process_message(&mut self, msg : Message ) {
       let response = match msg.body["type"].as_str() {
            Some("txn") => {
                let txn : Txn = serde_json::from_value(msg.body.clone()).unwrap();
                let transaction_output = self.transact(&txn.txn);
                match transaction_output {
                    Ok(values) => txn.response(&values),
                    Err(text) => txn.fail(text)
                }
            },
            _ => {
                panic!("Unexpected message {:?}", msg);
            }
        };                
        self.get_node_ref().send_reply(response, &msg)
    }
    fn notify(&mut self){}
}

fn main() -> std::io::Result<()> {
    let mut server = DatomicServer::new();   
    server.run();
    Ok(())
}