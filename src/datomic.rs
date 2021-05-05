
use std::io::Result;
mod node;
use crate::node::node::{Node, Server, Message}; 
use serde::{Deserialize};
use serde_json::{Value, json};
use std::collections::{ HashMap };   

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

#[derive(Deserialize)]
struct ReadOk {
    #[serde(rename="type")]
    #[allow(dead_code)]
    type_ : String,
    value: HashMap<u64, Vec<i64>>,
    in_reply_to: u32
}


struct DatomicServer {
    state : HashMap<u64, Vec<i64>>,
    node : Option<Node>
}

impl DatomicServer {
    fn new () -> DatomicServer {
        DatomicServer {
            state : HashMap::new(),
            node : None                
        }
    }

    fn transact(&mut self, txn : &Vec<Value>) -> std::result::Result<Vec<Value>, &str> {
        let node = self.node.as_ref().unwrap();
        let resp = node.send_to_node_sync(json!({ "type": "read", "key": "root"}), "lin-kv".to_string());
        if resp["type"] == "read_ok" {
            let read_ok : ReadOk = serde_json::from_value(resp).expect("Not a read ok response");
            self.state = read_ok.value;
        }
        let (result, updated_state) = self.apply_transaction(txn);
        let cas_resp = node.send_to_node_sync(json!({ 
            "type": "cas", 
            "key": "root", 
            "from": self.state,
            "to" : updated_state,
            "create_if_not_exists" : true
        }), "lin-kv".to_string());
        node::node::debug(format!("\nReceived CAS response {:?}", cas_resp));
        if cas_resp["type"].as_str().unwrap() != "cas_ok" {
            return Err("Failed to CAS");
        }
        node::node::debug(format!("\nTxn complete {:?}", txn));     
        Ok(result)
    }

    fn apply_transaction(&self, txn : &Vec<Value>) -> (Vec<Value>, HashMap<u64, Vec<i64>>) {
        let mut result = Vec::new();
        let mut new_state = self.state.clone();
                        
        for entry in txn.into_iter() {
            if let [ f, k, v ] = entry.as_array().expect("Entry should be an array").as_slice() {
                let action = f.as_str().expect("Action should be a string");
                let key = k.as_u64().expect("Key should be a u64");
                let entry = new_state.get(&key);
                match action {
                    "r" => {
                        match entry {
                            Some(e) => result.push(json!([ action, key, e ])),
                            None => result.push(json!([ action, key, Value::Null ]))
                        }
                    },
                    "append" => {
                        let value = v.as_i64().expect("Append value should be a signed int");
                        result.push(json!([ action, key, v ]));                        
                        let mut updated_entry = match entry {
                            Some(e) => e.clone(),
                            None =>Vec::new()
                        };
                        updated_entry.push(value);
                        new_state.insert(key, updated_entry);
                    },
                    _ => {
                        panic!(format!("unexpected action of ${:?}", action));
                    }
                }
            }
        }
        (result, new_state)
    }
}

impl Server for DatomicServer {
    fn process_reply(&self) {
        self.node.as_ref().unwrap().process_reply();
    }

    fn start(&mut self, node : Node){
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

fn main() -> Result<()> {
    let server = DatomicServer::new();   
    node::node::run(server);
    Ok(())
}