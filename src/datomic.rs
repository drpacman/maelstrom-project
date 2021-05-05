
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
    value: Vec<i64>,
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

    fn transact(&self, txn : &Vec<Value>) -> std::result::Result<Vec<Value>, &str> {
        let mut result = Vec::new();
        for entry in txn.into_iter() {
            if let [ f, k, v ] = entry.as_array().expect("Entry should be an array").as_slice() {
                let action = f.as_str().expect("Action should be a string");
                let key = k.as_u64().expect("Key should be a u64");
                node::node::debug(format!("\nAction {:?}\n", action));
                match action {
                    "r" => {
                        let n = self.node.as_ref().unwrap();
                        let resp = n.send_to_node_sync(json!({ "type": "read", "key": key}), "lin-kv".to_string());
                        if resp["type"] == "read_ok" {
                            let read_ok : ReadOk = serde_json::from_value(resp).expect("Not a read ok response");
                            result.push(json!([ action, key, read_ok.value ]));
                        } else {
                            result.push(json!([ action, key, Value::Null ]));
                        };
                        
                    },
                    "append" => {
                        let value = v.as_i64().expect("Append value should be a signed int");
                        result.push(json!([ action, key, v ]));                        
                        let n = self.node.as_ref().unwrap();
                        let resp = n.send_to_node_sync(json!({ "type": "read", "key": key}), "lin-kv".to_string());
                        node::node::debug(format!("\nReceived Sync Response: {:?}", resp));
                        let cas_resp = if resp["type"] == "read_ok" {
                            let read_ok : ReadOk = serde_json::from_value(resp).expect("Not a read ok response");
                            let existing = read_ok.value;
                            let mut updated = existing.clone();                        
                            updated.push(value);
                            n.send_to_node_sync(json!({ 
                                "type": "cas", 
                                "key": key, 
                                "from": existing,
                                "to" : updated,
                                "create_if_not_exists" : true
                            }), "lin-kv".to_string())
                        } else {
                            n.send_to_node_sync(json!({ 
                                "type": "cas", 
                                "key": key, 
                                "to" : vec![value],
                                "create_if_not_exists" : true
                            }), "lin-kv".to_string())
                        };
                        node::node::debug(format!("\nReceived CAS response {:?}", cas_resp));
                        if cas_resp["type"].as_str().unwrap() != "cas_ok" {
                            return Err("Failed to CAS");
                        }
                    },
                    _ => {
                        panic!(format!("unexpected action of ${:?}", action));
                    }
                }
            }
        };
        node::node::debug(format!("\nTxn complete {:?}", txn));     
        Ok(result)
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
       match msg.body["type"].as_str() {
            Some("txn") => {
                let txn : Txn = serde_json::from_value(msg.body.clone()).unwrap();
                let transaction_output = self.transact(&txn.txn);
                let n = self.node.as_ref().unwrap();
                match transaction_output {
                    Ok(values) => n.send(txn.response(&values), &msg),
                    Err(text) => n.send(txn.fail(&text.to_string()), &msg)
                }
            },
            _ => {
                panic!(format!("Unexpected message {:?}", msg));
            }
        }
    }
    fn notify(&mut self){}
}

fn main() -> Result<()> {
    let server = DatomicServer::new();   
    node::node::run(server);
    Ok(())
}