    
pub mod crdt_server {
    use serde::{Deserialize};
    use serde_json::{Value, json};
    use std::time::{ SystemTime };
    use crate::node::node::{debug, Node, Server, Message};                          
    
    #[derive(Deserialize)]
    struct Replicate {
        value: Value,
    }

    pub trait CRDT {
        fn to_json(&self) -> Value;
        fn from_json(json : Value) -> Self;
        fn read(&self) -> Value;    
        fn merge(&mut self, other : Self);
    }

    #[derive(Deserialize)]
    struct Read {}

    impl Read {
        fn response(self, contents : Value) -> Value {
            return json!({"type" : "read_ok", "value": contents})
        }
    }

    pub struct CRDTServer<T : CRDT >{
        node : Option<Node>,
        crdt : T,
        handler: fn(&String, &mut T, &Message) -> Value,
        last_replication: SystemTime
    }

    impl<T : CRDT>  CRDTServer<T>  {
        pub fn new(crdt : T, handler: fn(&String, &mut T, &Message) -> Value ) -> CRDTServer<T> {
            CRDTServer {
                node : None,
                crdt : crdt,
                handler : handler,
                last_replication: SystemTime::now()
            }
        }

        fn replicate(&self) {
            debug("Replicating".to_string());
            let node_ref = self.node.as_ref().unwrap();
            for neighbour in node_ref.node_ids.iter() {
                if neighbour.to_string() !=  node_ref.node_id {
                    node_ref.send_to_node_noack(
                        json!({ "type" : "replicate", "value": &self.crdt.to_json() }
                    ), neighbour.to_string());
                }
            }
        }        
    }

    impl<T: CRDT + Send + 'static>  Server for CRDTServer<T>  {
    
        fn get_node_ref(&self) -> &Node {
            self.node.as_ref().unwrap()
        }
                
        fn start(&mut self, node : Node) {
            self.node = Some(node);
        }

        fn notify(&mut self) {
            if let Ok(elapsed) = self.last_replication.elapsed() {
                if elapsed.as_secs() > 5 {
                    self.last_replication = SystemTime::now();
                    self.replicate();
                }
            };
        }    

        fn process_message(&mut self, msg : Message ) {
            let n = self.node.as_ref().unwrap();
            match msg.body["type"].as_str() {
                Some("replicate") => {
                    let repl : Replicate = serde_json::from_value(msg.body.clone()).unwrap();
                    self.crdt.merge(T::from_json(repl.value));
                },
                Some("read") => {
                    let read : Read = serde_json::from_value(msg.body.clone()).unwrap();
                    n.send_reply(read.response(self.crdt.read()), &msg );
                }
                _ => {
                    let resp = (self.handler)(&n.node_id, &mut self.crdt, &msg);
                    n.send_reply(resp, &msg);
                }
            }
        }
    }
}