    
pub mod crdt_server {
    use std::io::{self, Write};
    use serde::{Deserialize};
    use serde_json::{Value, json};
    use std::thread;
    use std::time::Duration;
    use std::sync::mpsc;
    use crate::node::node::{Node, Init, Message};                          

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

    pub struct Server<T : CRDT >{
        server : Option<Node>,
        crdt : T,
        handler: fn(&String, &mut T, &Message) -> Value
    }

    impl<T : CRDT>  Server<T>  {
        pub fn new(crdt : T, handler: fn(&String, &mut T, &Message) -> Value ) -> Server<T> {
            Server {
                server : None,
                crdt : crdt,
                handler : handler
            }
        }

        pub fn run(mut self) {
            let (input_tx, input_rx) = mpsc::channel();
            thread::spawn(move || {
                loop {
                    let mut buffer = String::new();
                    match io::stdin().read_line(&mut buffer) {
                        Ok(_n) => {
                            match serde_json::from_str::<Message>(buffer.as_str()) {
                                Ok(msg) => {
                                    input_tx.send(msg).expect("Failed to send message read from command line");
                                },
                                Err(error) => {
                                    debug(format!("Invalid JSON {} {}", buffer, error));
                                }
                            };
                            ()
                        },
                        Err(_error) => panic!("Failed to read from stdin")
                    }
                }
            });   
            let (replicate_tx, replicate_rx) = mpsc::channel();
            thread::spawn(move || {
                loop {
                    replicate_tx.send(()).expect("Failed to send replication ping");
                    thread::sleep(Duration::from_secs(5));
                }

            });
            loop {
                match input_rx.try_recv() {
                    Ok(msg) => {
                        &self.process_message(msg);
                    },
                    _ => ()
                };
                match replicate_rx.try_recv() {
                    Ok(_x) => {
                        debug("Replicating".to_string());
                        match &self.server {
                            Some(s) => {
                                for neighbour in s.node_ids.iter() {
                                    if neighbour.to_string() != s.node_id {
                                        s.send_to_node_noack(json!({ "type" : "replicate", "value": &self.crdt.to_json() }), &neighbour);
                                    }
                                }
                            },
                            None => ()
                        }
                    },
                    _ => ()
                };
            }   
        }    

        fn process_message(&mut self, msg : Message ) {
            match msg.body.get("in_reply_to") {
                Some(msg_id) => {
                    match &self.server {
                        Some(s) => s.acked(msg_id.as_u64().unwrap()),
                        None => panic!("Missing server")
                    }                
                },
                _ => {
                    match msg.body["type"].as_str() {
                        Some("init") => {
                            let init : Init = serde_json::from_value(msg.body.clone()).unwrap();
                            let s : Node = Node::new(&init);
                            s.send( init.response(), &msg );
                            self.server=Some(s);                              
                        },
                        Some("replicate") => {
                            let repl : Replicate = serde_json::from_value(msg.body.clone()).unwrap();
                            self.crdt.merge(T::from_json(repl.value));
                        },
                        Some("read") => {
                            let read : Read = serde_json::from_value(msg.body.clone()).unwrap();
                            match &self.server {
                                Some(s) => s.send(read.response(self.crdt.read()), &msg ),
                                None => panic!("Missing server")
                            }
                        }
                        _ => {
                            match &self.server {
                                Some(s) => {
                                    let resp = (self.handler)(&s.node_id, &mut self.crdt, &msg);
                                    s.send(resp, &msg);
                                },
                                None => panic!("Missing server")
                            }
                        }
                    }
                }            
            }
        }
    }
}