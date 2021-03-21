use std::io::{self, Write};
use serde::{Deserialize};
use serde_json::{Value, json};
mod node;                         
use crate::node::node::{Node, Init, Message};

#[derive(Deserialize)]
struct Echo {
    #[serde(rename="type")]
    #[allow(dead_code)]
    type_ : String,
    msg_id: u32,
    echo: String
}

impl Echo {
    fn response(self) -> Value {
        return json!({ "type" : "echo_ok", "in_reply_to": self.msg_id, "echo" : self.echo })
    }
}

fn debug(msg : String) {
    io::stderr().write(msg.as_bytes()).expect("Failed to write debug");
}

fn main() -> io::Result<()> {
    let mut server : Option<Box<Node>> = None;
    loop {
        let mut buffer = String::new();
        match io::stdin().read_line(&mut buffer) {
            Ok(_n) => {
                match serde_json::from_str::<Message>(buffer.as_str()) {
                    Ok(msg) => {
                        match msg.body["type"].as_str() {
                            Some("init") => {
                                let init : Init = serde_json::from_value(msg.body.clone()).unwrap();
                                let s : Node = Node::new(&init);
                                s.send( init.response(), &msg );
                                server = Some(Box::new(s))                                
                            },
                            Some("echo") => {
                                let echo : Echo = serde_json::from_value(msg.body.clone()).unwrap();
                                match &server {
                                    Some(server) => server.send( echo.response(), &msg ),
                                    None => panic!("Missing server")
                                }
                            }
                            _ => {}
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