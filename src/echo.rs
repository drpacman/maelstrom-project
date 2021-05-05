use std::io::{self, Write};
use serde::{Deserialize};
use serde_json::{Value, json};
mod node;                         
use crate::node::node::{Server, Node, Init, Message};

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

struct EchoServer {
    node : Option<Node>
}

impl Server for EchoServer {
    fn process_reply(&self) {}
    fn start(&mut self, node : Node) {
        self.node = Some(node);
    }
    fn process_message(&mut self, msg : Message) {
        match msg.body["type"].as_str() {
            Some("echo") => {
                let echo : Echo = serde_json::from_value(msg.body.clone()).unwrap();
                self.node.as_ref().unwrap().send( echo.response(), &msg );
            },
            _ => {}
        }
    }
    fn notify(&mut self) {}
}

fn main() -> io::Result<()> {
    let server = EchoServer { node: None };
    node::node::run(server);
    Ok(())
}