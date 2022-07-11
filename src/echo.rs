use std::io::{self};
use serde::{Deserialize};
use serde_json::{Value, json};
mod node;                         
use crate::node::{Server, Node, Message};

#[derive(Deserialize)]
struct Echo {
    echo: String
}

impl Echo {
    fn response(self) -> Value {
        return json!({ "type" : "echo_ok", "echo" : self.echo })
    }
}

struct EchoServer {
    node : Option<Node>
}

impl Server for EchoServer {
    fn get_node_ref(&self) -> &Node {
        self.node.as_ref().unwrap()
    }
    fn start(&mut self, node : Node) {
        self.node = Some(node);
    }
    fn process_message(&mut self, msg : Message) {
        if let Some("echo") = msg.body["type"].as_str() {
            let echo : Echo = serde_json::from_value(msg.body.clone()).unwrap();
            self.get_node_ref().send_reply( echo.response(), &msg );
        }
    }
    fn notify(&mut self) {}
}

fn main() -> io::Result<()> {
    let mut server = EchoServer { node: None };
    server.run();
    Ok(())
}