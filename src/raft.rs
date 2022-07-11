use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};
mod node;
use node::{Server, Node, Message, debug};
use json_patch::merge;
use std::time::{ SystemTime, Instant };
use std::error::Error;
use std::{ fmt };
use rand::Rng;

const ELECTION_PERIOD_IN_MILLISECONDS : u128 = 2000;
const STEP_DOWN_PERIOD_IN_MILLISECONDS : u128 = 2000;
const MIN_REPLICATION_INTERVAL : u128 = 50;
const HEARTBEAT_INTERVAL : u128 = 1000;

#[derive(Deserialize)]
struct RequestVote {
    term: u64,
    last_log_term: u64,
    last_log_index: u64
}

#[derive(Deserialize)]
struct RequestVoteResult {
    term: u64,
    vote_granted: bool,
    commit_index: u64
}

#[derive(Deserialize)]
struct AppendEntries {
    #[serde(rename="type")]
    #[allow(dead_code)]
    type_ : String,
    term: u64,
    leader_id: String,
    prev_log_index : u64,
    prev_log_term : u64,
    entries : Vec<RaftLogEntry>,
    leader_commit: u64,
    #[allow(dead_code)]
    msg_id: u32
}

impl AppendEntries {
    fn success_response(&self, next_log_index : usize) -> Value {
        self.response(true, next_log_index)
    }

    fn failure_response(&self) -> Value {
        self.response(false, 0)
    }

    fn response(&self, success: bool, next_log_index : usize) -> Value {
        json!( {
            "type" : "append_entries_res",
            "term" : self.term,
            "success" : success,
            "next_log_index" : next_log_index
        })
    }
}

#[derive(Deserialize)]
struct AppendEntriesResponse {
    term: u64,
    next_log_index : u64,
    success: bool
}

#[derive(Clone, Debug)]
struct StateMachine {
    map : HashMap<i64, i64>
}

impl StateMachine {
    fn process(&self, operation : Value) -> (StateMachine, Value) {
        // debug(format!("Processing operation {}", operation));
        let key = operation["key"].as_i64().unwrap();
        match operation["type"].as_str().unwrap() {
            "read" => {
                if self.map.contains_key(&key) {
                    ( self.clone(), json!( { "type": "read_ok", "value": &self.map.get(&key).unwrap() }) )
                } else {
                    ( self.clone(), json!( { "type" : "error", "reason" : format!("Missing key {}", key) }))
                }
            } ,
            "write" => {
                let mut updated_map = self.clone().map;
                let value = operation["value"].as_i64().unwrap();
                updated_map.insert(key, value);
                ( StateMachine { map : updated_map }, json!( { "type" : "write_ok" }))
            } ,
            "cas" => {
                let from = operation["from"].as_i64().unwrap();
                match self.map.get(&key) {
                    Some(existing_value) if *existing_value == from => {
                        let mut updated_map = self.clone().map;
                        let to = operation["to"].as_i64().unwrap();
                        updated_map.insert(key, to);
                        ( StateMachine { map : updated_map }, json!( { "type" : "cas_ok" }))
                    },
                    Some(current_value) => ( self.clone(), json!( { 
                        "type" : "error", 
                        "reason" : format!("CAS error - invalid from value of {} for key {}, current value is {}", from, key, current_value) 
                    })),
                    None => ( self.clone(), json!( { "type" : "error",  "reason" : format!("CAS error - invalid from value of {} for key {}, no current value in state machine for that key", from, key) }))
                }
            } ,
            op => {
                panic!("Unexpected operation {}", op);
            }           
        }
    }
}

#[derive(PartialEq, Debug)]
enum RaftState {
    Candidate,
    Leader,
    Follower
}

#[derive(Debug)]
struct RaftServer {
    state : RaftState,
    state_machine : StateMachine,
    node : Option<Node>,
    election_clock : SystemTime,
    step_down_clock : SystemTime,
    replication_clock : SystemTime,
    term : u64,
    log: RaftLog,
    votes: HashSet<(String, usize)>,
    voted_for : Option<String>,
    next_node_index: HashMap<String, usize>,
    match_index : HashMap<String, usize>,
    last_applied_index: usize,
    commit_index : usize,
    millis_till_next_election : u128,
    leader_id : Option<String>,
    proxied : HashMap<u64, Message>
}

#[derive(Debug)]
struct RaftError {
    reason : String
}

impl RaftError {
    fn new(reason: String) -> RaftError {
        RaftError { reason }
    }
}
impl Error for RaftError {}
impl fmt::Display for RaftError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f,"{}",self.reason)
    }
}


#[derive(Debug)]
struct RaftLog {
    entries : Vec::<RaftLogEntry>,
    timestamps : HashMap::<usize, Instant>
}

#[derive(Serialize, Deserialize, Debug, Clone)]    
struct RaftLogEntry {
    term : u64,
    operation : Value,
    client_id : String
}

impl RaftLog {
    fn new() -> RaftLog {
        let mut log = RaftLog {
            entries : Vec::new(),
            timestamps : HashMap::new()
        };
        log.append( RaftLogEntry{ term: 0, operation : Value::Null, client_id : "".to_string() } );
        log
    }

    fn append(&mut self, entry : RaftLogEntry) {
        self.entries.push( entry );
        self.timestamps.insert( self.entries.len(), Instant::now() );
    }

    fn last(&self) -> RaftLogEntry {
        self.entries.last().unwrap().clone()
    }

    fn log_entry(&self, index : usize) -> Option<RaftLogEntry> {
        self.entries.get(index).cloned()
    }

    fn truncate(&mut self, index : usize) {
        self.entries.truncate(index);
    }

    fn size(&self) -> usize {
        self.entries.len()
    }

    fn get_entries_from(&self, index : usize) -> Vec<RaftLogEntry> {
        self.entries[ index.. ].to_vec()
    }

    fn get_entries_to(&self, from: usize, to : usize) -> Vec<RaftLogEntry> {
        self.entries[ (from+1)..to ].to_vec()
    }

    fn get_latency(&self, index : usize) -> u128 {
        self.timestamps.get(&index).unwrap().elapsed().as_millis()
    }
}

impl RaftServer {
    fn new() -> RaftServer {
        RaftServer { 
            state_machine : StateMachine { map : HashMap::new() },
            node : None,
            state: RaftState::Follower,
            election_clock : SystemTime::now(),
            step_down_clock: SystemTime::now(),
            replication_clock: SystemTime::now(),
            term : 0,
            log : RaftLog::new(),
            votes : HashSet::new(),
            voted_for: None,
            next_node_index : HashMap::new(),
            match_index: HashMap::new(),
            last_applied_index: 0,
            commit_index : 0,
            millis_till_next_election : RaftServer::jitter(),
            leader_id : None,
            proxied: HashMap::new(),            
        }
    }

    fn majority(size : usize) -> usize {
        size/2 + 1
    }

    fn median_match_index(&self) -> usize {
        let mut indexes : Vec<usize> = self.match_index.clone().into_iter().map(|(_k, v)| v).collect();
        indexes.push(self.log.size());
        indexes.sort_unstable();
        let m = RaftServer::majority(indexes.len());
        indexes[m]
    }

    fn advance_state_machine(&mut self){
        debug("Advancing state machine".to_string());
        let entries = self.log.get_entries_to(self.last_applied_index, self.commit_index);
        for entry in entries {
            // debug(format!("Applying req {}", entry.operation));
            let (next, resp) = self.state_machine.process(entry.operation.clone());            
            self.state_machine = next;
            self.last_applied_index += 1;
            if self.state == RaftState::Leader {
                let msg_id = entry.operation["msg_id"].as_i64().unwrap();        
                let mut reply = json!({ "in_reply_to": msg_id });
                merge(&mut reply, &resp);
                debug(format!("Replying to {} with {}", entry.client_id, reply));
                let node = self.node.as_ref().unwrap();
                node.send_to_node_noack(reply, entry.client_id);
            }
        }
    }

    fn become_candidate(&mut self) {
        self.state = RaftState::Candidate;
        self.leader_id = None;
        self.reset_step_down_clock();
        match self.advance_term(self.term + 1) {
            Ok(()) => {
                debug(format!("{} became CANDIDATE for term {}", self.node.as_ref().unwrap().node_id, self.term));
                self.voted_for = Some(self.node.as_ref().unwrap().node_id.clone());        
                self.request_votes(); 
            }, 
            Err(_e) => {
                self.become_follower();
            }       
        }
    }

    fn become_follower(&mut self) {
        self.state = RaftState::Follower;
        self.leader_id = None;
        debug(format!("{} became FOLLOWER for term {}", self.node.as_ref().unwrap().node_id, self.term));
    }

    fn become_leader(&mut self) {
        self.state = RaftState::Leader;
        let node = self.node.as_ref().unwrap();
        self.leader_id = Some(node.node_id.clone());
        self.next_node_index.clear();
        self.match_index.clear();
        let match_index = self.votes.iter().fold(std::usize::MAX, |min_index, (_src, index)| std::cmp::min(min_index, *index));
        for node_id in node.other_node_ids() {
            self.next_node_index.insert(node_id.clone(), self.log.size());
            self.match_index.insert(node_id.clone(), match_index);
        }   
        self.reset_step_down_clock();
        debug(format!("{} became LEADER for term {}", self.node.as_ref().unwrap().node_id, self.term));
    }

    fn advance_term(&mut self, term : u64) -> Result<(), RaftError> {
        if self.term > term {
            return Err( RaftError::new(format!("Term {} is in before {}", term, self.term)) );
        }
        self.term = term;
        self.voted_for = None;
        Ok(())
    }

    fn maybe_step_down(&mut self, term : u64) {
        if self.term < term {
            debug(format!("Not latest term - Node {} stepping down", self.node.as_ref().unwrap().node_id));
            self.advance_term(term).expect("Failed to advance term");
            self.become_follower();
        }
    }

    fn request_votes(&mut self) {
        self.votes = HashSet::new();
        let node = self.node.as_ref().unwrap();
        // vote for yourself
        self.votes.insert( (node.node_id.clone(), self.commit_index) );
        node.broadcast_acked(json!({  
            "type": "request_vote",  
            "term": self.term,  
            "candidate_id": node.node_id,
            "last_log_index" : self.log.size(),
            "last_log_term" : self.log.last().term
        }));
        self.reset_step_down_clock();        
    }

    fn jitter() -> u128 {
        rand::thread_rng().gen_range(0..500)
    }

    fn choose_election_duration(&mut self) {
        self.millis_till_next_election = ELECTION_PERIOD_IN_MILLISECONDS + RaftServer::jitter();
    }

    fn reset_step_down_clock(&mut self) {
        self.step_down_clock = SystemTime::now();
    }

    fn reset_election_deadline(&mut self) {
        self.choose_election_duration();
        self.election_clock = SystemTime::now();
    }

    fn replicate_logs(&mut self) {
        let node = self.get_node_ref();
        let elapsed = self.replication_clock.elapsed().unwrap();
        for node_id in node.other_node_ids() {
            let node_index = self.next_node_index.get(&node_id).unwrap();
                
            let prev_entry = self.log.log_entry(node_index - 1).unwrap();
            let entries = self.log.get_entries_from(*node_index);
            if !entries.is_empty() || elapsed.as_millis() > HEARTBEAT_INTERVAL {
                // debug(format!("Node index {} for node {}", node_index, node_id));
                debug(format!("Replicating {} entries to {}", entries.len(), &node_id));
                let append_entries_req = json!( {
                    "type" : "append_entries",
                    "term" : self.term,
                    "leader_id" : node.node_id,
                    "prev_log_index": node_index - 1,
                    "prev_log_term" : prev_entry.term,
                    "entries" : entries,
                    "leader_commit" : self.commit_index
                });
                node.send_to_node_acked(&append_entries_req, node_id);
            }
        }
        self.replication_clock = SystemTime::now();
    }
}

impl Server for RaftServer {
    fn get_node_ref(&self) -> &Node {
        self.node.as_ref().unwrap()
    }
    
    fn start(&mut self, node : Node) {
        self.node = Some(node);
    }

    fn process_reply(&mut self, msg : Message) {
        debug("Processing reply".to_string());
        let body = msg.body.clone();
        match body["type"].as_str() {
            Some("request_vote_res")  => {
                let res : RequestVoteResult = serde_json::from_value(body.clone()).unwrap();
                if self.state == RaftState::Candidate && res.vote_granted && res.term == self.term {
                    self.votes.insert( (msg.src, res.commit_index as usize ) );
                    debug(format!("Have received {:?} votes", self.votes));
                    let node = self.node.as_ref().unwrap();
                    if RaftServer::majority(node.node_ids.len()) <= self.votes.len() {
                        self.become_leader();
                    }
                }
            },
            Some("append_entries_res")  => {
                if self.state == RaftState::Leader {
                    let current_term = self.term;
                    let append_entries_resp : AppendEntriesResponse = serde_json::from_value(body.clone()).unwrap();
                    self.maybe_step_down(append_entries_resp.term);
                    if current_term == append_entries_resp.term {
                        self.reset_step_down_clock();
                        if append_entries_resp.success {
                            debug(format!("Updating next log index for {} to {}", msg.src, append_entries_resp.next_log_index));
                            self.next_node_index.insert( msg.src.clone(), append_entries_resp.next_log_index as usize );
                            self.match_index.insert(msg.src.clone(), append_entries_resp.next_log_index as usize);
                            // find majority match index
                            let median_match_index = self.median_match_index();
                            if self.commit_index < median_match_index {
                                // update commit index
                                self.commit_index = median_match_index;
                                debug(format!("Updating leader commit index to {} - commit latency is {}", self.commit_index, self.log.get_latency(self.commit_index)));
                                self.advance_state_machine()
                            }
                        } else {
                            *self.next_node_index.get_mut(&msg.src).unwrap() -= 1;
                        }
                    }
                }
            },
            _ => {
                let request_msg_id = body["in_reply_to"].as_u64().unwrap();
                match self.proxied.remove(&request_msg_id) {
                    Some(client_msg) => {
                        debug(format!("Reply from proxied message to leader - {}", body));
                        let node = self.node.as_ref().unwrap();
                        node.send_reply(body, &client_msg);
                    },
                    None => {
                        debug(format!("Ignored reply - {}", body));
                    }
                }
            }
        };
    }

    fn process_message(&mut self, msg : Message) {
        let body = msg.body.clone();
        match body["type"].as_str() {
            Some("request_vote") => {
                // debug(format!("Processing request vote {:?}", self));
                let mut granted = false;
                let request_vote : RequestVote = serde_json::from_value(body.clone()).unwrap();
                self.maybe_step_down(request_vote.term);
                if request_vote.term < self.term {
                    debug(format!("Not granting vote for term {} - our current term is {}", request_vote.term, self.term));
                } else if let Some(v) = self.voted_for.as_ref() {
                    debug(format!("Already voted for {} cannot vote for {}", v, msg.src));
                } else if request_vote.last_log_term < self.log.last().term {
                    debug(format!("Have log terms from term {} which is newer the remote last term {}", self.log.last().term, request_vote.last_log_term));
                } else if request_vote.last_log_term == self.log.last().term && request_vote.last_log_index < self.log.size() as u64 {
                    debug(format!("Both have log terms from term {} but our log has length {} whilst theirs has {}", request_vote.last_log_term, self.log.size(), request_vote.last_log_index));
                } else {
                    granted = true;
                    self.voted_for = Some(msg.src.clone());
                    self.election_clock = SystemTime::now();
                    debug(format!("Accepted vote for {} for term {}", self.voted_for.as_ref().unwrap(), self.term));
                }
                let node = self.node.as_ref().unwrap();
                node.send_reply(json!({ 
                    "type" : "request_vote_res",
                    "term" : self.term,
                    "vote_granted" : granted,
                    "commit_index" : self.commit_index
                }), &msg);                
            },
            Some("append_entries") => {
                let append_entries : AppendEntries = serde_json::from_value(body.clone()).unwrap();                
                let current_term = self.term;
                self.maybe_step_down(append_entries.term);
                if append_entries.term < current_term {
                    let node = self.node.as_ref().unwrap();
                    node.send_reply(append_entries.failure_response(), &msg);
                    return
                } 
                self.leader_id = Some(append_entries.leader_id.clone());
                self.reset_election_deadline();
                match self.log.log_entry(append_entries.prev_log_index as usize) {
                    Some(prev_entry) if append_entries.prev_log_term == prev_entry.term => {
                        // trim log to prev item
                        self.log.truncate( (append_entries.prev_log_index + 1) as usize);
                        // append entries to log
                        for entry in append_entries.entries.iter() {
                            self.log.append(entry.clone());
                        }
                        if self.commit_index < append_entries.leader_commit as usize {
                            self.commit_index = std::cmp::min(self.log.size(), append_entries.leader_commit as usize);
                            self.advance_state_machine();
                        }
                        let node = self.node.as_ref().unwrap();                
                        node.send_reply(append_entries.success_response(self.log.size()), &msg)
                    },
                    Some(_prev_entry) => {
                        // entry exists but terms don't match
                        self.log.truncate((append_entries.prev_log_index + 1) as usize);
                        let node = self.node.as_ref().unwrap();                
                        node.send_reply(append_entries.failure_response(), &msg)
                    },
                    None => {
                        let node = self.node.as_ref().unwrap();                
                        node.send_reply(append_entries.failure_response(), &msg)
                    }
                }
            },
            _ => {
                if self.state == RaftState::Leader {
                    self.log.append( RaftLogEntry{ term : self.term, operation: msg.body, client_id : msg.src } );
                } else if self.leader_id.is_some() {
                    let node = self.node.as_ref().unwrap();
                    debug(format!("Proxying to leader - {}", body));
                    let proxied_msg_id = node.send_to_node_acked(&body, self.leader_id.clone().unwrap());
                    self.proxied.insert(proxied_msg_id, msg);
                } else {
                    let node = self.node.as_ref().unwrap();
                    node.send_reply(json!({
                        "type" : "error",
                        "code" : 11,
                        "text" : "No known leader so temporarily unavailable",
                        "leader_id" : self.leader_id
                    }), &msg);
                }
            }
        };
    }

    fn notify(&mut self) {
        // based on timeouts take any needed actions
        // step down if needed
        if let Ok(elapsed) = self.step_down_clock.elapsed() {
            if elapsed.as_millis() > STEP_DOWN_PERIOD_IN_MILLISECONDS && self.state == RaftState::Leader {
                let node = self.node.as_ref().unwrap();
                debug(format!("Node {} stepping down", node.node_id));
                self.become_follower();
            }
        }
        
        // replicate logs if leader
        if let Ok(elapsed) = self.replication_clock.elapsed() {
            if elapsed.as_millis() > MIN_REPLICATION_INTERVAL && self.state == RaftState::Leader {
                self.replicate_logs();        
            }
        }

        // start election if not a leader
        if let Ok(elapsed) = self.election_clock.elapsed() {
            if elapsed.as_millis() > self.millis_till_next_election {
                 self.reset_election_deadline();
                 if self.state != RaftState::Leader {
                     self.become_candidate();
                 }
            }
        }
    }
}

fn main() -> std::io::Result<()> {
    let mut server = RaftServer::new();   
    server.run();
    Ok(())
}