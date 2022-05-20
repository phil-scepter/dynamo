use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Duration, Instant};
use crossbeam::channel::{after, bounded, tick};
use crossbeam::select;

/*
void global state in general. Instead, construct the object somewhere early (perhaps in main), then pass mutable references to that object into the places that need it. This will usually make your code easier to reason about and doesn't require as much bending over backwards.
*/


static DUMMY: &str = "dummy_value";

pub struct Peermap<'x> {
    peers: &'x Arc<RwLock<HashMap<i32, i32>>>,
}

impl Peermap<'_> {
    pub fn set(&self, key: i32, value: i32){
        let peers = self.peers.clone();
        peers.write().unwrap().insert(key, value);
    }

    pub fn get(&self, key: i32) -> i32 {
        let peers = self.peers.clone();
        let lock_result = peers.read().unwrap();
        let value = lock_result.get(&key).unwrap();

        return *value;
    }

    pub fn size(&self) -> usize {
        let peers = self.peers.clone();
        let lock_result = peers.read().unwrap();

        return lock_result.len();
    }
}

impl Clone for Peermap<'_> {
    fn clone(&'_ self) -> Self {
        return Self{
            peers: &self.peers,
        }
    }
}

pub fn main(){
    let start = Instant::now();
    let ticker = tick(Duration::from_millis(500));
    let (sender , requests) = bounded::<(i32, i32)>(3);

    let hashmap: HashMap<i32, i32> = HashMap::new();
    let rwlock: RwLock<HashMap<i32, i32>> = RwLock::new(hashmap);
    let peers: &Arc<RwLock<HashMap<i32, i32>>> = &Arc::new(rwlock);
    let peer_map: &Peermap = &Peermap{ peers };

    peer_map.set(0, 0);
    // &peer_map.gossip();

    // clone reference in main thread
    // move cloned reference out of main thread into worker thread
    // used cloned reference to create cloned struct
    let peers_2 = Arc::clone(peers);

    thread::spawn(move || {
        let mut index = 6;
        loop {
            sender.send((index, index));
            thread::sleep(Duration::from_secs(1));
            index += 1;
        }
    });

    let peers: Vec<(i32, i32)> = vec![(1, 1), (2, 2), (3, 3), (4, 4), (5, 5)];
    let mut index = 0;
    loop {
        select! {
            recv(ticker) -> _ => {
                    println!("peer_map size is {:?}", &peer_map.peers.clone().read().unwrap().keys().len()); // .size()
                },
            recv(requests) -> request => {
                    if let Ok(request) = request {
                        println!("new request received {:?}", request);
                        peer_map.set(request.0, request.1);
                    if index < 5 {
                        let (peer_name, peer_id) = peers[index];
                        peer_map.set(peer_name, peer_id);
                        index += 1;
                    }
                }
            },
        }
    }
}

/*
use std::borrow::Borrow;
use std::collections::{HashMap, HashSet};
use std::env;
use std::io::{Read, Write};
use std::net::{IpAddr, SocketAddr, TcpListener, TcpStream};
use std::thread;
use std::thread::sleep;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

static HEARTBEAT: Duration = Duration::from_secs(5);

struct Config {
    seed_node_address: SocketAddr,
    heartbeat: Duration,
}

struct WireMessage {}

fn now() -> Duration {
    return SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
}

enum WireCommand {
    NewPeersSince
}

struct WireProtocol {
}

impl WireProtocol {
    pub fn new_peers_since(conn: TcpStream) -> Result<TcpStream, &'static str> {
        //
        return Ok(conn);
    }

    pub fn parse_request(stream: &mut TcpStream) -> Result<WireMessage, &'static str> {
        let mut bytes: [u8; 128] = [0;128];
        stream.read(&mut bytes);
        if bytes.len() < 8 { // arbitrary number, replace later with correct minimum message length
            return Err("Unknown message type!");
        }

        let buffer = std::str::from_utf8(&mut bytes).expect("could not parse message");
        println!("received buffer {}", buffer);
        match &buffer[0..7] {
            "long_message" => {
                println!("Received long_message!")
            },
            "bar" => {},
            _ => return Err("Unknown message type!"),
        }

        return Ok(WireMessage{});
    }
}

struct Node {
    peers: HashMap<SocketAddr, Duration>,
    address: SocketAddr,
    config: Config,
    // todo: tcpstream conn pool
}

impl Node {
    pub fn run(&self) {
        let listener = TcpListener::bind(self.address).expect("could not bind");
        for stream in listener.incoming() {
            match stream {
                Err(e) => { eprint!("failed {}", e) }
                Ok(mut stream) => {
                    thread::spawn(move || {
                        WireProtocol::parse_request(&mut stream);
                    });
                }
            }
        }
    }

    fn gossip(&mut self) {
        // prelude
        if self.peers.keys().len() == 0 && self.address != self.config.seed_node_address {
            let timestamp = now();
            self.peers.insert(self.config.seed_node_address, timestamp);
        }

        thread::spawn( || {
            let mut count :i32 = 1;
            loop {
                println!("heartbeat {}..", count);
                let timestamp = now();
                for (peer, duration) in self.peers.clone() {
                    for new_peer in self.request_new_peers(&peer) {
                        if self.peers.contains_key(&new_peer){
                            continue;
                        }
                        // insert with lock
                        self.peers.insert(new_peer, timestamp);
                    }
                }
                count += 1;
                sleep(self.config.heartbeat);
            }
        });
    }

    fn request_new_peers(&self, peer_addr: &SocketAddr) -> HashSet<SocketAddr> {
        // no-op
        let mut stream = TcpStream::connect(peer_addr).expect("Could not connect to peer");
        stream.write("long_message".as_bytes()).expect("Failed to write to server");
        // write ping -> new peer
        // receive pong <- list of all known peers
        // split list
        // coerce into socketaddrs
        // update self.peers with any new peers
        println!("wrote long_message");
        return HashSet::new();
    }
}

fn ping(peer: SocketAddr) -> Vec<SocketAddr> {
    let mut new_peers: Vec<SocketAddr> = Vec::new();
    new_peers.push(SocketAddr::V4("192.168.0.1:8080".parse().unwrap()));
    return new_peers;
}

fn pong() -> Vec<SocketAddr>  {
    let mut peers = HashMap::new();
    let peer_age = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    peers.insert(SocketAddr::V4("192.168.0.1:8080".parse().unwrap()), peer_age);

    let mut new_peers: Vec<SocketAddr> = Vec::new();

    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    for (peer, age) in peers {
        if age < now{
            new_peers.push(peer)
        }
    }

    println!("Peers are {}", new_peers[0]);
    return new_peers;
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let hostname = &args[1];
    let port = &args[2];
    let ip_address = format!("{}:{}", hostname, port);
    let config = Config {
        seed_node_address: SocketAddr::V4("127.0.0.1:8080".parse().unwrap()),
        heartbeat: Duration::from_secs(5),
    };

    let peers: HashMap<SocketAddr, Duration> = HashMap::new();
    let mut node = &Node{
        peers: peers,
        config: config,
        address: SocketAddr::V4(ip_address.parse().unwrap()),
    };

    println!("{:?}", args);
    // get ip address and port to bind to from cmd line arguments
    // close
    node.gossip();
    node.run();
}
 */