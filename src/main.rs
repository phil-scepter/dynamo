/*
next steps:
- clean up multithreaded hashmap implementation
- integrate basic networking implementation/gossip protocol
- tests for hashmap concurrency & basic networking (including python)
- consistent hashing implementation & tests
- eventual consistency parameter tuning
*/

use std::collections::{HashMap, HashSet};
use std::collections::hash_map::Iter;
use std::hash::Hash;
use std::io::{Read, Write};
use std::net::{IpAddr, SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::{Arc, RwLock};
use std::{env, thread};
use std::fmt::Error;
use std::thread::sleep;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use crossbeam::channel::{after, bounded, tick};
use crossbeam::select;

pub struct PeerMap {
    peers: Arc<RwLock<HashMap<SocketAddr, Duration>>>, // socketaddr :: duration
}

impl PeerMap<> {
    pub fn set(&self, peer_addr: SocketAddr, last_seen: Duration){
        let peers = self.peers.clone();
        peers.write().unwrap().insert(peer_addr, last_seen);
    }

    pub fn get(&self, socket_addr: &SocketAddr) -> Option<(Duration)> {
        let peers = self.peers.clone();
        let lock_result = peers.read().unwrap();

        return match lock_result.get(socket_addr) {
            Some(value) => Some(*value),
            None => None,
        };
    }

    pub fn size(&self) -> usize {
        let peers = self.peers.clone();
        let lock_result = peers.read().unwrap();

        return lock_result.len();
    }

    pub fn pairs(&self) -> Vec<(SocketAddr, Duration)> {
        let peers = self.peers.clone();
        let lock_result = peers.read().unwrap();

        let mut pairs = vec![];

        for (socket_addr, duration) in lock_result.iter() {
            pairs.push((*socket_addr, *duration));
        }

        return pairs;
    }
}

impl Clone for PeerMap {
    fn clone(&self) -> Self {
        return Self{
            peers: Arc::clone(&self.peers),
        }
    }
}

trait WireMessage {
    fn process(stream: &mut TcpStream); // -> std::result::Result<Self, Error>;
}

struct RequestPeers {}

impl WireMessage for RequestPeers {
    fn process(stream: &mut TcpStream) {
        let peers = "127.0.0.1:8001\n127.0.0.1:8002\n127.0.0.1:8003\n127.0.0.1:8004\n127.0.0.1:8006";
        stream.write(peers.as_bytes()).expect("Failed to write to server");

        return; // Ok(RequestPeers{});
    }
}

struct WireProtocol {
}

impl WireProtocol {
    pub fn parse_request(stream: &mut TcpStream) { // -> std::result::Result<WireMessage, Error> {
        let mut bytes: [u8; 128] = [0;128]; // todo - arbitrary sized read
        stream.read(&mut bytes);
        if bytes.len() < 8 { // arbitrary number, replace later with correct minimum message length
            return // Err("Unknown message type!".into_string());
        }

        let buffer = std::str::from_utf8(&mut bytes).expect("could not parse message");
        println!("received buffer {}", buffer);
        match &buffer[0..13] {
            "request_peers" => {
                RequestPeers::process(stream)
            },
            "bar" => {},
            _ => return, // Err("Unknown message type!".into_string()),
        }

        return // Ok(WireMessage{});
    }
}

#[derive(Clone, Copy)]
struct Config {
    seed_node_address: SocketAddr,
    heartbeat: Duration,
}

struct Node {
    peer_map: PeerMap,
    address: SocketAddr,
    config: Config,
    // todo: TcpStream conn pool
}

impl Node {
    pub fn listen(&self) {
        let listener = TcpListener::bind(self.address).expect("could not bind");
        for stream in listener.incoming() {
            match stream {
                Err(e) => { eprint!("failed {}", e) }
                Ok(mut stream) => {
                    thread::spawn(move || { // thread pool
                        WireProtocol::parse_request(&mut stream);
                    });
                }
            }
        }
    }

    // this is a static method or even a separate function
    // i don't think rust wants me to spin a thread referencing self inside a method
    fn gossip(&self) {
        // prelude
        if self.peer_map.size() == 0 && self.address != self.config.seed_node_address {
            let timestamp = now();
            self.peer_map.set(self.config.seed_node_address, timestamp);
        }

        let peers = Arc::clone(&self.peer_map.peers);
        let peer_map = PeerMap{peers};
        let mut count :i32 = 1;
        loop {
            println!("heartbeat {}..", count);
            let timestamp = now();
            for (peer, duration) in peer_map.pairs().clone() {
                let new_peers = self.request_new_peers(&peer);
                match new_peers {
                    None => {},
                    Some(new_peers) => {
                        for new_peer in &new_peers {
                            if let None = peer_map.get(&new_peer){
                                // insert with lock
                                peer_map.set(*new_peer, timestamp); //todo: timestamp should come from response?
                            }
                        }
                    }
                }
            }
            count += 1;
            sleep(self.config.heartbeat);
        }
    }

    fn request_new_peers(&self, peer_addr: &SocketAddr) -> Option<HashSet<SocketAddr>> {
        println!("requesting new peers..");
        let buf = &mut [0; 74];
        let mut new_peers = HashSet::new();

        if self.address == *peer_addr {
            println!("early exit");
            return None; // ret none probably redundant, either Result<Hashmap, Err> or just return an empty hashmap
        }

        let conn_result = TcpStream::connect(peer_addr);
        match conn_result {
            Ok(mut conn) => {
                let resp = conn.write("request_peers".as_bytes());
                if resp.is_err() {
                    println!("Failed to write to node");
                    return None;
                }

                println!("wrote request_peers");
                conn.set_read_timeout(Some(Duration::from_millis(5000)));
                conn.read(buf);
                println!("read response from buf is {:?}", buf);
            },
            Err(e) => {
                println!("Failed to connect to server: {}", e);
                return None
            }
        }

        let peer_addrs = match std::str::from_utf8(buf) {
            Ok(v) => v,
            Err(e) => {
                panic!("Invalid UTF-8 sequence");
            },
        };
        // let peer_addrs: std::iter::Iterator<Item=&str> = peer_addrs_str.split("\n").collect();
        println!("peer_addrs are {}", peer_addrs);

        for peer_addr in peer_addrs.split("\n") {
            println!("peer_addr is {} - len {}", peer_addr, peer_addr.replace(
                " ",
                "",
            ).len());
            let socket_addr = SocketAddr::V4(peer_addr.parse().unwrap());
            new_peers.insert(socket_addr);
        }
        println!("new_peers is {:?}", new_peers);

        // split list
        // coerce into socketaddrs
        // update self.peers with any new peers
        return Some(new_peers);
    }
}

fn now() -> Duration {
    return SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
}

pub fn main(){
    let ticker = tick(Duration::from_millis(500));
    let (sender , requests) = bounded::<(SocketAddr, Duration)>(3);

    let hashmap: HashMap<SocketAddr, Duration> = HashMap::new();
    let rwlock: RwLock<HashMap<SocketAddr, Duration>> = RwLock::new(hashmap);
    let peers: Arc<RwLock<HashMap<SocketAddr, Duration>>> = Arc::new(rwlock);

    // clone reference in main thread
    // move cloned reference out of main thread into worker thread
    // used cloned reference to create cloned struct
    let peers_clone = Arc::clone(&peers);
    let ticker_clone = ticker.clone();
    let requests_clone= requests.clone();

    thread::spawn(move || {
        let peer_map: &PeerMap = &PeerMap { peers: peers_clone };
        loop {
            select! {
                recv(ticker_clone) -> _ => {
                    println!("peer_map size is {:?} - worker 1", &peer_map.size());
                },
                recv(requests_clone) -> request => {
                    if let Ok(request) = request {
                        // println!("new request received {:?} - worker 1", request);
                        peer_map.set(request.0, request.1);
                    }
                },
            }
        }
    });

    let args: Vec<String> = env::args().collect();
    let hostname = &args[1];
    let port = &args[2];
    let seed_node_address = SocketAddr::V4("127.0.0.1:8080".parse().unwrap());

    let config = Config {
        seed_node_address: seed_node_address,
        heartbeat: Duration::from_secs(5),
    };

    let gossip_node = Node{
        peer_map: PeerMap {peers: Arc::clone(&peers)},
        config: config,
        address: SocketAddr::V4(format!("{}:{}", hostname, port).parse().unwrap()),
    };

    // get ip address and port to bind to from cmd line arguments
    // close

    thread::spawn(move || {
        gossip_node.gossip();
    });

    let listener_node = Node{
        peer_map: PeerMap {peers: Arc::clone(&peers)},
        config: config,
        address: SocketAddr::V4(format!("{}:{}", hostname, port).parse().unwrap()),
    };

    thread::spawn(move || {
        listener_node.listen();
    });

    let mut index = 1;
    loop {
        let ip_addr = format!("127.0.0.1:8080");
        sender.send((SocketAddr::V4(ip_addr.parse().unwrap()), now()));
        thread::sleep(Duration::from_secs(1));
        index += 1;
    }
}

#[test]
fn test_peermap(){ // ensure that peermap provides a thread safe interface
    let ticker = tick(Duration::from_millis(50));
    let (sender , requests) = bounded::<(SocketAddr, Duration)>(5);
    let timeout = after(Duration::from_millis(500));

    let hashmap: HashMap<SocketAddr, Duration> = HashMap::new();
    let rwlock: RwLock<HashMap<SocketAddr, Duration>> = RwLock::new(hashmap);
    let peers: &Arc<RwLock<HashMap<SocketAddr, Duration>>> = &Arc::new(rwlock);

    let peers_clone = peers.clone();
    let ticker_clone = ticker.clone();
    let requests_clone= requests.clone();

    thread::spawn( move || {
        let peer_map: &PeerMap = &PeerMap { peers: peers_clone };
        loop {
            select! {
                recv(ticker_clone) -> _ => {
                    println!("peer_map size is {:?} - worker 1", &peer_map.size());
                },
                recv(requests_clone) -> request => {
                    if let Ok(request) = request {
                        println!("new request received {:?} - worker 1", request);
                        peer_map.set(request.0, request.1);
                    }
                },
            }
        }
    });

    let peer_map: &PeerMap = &PeerMap { peers: peers.clone() };
    assert_eq!(peer_map.size(), 0);

    let mut index = 1;
    while index <= 5 {
        let ip_addr = format!("0.0.0.0:{}", index);
        sender.send((SocketAddr::V4(ip_addr.parse().unwrap()), now()));
        thread::sleep(Duration::from_millis(100));
        index += 1;
    }
    assert_eq!(peer_map.size(), 5);

    // TODO: test get interface
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

fn now() -> Duration {
    return SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
}

enum WireCommand {
    NewPeersSince
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