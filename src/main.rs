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
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::net::{IpAddr, SocketAddr, SocketAddrV4, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::{Arc, RwLock};
use std::{env, io, thread};
use std::fmt::{Debug, Error};
use std::thread::sleep;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use crossbeam::channel::{after, bounded, tick};
use crossbeam::select;

fn remove_whitespace(s: &str) -> String {
    s.split_whitespace().collect()
}

fn now() -> Duration {
    return SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
}

struct BufTcpStream {
    input: BufReader<TcpStream>,
    unused: BufWriter<TcpStream>,
    output: TcpStream,
}

impl BufTcpStream {
    fn new(stream: TcpStream) -> io::Result<Self> {
        let input = BufReader::new(stream.try_clone()?);
        let unused = BufWriter::new(stream.try_clone()?);
        let output = stream;

        Ok(Self { input, unused, output })
    }
}

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
    fn process(&self, stream: BufTcpStream); // -> std::result::Result<Self, Error>;
}

struct RequestPeers {
    peer_map: PeerMap,
}

impl RequestPeers {
    fn new(peer_map: PeerMap) -> Self {
        return RequestPeers{peer_map};
    }
}

impl WireMessage for RequestPeers {
    fn process(&self, mut stream: BufTcpStream) {
        println!("writing peers to stream");
        let mut buffer = "".to_owned(); // ":127.0.0.1:8001\n127.0.0.1:8002\n127.0.0.1:8003\n127.0.0.1:8004\n127.0.0.1:8005\n";
        for (peer_addr, _) in self.peer_map.pairs() {
            buffer += &format!("{}\n", peer_addr);
        }

        println!("payload is {}", buffer);
        let bytes_written = stream.unused.write(buffer.as_bytes()).unwrap(); // expect("Failed to write to server");

        println!("wrote {} bytes to buffer", bytes_written);
        match stream.output.flush(){
            Ok(ok) => {
                println!("success response from flush {:?}", ok);
            },
            Err(e) => {
                println!("error response from flush {:?}", e);
            }
        };
        println!("flushed result");
        return;
    }
}

struct WireProtocol {
}

impl WireProtocol {
    pub fn parse_request(mut stream: TcpStream, peer_map: PeerMap) {
        let mut buf_stream = match BufTcpStream::new(stream) {
            Ok(buf_stream) => {
                println!("successfully created BufTcpStream");
                buf_stream
            },
            Err(e) => {
                println!("error creating BufTcpStream {}", e);
                return;
            },
        };

        let mut command= String::new();
        println!("about to read result");
        let read_result = buf_stream.input.read_line(&mut command);
        println!("read_result: {:?}", read_result);
        let mut total_bytes_read = match read_result {
            Ok(n) => {
                println!("n: {}", n);
                n
            },
            Err(e) => {
                println!("error reading off buf_stream {}", e);
                return;
            },
        };

        println!("total_bytes_read: {}", total_bytes_read);

        if total_bytes_read == 0 || command.len() < 13 { // number can change replace later with codified minimum message length
            return
        }

        let request_peers = "request_peers\n".to_string();

        let mut peer_address = String::new();
        total_bytes_read = buf_stream.input.read_line(&mut peer_address).unwrap();
        peer_address = remove_whitespace(&peer_address);
        println!("peer_address value is {}", peer_address);
        println!("total_bytes_read for peer_address: {}", total_bytes_read);

        let remote_addr = match peer_address.parse::<SocketAddr>(){
            Ok(v) => {
                println!("remote socket_addr is {}", v);
                v
            },
            Err(e) => {
                println!("error coercing remote socket_addr from str : {}", e);
                return
            },
        };
        println!("remote_addr is {}", remote_addr);

        if None == peer_map.get(&remote_addr) {
            peer_map.set(remote_addr, now());
        }

        match command { // newline
            request_peers=> {
                let handler = RequestPeers::new(peer_map);
                println!("created handler");
                handler.process(buf_stream)
            },
            v => {
                println!("unexpected value {}", v);
                return;
            },
        }

        return
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
                    let peer_map: PeerMap = self.peer_map.clone();

                    thread::spawn(move || { // thread pool
                        WireProtocol::parse_request(stream, peer_map);
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
        println!("requesting new peers from {}", peer_addr);
        let buf = &mut [0; 74];
        let mut new_peers = HashSet::new();

        if self.address == *peer_addr {
            return None;
        }

        let conn_result = TcpStream::connect(peer_addr);
        match conn_result {
            Ok(mut conn) => {
                let local_addr = self.address.to_string();
                println!("local_addr is {}", local_addr);
                let request = format!("request_peers\n{}\n", local_addr);
                let resp = conn.write(request.as_bytes());
                if resp.is_err() {
                    println!("Failed to send request to peer {}", peer_addr);
                    return None;
                }

                println!("wrote request_peers to peer {} with {}", peer_addr, request);
                conn.set_read_timeout(Some(Duration::from_millis(5000)));
                conn.read(buf);
                println!("read response from peer {} is {:?}", peer_addr, buf);
            },
            Err(e) => {
                println!("Failed to connect to peer {}: {}", peer_addr, e);
                return None
            }
        }

        let mut peer_addrs = match std::str::from_utf8(buf) {
            Ok(v) => v,
            Err(e) => {
                print!("Invalid UTF-8 sequence: {}", e);
                return None;
            },
        };

        println!("peer_addrs from peer {} are ..{}..", peer_addr, peer_addrs);
        for peer_addr in peer_addrs.split("\n") {
            let peer_addr = &remove_whitespace(&peer_addr);
            println!("peer_addr is {} - len {}", peer_addr, peer_addr.len());
            let socket_addr = match peer_addr.parse::<SocketAddr>(){
                Ok(val) => val,
                Err(_) => continue,
            };
            new_peers.insert(socket_addr);
        }
        println!("new_peers is {:?}", new_peers);

        return Some(new_peers);
    }
}

pub fn main(){
    let ticker = tick(Duration::from_millis(2500));
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
    let seed_node_address = SocketAddr::V4("127.0.0.1:8002".parse().unwrap());

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

    listener_node.listen();
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