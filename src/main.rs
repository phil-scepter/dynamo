use std::borrow::Borrow;
use std::collections::{HashMap, HashSet};
use std::env;
use std::io::{Read, Write};
use std::net::{IpAddr, SocketAddr, TcpListener, TcpStream};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

static HEARTBEAT: Duration = Duration::from_secs(2);

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

    pub fn parse_request(bytes: &mut [u8]) -> Result<WireMessage, &'static str> {
        if bytes.len() < 8 { // arbitrary number, replace later with correct minimum message length
            return Err("Unknown message type!");
        }

        let buffer = std::str::from_utf8(bytes).expect("could not parse message");

        match &buffer[0..7] {
            "foo" => {},
            "bar" => {},
            _ => panic!(),
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
    pub fn run(
        &mut self,
    ) {
        let listener = TcpListener::bind(self.address).expect("could not bind");
        for stream in listener.incoming() {
            match stream {
                Err(e) => { eprint!("failed {}", e) }
                Ok(stream) => {
                    thread::spawn(move || {
                        pong()
                    });
                }
            }
        }
    }

    fn bootstrap(&mut self) {
        let timestamp = now();
        if self.peers.keys().len() == 0 && self.address != self.config.seed_node_address {
            self.peers.insert(self.config.seed_node_address, timestamp);
        }

        for (peer, duration) in self.peers.clone() {
            for new_peer in self.request_new_peers(peer) {
                if self.peers.contains_key(&new_peer){
                    continue;
                }
                self.peers.insert(new_peer, timestamp);
            }
        }

        for peer in self.peers.keys() {
            // todo: change to request_new_peers
            self.connect_to_new_peer(*peer);
        }
    }

    fn connect_to_new_peer(&self, peer_addr: SocketAddr) -> Result<TcpStream, &'static str> {
        // no-op
        let mut stream = TcpStream::connect(peer_addr).expect("Could not connect to peer");
        stream.write("input".as_bytes()).expect("Failed to write to server");
        // write ping -> new peer
        // receive pong <- list of all known peers
        // split list
        // coerce into socketaddrs
        // update self.peers with any new peers
        return Ok(stream);
    }

    fn request_new_peers(&mut self, peer_addr: SocketAddr) -> HashSet<SocketAddr> {
        // self.connect_to_peer(self.config.seed_node_address);
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
    let config = Config{
        seed_node_address: SocketAddr::V4("0.0.0.0:8000".parse().unwrap()),
        heartbeat: HEARTBEAT,
    };

    let peers: HashMap<SocketAddr, Duration> = HashMap::new();
    let mut node = Node{
        peers: peers,
        config: config,
        address: SocketAddr::V4(ip_address.parse().unwrap()),
    };

    println!("{:?}", args);
    // get ip address and port to bind to from cmd line arguments
    // close
    node.run()
}
