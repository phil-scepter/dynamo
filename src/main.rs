use std::collections::{HashMap};
use std::net::{IpAddr, TcpListener};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

struct Node {
    peers: HashMap<IpAddr, Duration>,
    heartbeat: Duration,
    address: IpAddr
}

impl Node {
    fn run(){
        let listener = TcpListener::bind("0.0.0.0:8080").expect("could not bind");
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
}

fn ping(peer: IpAddr) -> Vec<IpAddr> {
    let mut new_peers: Vec<IpAddr> = Vec::new();
    new_peers.push(IpAddr::V4("192.168.0.1:8080".parse().unwrap()));
    return new_peers;
}

fn pong() -> Vec<IpAddr>  {
    let mut peers = HashMap::new();
    let peer_age = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    peers.insert(IpAddr::V4("192.168.0.1".parse().unwrap()), peer_age);

    let mut new_peers: Vec<IpAddr> = Vec::new();

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
    Node::run()
}
