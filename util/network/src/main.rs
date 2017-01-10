extern crate ethcore_network as net;
extern crate docopt;
extern crate rustc_serialize;
#[macro_use] extern crate log;
extern crate env_logger;
use log::LogLevel;

use net::*;
use std::sync::{Arc, Mutex};

use std::time::Duration;
use std::thread;
use std::str;
use std::fmt;


use std::io::prelude::*;
use std::fs::File;

use docopt::Docopt;

struct Pinger {
    rx: Arc<Mutex<[i32; 6]>>,
}

impl Pinger {
    pub fn new() -> Self {
        Pinger {
            rx: Arc::new(Mutex::new([0, 0, 0, 0, 0, 0])),
        }
    }

    pub fn send_n(&self, io: &NetworkContext, peer: &PeerId, packet_id: u8) {
        io.send(*peer, packet_id, "ping pong".to_string().into_bytes()).expect("unsuccessful send");
        ()
    }
}

impl fmt::Display for Pinger {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let counters = self.rx.lock().unwrap();
        write!(f, "\n{}=>{}\n{}=>{}\n{}=>{}\n{}=>{}\n{}=>{}\n{}=>{}",
               0, (*counters)[0],
               1, (*counters)[1],
               2, (*counters)[2],
               3, (*counters)[3],
               4, (*counters)[4],
               5, (*counters)[5],

        )
    }
}

impl NetworkProtocolHandler for Pinger {
    fn read(&self, io: &NetworkContext, peer: &PeerId, packet_id: u8, _data: &[u8]) {
        let mut counters = self.rx.lock().unwrap();
        let index: usize = (packet_id-1) as usize;
        (*counters)[index] += 1;
        let len = (*counters).len();
        if (packet_id as usize) < len {
            self.send_n(io, peer, packet_id + 1)
        }
    }

    fn connected(&self, io: &NetworkContext, peer: &PeerId) {
        for _i in 0..100 {
            io.send(*peer, 1, "hello, world".to_string().into_bytes()).unwrap()
        }
    }

    fn disconnected(&self, _io: &NetworkContext, peer: &PeerId) {
        println!("NPH Disconnected {}", peer);
    }
}

#[derive(RustcDecodable)]
struct Args {
    flag_connect: bool,
}

fn main() {
    env_logger::init().unwrap();

    // the Docopt usage string
    // libtest (--connect | --listen | --help)

    const USAGE: &'static str = "
Usage:
    libtest (--connect | --listen | --help)

Options:
    -h, --help     Show this screen.
    -c, --connect     Connect to node listed in 'nodefile' file
    -l, --listen      Write your hostname into 'nodefile' and listen

";
    let args: Args = Docopt::new(USAGE).and_then(|d| d.decode()).unwrap_or_else(|e| e.exit());

    let mut service = NetworkService::new(NetworkConfiguration::new_local())
        .expect("Error creating network service");
    service.start().expect("Error starting service");

    match args.flag_connect {
        false => listen(&mut service),
        true => connect(&mut service)
    }
}

fn connect(service: &mut NetworkService) {
    let node_name = get_own_name(&service);
    let pinger = register_pinger(service);
    println!("local_url: {}", &node_name);
    match read_node_name() {
        Ok(remote) => {
            println!("remote name: {}", remote);
            match service.add_reserved_peer(&&remote) {
                Ok(()) => {
                    println!("peer added");
                    thread::sleep(Duration::from_millis(10000));
                    let stats = service.stats();
                    println!("Got {} bytes", stats.recv());
                    println!("Did {} sessions", stats.sessions());
                    println!("Pinger: {}", pinger)
                },
                Err(_) =>
                    println!("connection failed")
            }
        },
        Err(_) => {
            println!("Error while reading file with name of remote, exiting");
            return
        }
    }
}

fn listen(service: &mut NetworkService) {
    let node_name = get_own_name(&service);
    println!("Local_url: {}", &node_name);
    let pinger = register_pinger(service);
    let node_name = service.local_url().unwrap_or("Unknown".to_string());
    write_node_name(&node_name);
    // Wait for quit condition
    thread::sleep(Duration::from_millis(10000));
    let stats = service.stats();
    println!("Got {} bytes", stats.recv());
    println!("Did {} sessions", stats.sessions());
    println!("Pinger: {}", pinger)
    // Drop the service
}

fn register_pinger(service: &NetworkService) -> Arc<Pinger> {
    let number_of_different_packet_types = 10;
    let handler = Arc::new(Pinger::new());
    service
        .register_protocol(handler.clone(), *b"myp",
                           number_of_different_packet_types, &[1u8])
        .expect("Error registering pinger protocol");
    handler
}

fn get_own_name(service: &NetworkService) -> String {
    service.local_url().unwrap_or("Unknown".to_string())
}

fn write_node_name(node_name: &str) -> Result<(), std::io::Error> {
    let mut f = try!(File::create("nodename"));
    let fs: &str= &format!("{}", node_name);
    f.write_all(fs.as_bytes())
}

fn read_node_name() -> Result<String, std::io::Error> {
    let mut f = try!(File::open("nodename"));
    let mut s = String::new();
    try!(f.read_to_string(&mut s));
    Ok(s)
}
