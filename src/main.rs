use std::env::args;

use simple_logger::SimpleLogger;

mod net;
mod leader;
mod storage;
mod api;

#[tokio::main]
async fn main() {
    SimpleLogger::new().with_level(log::LevelFilter::Debug).init().unwrap();

    // alice 6000 8000 bob,127.0.0.1:6001 charlie,127.0.0.1:6002
    // bob 6001 8001 alice,127.0.0.1:6000 charlie,127.0.0.1:6002
    // charlie 6000 8002 bob,127.0.0.1:6000 alice,127.0.0.1:6000
    let args: Vec<String> = args().collect();

    let id = args[1].clone();
    let cluster_port = args[2].parse::<u16>().unwrap();
    let api_port = args[3].parse::<u16>().unwrap();


    log::info!("id = {}, port = {}", id, cluster_port);

    let mut peers = Vec::new();
    for peer in args[4..].iter() {
        let parts: Vec<String> = peer.split(",").map(|x| x.to_string()).collect();
        peers.push(net::Peer { id: parts[0].clone(), addr: parts[1].clone() });
    }

    let mut node = storage::Node::new(id, cluster_port, peers);

    node.ready().await;
    log::info!("ready!");

    api::serve(api_port, node).await;
}