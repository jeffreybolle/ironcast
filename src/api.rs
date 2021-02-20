use std::convert::Infallible;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use warp::Filter;

use crate::storage;

type Node = Arc<storage::Node>;

const NAMESPACE: &str = "__default__";

pub async fn serve(port: u16, node: storage::Node) {

    let node = Arc::new(node);

    let get = warp::path!("v1" / String)
        .and(with_node(node.clone()))
        .and_then(get);

    let put = warp::path!("v1" / String)
        .and(json_body())
        .and(with_node(node.clone()))
        .and_then(put);

    let router = warp::get().and(get).or(warp::put().and(put));

    warp::serve(router)
        .run(([0, 0, 0, 0], port))
        .await
}

#[derive(Debug,Serialize,Deserialize)]
struct Object {
    value: Option<String>
}

async fn get(key: String, node: Node) -> Result<impl warp::Reply, Infallible> {
    let value = match node.get(NAMESPACE, key.as_bytes()).await.unwrap() {
        Some(v) => Some(String::from_utf8_lossy(&v).to_string()),
        None => None
    };
    Ok(warp::reply::json(&Object { value }))
}

async fn put(key: String, obj: Object, node: Node) -> Result<impl warp::Reply, Infallible> {
    match &obj.value {
        None => panic!("unimplemented"),
        Some(v) => {
            node.put(NAMESPACE, key.as_bytes(), v.as_bytes().to_vec()).await.unwrap();
        }
    };
    Ok(warp::reply::json(&obj))
}

fn json_body() -> impl Filter<Extract = (Object,), Error = warp::Rejection> + Clone {
    // When accepting a body, we want a JSON body
    // (and to reject huge payloads)...
    warp::body::content_length_limit(1024 * 16).and(warp::body::json())
}

fn with_node(node: Node) -> impl Filter<Extract = (Node,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || node.clone())
}