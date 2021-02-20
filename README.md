# Iron Cast

Proof-of-concept distributed key-value store written in async Rust:

- Async net code build on-top of the Tokio framework
- Leadership election loosely based on Raft
- Designed to have a small memory footprint
- Avoids the split-brain problem by requiring a fixed cluster size
- This is a proof-of-concept only, **DO NOT USE THIS IN PRODUCTION**
- There are plenty of TODOs in the code and a lot of things are not yet finished

## Usage

You can create a cluster, write key-value pairs to it and read them back:
```shell
cargo run alice 6000 8000 bob,127.0.0.1:6001 charlie,127.0.0.1:6002 &
cargo run bob 6001 8001 alice,127.0.0.1:6000 charlie,127.0.0.1:6002 &
cargo run charlie 6000 8002 bob,127.0.0.1:6000 alice,127.0.0.1:6000 &

curl -X PUT http://localhost:8000/v1/key1 -H 'Content-Type: application/json' -d '{"value":"value1"}'

curl http://localhost:8000/v1/key1
```

# License

Iron Cast is distributed under the terms of both the MIT license and the
Apache License (Version 2.0).

See [LICENSE-APACHE](LICENSE-APACHE) and [LICENSE-MIT](LICENSE-MIT), and
[COPYRIGHT](COPYRIGHT) for details.
