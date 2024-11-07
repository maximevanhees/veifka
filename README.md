Log-structured Merge Tree DB with Fjall-rs backend and RESP frontend.
- https://github.com/fjall-rs/fjall

## Example usage
```
$ redis-cli -p 6379
127.0.0.1 > PING
PONG
127.0.0.1 > SET testkey testval
OK
127.0.0.1 > GET testkey
testval
```
