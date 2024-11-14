use futures::stream::StreamExt;
use futures::SinkExt;
use redis_protocol::resp2::types::BytesFrame;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use veifka::{DataStore, DataStoreError};

#[tokio::main]
async fn main() -> Result<(), DataStoreError> {
    let datastore = DataStore::new("test_datastore", "test_partition");

    let listener = tokio::net::TcpListener::bind("127.0.0.1:6379")
        .await
        .expect("Failed to bind to port");

    loop {
        let (socket, _) = listener
            .accept()
            .await
            .expect("Failed to accept connection");

        let datastore = datastore.clone()?;
        tokio::spawn(async move {
            if let Err(e) = handle_client(socket, datastore).await {
                eprintln!("Error handling client: {:?}", e)
            }
        });
    }
}

async fn handle_client(
    socket: TcpStream,
    datastore: DataStore,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut framed = Framed::new(socket, redis_protocol::codec::Resp2);
    while let Some(result) = framed.next().await {
        match result {
            Ok(frame) => {
                let response = handle_command(frame, &datastore).await;
                framed.send(response).await?;
            }
            Err(e) => {
                eprintln!("Error reading frame: {:?}", e);
                let err_response = BytesFrame::Error(format!("ERR {:?}", e).into());
                framed.send(err_response).await?;
            }
        }
    }

    Ok(())
}

async fn handle_command(frame: BytesFrame, datastore: &DataStore) -> BytesFrame {
    match frame {
        BytesFrame::SimpleString(_bytes) => todo!(),
        BytesFrame::Error(_str_inner) => todo!(),
        BytesFrame::Integer(_) => todo!(),
        BytesFrame::BulkString(_bytes) => todo!(),
        BytesFrame::Array(commands) => {
            if commands.is_empty() {
                return BytesFrame::Error("ERR Empty command".into());
            }

            let cmd = match &commands[0] {
                BytesFrame::BulkString(bytes) => {
                    String::from_utf8_lossy(bytes).to_ascii_uppercase()
                }
                BytesFrame::SimpleString(s) => String::from_utf8_lossy(s).to_ascii_uppercase(),
                _ => return BytesFrame::Error("ERR invalid command type".into()),
            };

            match cmd.as_str() {
                "PING" => BytesFrame::SimpleString("PONG".into()),
                "SET" => {
                    if commands.len() != 3 {
                        return BytesFrame::Error("ERR Wrong number of arguments for SET".into());
                    }
                    let key = match &commands[1] {
                        BytesFrame::BulkString(bytes) => bytes.clone(),
                        _ => return BytesFrame::Error("ERR Invalid key type".into()),
                    };
                    let value = match &commands[2] {
                        BytesFrame::BulkString(bytes) => bytes.clone(),
                        _ => return BytesFrame::Error("ERR Invalid value type".into()),
                    };
                    let datastore = datastore.clone();
                    match tokio::task::spawn_blocking(move || datastore.set(&key, &value)).await {
                        Ok(Ok(_)) => BytesFrame::SimpleString("OK".into()),
                        Ok(Err(e)) => BytesFrame::Error(format!("ERR SET error: {:?}", e).into()),
                        Err(e) => BytesFrame::Error(format!("ERR task error: {:?}", e).into()),
                    }
                }
                "GET" => {
                    if commands.len() != 2 {
                        return BytesFrame::Error("ERR Wrong number of arguments for GET".into());
                    }
                    let key = match &commands[1] {
                        BytesFrame::BulkString(bytes) => bytes.clone(),
                        _ => return BytesFrame::Error("ERR Invalid key type".into()),
                    };
                    let datastore = datastore.clone();
                    match tokio::task::spawn_blocking(move || datastore.get(&key)).await {
                        Ok(Ok(Some(value))) => BytesFrame::BulkString(value.to_vec().into()),
                        Ok(Ok(None)) => BytesFrame::Null,
                        Ok(Err(e)) => BytesFrame::Error(format!("ERR GET error: {:?}", e).into()),
                        Err(e) => BytesFrame::Error(format!("ERR task error: {:?}", e).into()),
                    }
                }
                "DEL" => {
                    if commands.len() < 2 {
                        return BytesFrame::Error("ERR Wrong number of arguments for DEL".into());
                    }
                    let keys: Vec<_> = commands[1..]
                        .iter()
                        .filter_map(|cmd| match cmd {
                            BytesFrame::BulkString(bytes) => Some(bytes.clone()),
                            _ => None,
                        })
                        .collect();

                    let datastore = datastore.clone();
                    match tokio::task::spawn_blocking(move || {
                        let mut deleted = 0;
                        for key in keys {
                            if datastore.delete(&key).is_ok() {
                                deleted += 1;
                            }
                        }
                        Ok::<i64, fjall::Error>(deleted)
                    })
                    .await
                    {
                        // or should we send over SimpleString with "deleted X keys"?
                        Ok(Ok(amount_deleted)) => BytesFrame::Integer(amount_deleted),
                        Ok(Err(e)) => BytesFrame::Error(format!("ERR DEL error: {:?}", e).into()),
                        Err(e) => BytesFrame::Error(format!("ERR task error: {:?}", e).into()),
                    }
                }
                "EXISTS" => {
                    if commands.len() != 2 {
                        return BytesFrame::Error(
                            "ERR Wrong number of arguments for EXISTS".into(),
                        );
                    }
                    let key = match &commands[1] {
                        BytesFrame::BulkString(bytes) => bytes.clone(),
                        _ => return BytesFrame::Error("ERR Invalid key type".into()),
                    };
                    let datastore = datastore.clone();
                    match tokio::task::spawn_blocking(move || datastore.get(&key)).await {
                        Ok(Ok(Some(_))) => BytesFrame::Integer(1),
                        Ok(Ok(None)) => BytesFrame::Integer(0),
                        Ok(Err(e)) => {
                            BytesFrame::Error(format!("ERR EXISTS error: {:?}", e).into())
                        }
                        Err(e) => BytesFrame::Error(format!("ERR task error: {:?}", e).into()),
                    }
                }
                "MGET" => {
                    if commands.len() < 2 {
                        return BytesFrame::Error("ERR Wrong number of arguments for MGET".into());
                    }
                    let keys: Vec<_> = commands[1..]
                        .iter()
                        .filter_map(|cmd| match cmd {
                            BytesFrame::BulkString(bytes) => Some(bytes.clone()),
                            _ => None,
                        })
                        .collect();
                    let datastore = datastore.clone();
                    match tokio::task::spawn_blocking(move || {
                        let mut results = Vec::with_capacity(keys.len());
                        for key in keys {
                            match datastore.get(&key)? {
                                Some(value) => {
                                    results.push(BytesFrame::BulkString(value.to_vec().into()))
                                }
                                None => results.push(BytesFrame::Null),
                            }
                        }
                        Ok::<Vec<BytesFrame>, fjall::Error>(results)
                    })
                    .await
                    {
                        Ok(Ok(results)) => BytesFrame::Array(results),
                        Ok(Err(e)) => BytesFrame::Error(format!("ERR MGET error: {:?}", e).into()),
                        Err(e) => BytesFrame::Error(format!("ERR task error: {:?}", e).into()),
                    }
                }
                _ => BytesFrame::Error(format!("ERR unknown command '{}'", cmd).into()),
            }
        }
        BytesFrame::Null => todo!(),
    }
}
