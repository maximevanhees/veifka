use fjall::{Config, Error, Keyspace, PartitionCreateOptions, PartitionHandle};
use futures::stream::StreamExt;
use futures::SinkExt;
use redis_protocol::resp2::types::BytesFrame;
use thiserror::Error;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

#[cfg(test)]
use tempfile::TempDir;

#[derive(Error, Debug)]
pub enum ServerError {
    #[error("keyscape error")]
    KeyspaceError(String),
    #[error("create parition error")]
    CreatePartitionError(String),
    #[error("get key error")]
    GetKeyError(String),
}

#[tokio::main]
async fn main() -> Result<(), ServerError> {
    // A keyspace is a database, which may contain multiple collections ("partitions")
    let keyspace = init_keyspace("test_keyspace")?;

    // Each partition is its own physical LSM-tree
    let partition_handle = create_partition(&keyspace, "test_partition")?;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:6379")
        .await
        .expect("Failed to bind to port");

    loop {
        let (socket, _) = listener
            .accept()
            .await
            .expect("Failed to accept connection");

        let partition = partition_handle.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_client(socket, &partition).await {
                eprintln!("Error handling client: {:?}", e)
            }
        });
    }
}

fn init_keyspace(name: &str) -> Result<Keyspace, ServerError> {
    Config::new(name)
        .open()
        .map_err(|e| ServerError::KeyspaceError(e.to_string()))
}

fn create_partition(keyspace: &Keyspace, name: &str) -> Result<PartitionHandle, ServerError> {
    keyspace
        .open_partition(name, PartitionCreateOptions::default())
        .map_err(|e| ServerError::CreatePartitionError(e.to_string()))
}

async fn handle_client(
    socket: TcpStream,
    partition: &PartitionHandle,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut framed = Framed::new(socket, redis_protocol::codec::Resp2);
    while let Some(result) = framed.next().await {
        match result {
            Ok(frame) => {
                let response = handle_command(frame, partition).await;
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

async fn handle_command(frame: BytesFrame, partition: &PartitionHandle) -> BytesFrame {
    match frame {
        BytesFrame::SimpleString(bytes) => todo!(),
        BytesFrame::Error(str_inner) => todo!(),
        BytesFrame::Integer(_) => todo!(),
        BytesFrame::BulkString(bytes) => todo!(),
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
                    let partition = partition.clone();
                    match tokio::task::spawn_blocking(move || partition.insert(&key, &value)).await
                    {
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
                    let partition = partition.clone();
                    match tokio::task::spawn_blocking(move || partition.get(&key)).await {
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

                    let partition = partition.clone();
                    match tokio::task::spawn_blocking(move || {
                        let mut deleted = 0;
                        for key in keys {
                            if partition.remove(&key).is_ok() {
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
                    let partition = partition.clone();
                    match tokio::task::spawn_blocking(move || partition.get(&key)).await {
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
                    let partition = partition.clone();
                    match tokio::task::spawn_blocking(move || {
                        let mut results = Vec::with_capacity(keys.len());
                        for key in keys {
                            match partition.get(&key)? {
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

#[cfg(test)]
fn create_test_partition() -> PartitionHandle {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let keyspace = Config::new(temp_dir.path())
        .open()
        .expect("Failed to open keyspace");
    keyspace
        .open_partition("test_partition", PartitionCreateOptions::default())
        .expect("Failed to create partition")
}

#[cfg(test)]
#[tokio::test]
async fn test_ping_command() {
    let partition = create_test_partition();
    let frame = BytesFrame::Array(vec![BytesFrame::BulkString(b"PING".to_vec().into())]);
    let response = handle_command(frame, &partition).await;
    assert_eq!(response, BytesFrame::SimpleString("PONG".into()));
}

#[cfg(test)]
#[tokio::test]
async fn test_set_get_commands() {
    let partition = create_test_partition();

    // Test SET command
    let set_frame = BytesFrame::Array(vec![
        BytesFrame::BulkString(b"SET".to_vec().into()),
        BytesFrame::BulkString(b"mykey".to_vec().into()),
        BytesFrame::BulkString(b"myvalue".to_vec().into()),
    ]);
    let set_response = handle_command(set_frame, &partition).await;
    assert_eq!(set_response, BytesFrame::SimpleString("OK".into()));

    // Test GET command
    let get_frame = BytesFrame::Array(vec![
        BytesFrame::BulkString(b"GET".to_vec().into()),
        BytesFrame::BulkString(b"mykey".to_vec().into()),
    ]);
    let get_response = handle_command(get_frame, &partition).await;
    assert_eq!(
        get_response,
        BytesFrame::BulkString(b"myvalue".to_vec().into())
    );
}

#[cfg(test)]
#[tokio::test]
async fn test_del_command() {
    let partition = create_test_partition();

    // Set a key to delete
    let set_frame = BytesFrame::Array(vec![
        BytesFrame::BulkString(b"SET".to_vec().into()),
        BytesFrame::BulkString(b"delkey".to_vec().into()),
        BytesFrame::BulkString(b"value".to_vec().into()),
    ]);
    handle_command(set_frame, &partition).await;

    // Test DEL command
    let del_frame = BytesFrame::Array(vec![
        BytesFrame::BulkString(b"DEL".to_vec().into()),
        BytesFrame::BulkString(b"delkey".to_vec().into()),
    ]);
    let del_response = handle_command(del_frame, &partition).await;
    assert_eq!(del_response, BytesFrame::Integer(1));

    // Verify that the key is deleted
    let get_frame = BytesFrame::Array(vec![
        BytesFrame::BulkString(b"GET".to_vec().into()),
        BytesFrame::BulkString(b"delkey".to_vec().into()),
    ]);
    let get_response = handle_command(get_frame, &partition).await;
    assert_eq!(get_response, BytesFrame::Null);
}

#[cfg(test)]
#[tokio::test]
async fn test_exists_command() {
    let partition = create_test_partition();

    // Key should not exist initially
    let exists_frame = BytesFrame::Array(vec![
        BytesFrame::BulkString(b"EXISTS".to_vec().into()),
        BytesFrame::BulkString(b"existkey".to_vec().into()),
    ]);
    let exists_response = handle_command(exists_frame.clone(), &partition).await;
    assert_eq!(exists_response, BytesFrame::Integer(0));

    // Set the key
    let set_frame = BytesFrame::Array(vec![
        BytesFrame::BulkString(b"SET".to_vec().into()),
        BytesFrame::BulkString(b"existkey".to_vec().into()),
        BytesFrame::BulkString(b"value".to_vec().into()),
    ]);
    handle_command(set_frame, &partition).await;

    // Key should now exist
    let exists_response = handle_command(exists_frame, &partition).await;
    assert_eq!(exists_response, BytesFrame::Integer(1));
}

#[cfg(test)]
#[tokio::test]
async fn test_mget_command() {
    let partition = create_test_partition();

    // Set multiple keys
    let keys_values = vec![
        (b"key1", b"value1"),
        (b"key2", b"value2"),
        (b"key3", b"value3"),
    ];
    for (key, value) in &keys_values {
        let set_frame = BytesFrame::Array(vec![
            BytesFrame::BulkString(b"SET".to_vec().into()),
            BytesFrame::BulkString(key.to_vec().into()),
            BytesFrame::BulkString(value.to_vec().into()),
        ]);
        handle_command(set_frame, &partition).await;
    }

    // Test MGET command
    let mget_frame = BytesFrame::Array(vec![
        BytesFrame::BulkString(b"MGET".to_vec().into()),
        BytesFrame::BulkString(b"key1".to_vec().into()),
        BytesFrame::BulkString(b"key2".to_vec().into()),
        BytesFrame::BulkString(b"nonexistent".to_vec().into()),
        BytesFrame::BulkString(b"key3".to_vec().into()),
    ]);
    let mget_response = handle_command(mget_frame, &partition).await;

    // Expected response: [value1, value2, nil, value3]
    let expected_response = BytesFrame::Array(vec![
        BytesFrame::BulkString(b"value1".to_vec().into()),
        BytesFrame::BulkString(b"value2".to_vec().into()),
        BytesFrame::Null,
        BytesFrame::BulkString(b"value3".to_vec().into()),
    ]);
    assert_eq!(mget_response, expected_response);
}

#[cfg(test)]
#[tokio::test]
async fn test_set_wrong_arguments() {
    let partition = create_test_partition();

    // Missing value argument
    let set_frame = BytesFrame::Array(vec![
        BytesFrame::BulkString(b"SET".to_vec().into()),
        BytesFrame::BulkString(b"key".to_vec().into()),
        // Missing value
    ]);
    let response = handle_command(set_frame, &partition).await;
    assert_eq!(
        response,
        BytesFrame::Error("ERR Wrong number of arguments for SET".into())
    );
}

#[cfg(test)]
#[tokio::test]
async fn test_unknown_command() {
    let partition = create_test_partition();

    let frame = BytesFrame::Array(vec![BytesFrame::BulkString(b"UNKNOWN".to_vec().into())]);
    let response = handle_command(frame, &partition).await;
    assert_eq!(
        response,
        BytesFrame::Error("ERR unknown command 'UNKNOWN'".into())
    );
}
