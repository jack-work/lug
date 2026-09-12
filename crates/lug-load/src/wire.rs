use crate::{Result, error};
use lug_proto::{Request, Response, VERSION};
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::UnixStream,
    sync::mpsc,
    task::JoinHandle,
};

#[derive(Default)]
pub struct Traffic {
    pub tx: AtomicU64,
    pub rx: AtomicU64,
    pub sockets: AtomicU64,
}

impl Traffic {
    pub fn snapshot(&self) -> (u64, u64) {
        (
            self.tx.load(Ordering::Relaxed),
            self.rx.load(Ordering::Relaxed),
        )
    }
}

#[derive(Clone)]
pub struct Endpoint {
    pub socket: PathBuf,
    pub http: Option<std::net::SocketAddr>,
    pub token: Arc<String>,
}

pub struct Event {
    pub peer: usize,
    pub arrived: Instant,
    pub response: Result<Response>,
}

pub struct Peer {
    pub out: mpsc::Sender<Request>,
    tasks: Vec<JoinHandle<()>>,
}

impl Drop for Peer {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

impl Peer {
    pub fn from_tasks(out: mpsc::Sender<Request>, tasks: Vec<JoinHandle<()>>) -> Self {
        Self { out, tasks }
    }

    pub async fn connect(
        endpoint: &Endpoint,
        index: usize,
        events: mpsc::Sender<Event>,
        traffic: Arc<Traffic>,
    ) -> Result<Self> {
        if endpoint.http.is_some() {
            return crate::http::connect(endpoint, index, events, traffic).await;
        }
        let mut socket = UnixStream::connect(&endpoint.socket).await?;
        write_request(
            &mut socket,
            &Request::Hello {
                id: 0,
                version: VERSION,
            },
            &traffic,
        )
        .await?;
        let welcome = read_response(&mut socket, &traffic).await?;
        validate_welcome(&welcome)?;
        traffic.sockets.fetch_add(1, Ordering::Relaxed);
        let (mut read, mut write) = socket.into_split();
        let (out, mut inbox) = mpsc::channel::<Request>(256);
        let reader_events = events.clone();
        let reader_traffic = traffic.clone();
        let reader = tokio::spawn(async move {
            loop {
                let response = read_response(&mut read, &reader_traffic).await;
                let failed = response.is_err();
                if reader_events
                    .send(Event {
                        peer: index,
                        arrived: Instant::now(),
                        response,
                    })
                    .await
                    .is_err()
                    || failed
                {
                    break;
                }
            }
        });
        let writer = tokio::spawn(async move {
            while let Some(request) = inbox.recv().await {
                if let Err(e) = write_request(&mut write, &request, &traffic).await {
                    let _ = events
                        .send(Event {
                            peer: index,
                            arrived: Instant::now(),
                            response: Err(e),
                        })
                        .await;
                    break;
                }
            }
        });
        Ok(Self::from_tasks(out, vec![reader, writer]))
    }
}

pub fn validate_welcome(welcome: &Response) -> Result<()> {
    match welcome {
        Response::Welcome {
            version, max_frame, ..
        } if *version == VERSION && *max_frame > 0 => Ok(()),
        _ => Err(error(format!("invalid Hello response {welcome:?}"))),
    }
}

pub async fn read_response(
    reader: &mut (impl AsyncRead + Unpin),
    traffic: &Traffic,
) -> Result<Response> {
    let body = read_frame(reader).await?;
    traffic
        .rx
        .fetch_add(body.len() as u64 + 4, Ordering::Relaxed);
    Ok(serde_json::from_slice(&body)?)
}

pub async fn read_frame(reader: &mut (impl AsyncRead + Unpin)) -> Result<Vec<u8>> {
    let len = reader.read_u32().await?;
    if len > lug_proto::MAX_FRAME {
        return Err(error(format!(
            "peer frame {len} exceeds {}",
            lug_proto::MAX_FRAME
        )));
    }
    let mut body = vec![0; len as usize];
    reader.read_exact(&mut body).await?;
    Ok(body)
}

pub async fn write_request(
    writer: &mut (impl AsyncWrite + Unpin),
    request: &Request,
    traffic: &Traffic,
) -> Result<()> {
    let frame = lug_proto::encode(request)?;
    writer.write_all(&frame).await?;
    traffic.tx.fetch_add(frame.len() as u64, Ordering::Relaxed);
    Ok(())
}

pub async fn receive(events: &mut mpsc::Receiver<Event>) -> Result<Event> {
    events
        .recv()
        .await
        .ok_or_else(|| error("all socket readers exited"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn fragmented_frames_and_prefix_limit_use_real_io() {
        let (mut client, mut server) = tokio::io::duplex(16);
        let task = tokio::spawn(async move {
            let frame = lug_proto::encode(&Response::Pong { id: 42 }).unwrap();
            for byte in frame {
                server.write_all(&[byte]).await.unwrap();
            }
        });
        assert_eq!(
            read_response(&mut client, &Traffic::default())
                .await
                .unwrap(),
            Response::Pong { id: 42 }
        );
        task.await.unwrap();
        let (mut client, mut server) = tokio::io::duplex(4);
        server.write_u32(lug_proto::MAX_FRAME + 1).await.unwrap();
        assert!(
            read_frame(&mut client)
                .await
                .unwrap_err()
                .to_string()
                .contains("exceeds")
        );
    }
}
