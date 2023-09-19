use futures::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use std::rc::Rc;
use std::cell::RefCell;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::tungstenite::Error;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use url::Url;

use crate::procs::SharedRunningProcs;
use crate::proto;

//------------------------------------------------------------------------------

pub struct Connection {
    write: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
}

impl Connection {
    async fn send(&mut self, msg: &proto::OutgoingMessage) -> Result<(), proto::Error> {
        let json = serde_json::to_string(msg)?;
        self.write.send(Message::Text(json)).await?;
        Ok(())
    }
}

type SharedConnection = Rc<RefCell<Connection>>;

pub struct Handler {
    read: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    connection: SharedConnection,
}

impl Handler {
    async fn handle(connection: SharedConnection, procs: SharedRunningProcs, msg: Message) -> Result<(), proto::Error> {
        match msg {
            Message::Text(json) => {
                match serde_json::from_str::<proto::IncomingMessage>(&json) {
                    Ok(msg) => {
                        eprintln!("msg: {:?}", msg);
                        match proto::handle_incoming(procs, msg).await {
                            Ok(Some(rsp)) => {
                                eprintln!("rsp: {:?}", rsp);
                                connection.borrow_mut().send(&rsp).await?
                            },
                            Ok(None) => (),
                            Err(err) => eprintln!("message error: {:?}: {}", err, json),
                        }
                    }
                    Err(err) => eprintln!("invalid JSON: {:?}: {}", err, json),
                }
            }
            _ => eprintln!("unexpected ws msg: {}", msg),
        }
        Ok(())
    }

    pub async fn run(self, procs: SharedRunningProcs) -> Result<(), Error> {
        // FIXME: Reconnect.
        self.read
            .for_each(|msg| async {
                if let Ok(msg) = msg {
                    if let Err(err) = Self::handle(self.connection.clone(), procs.clone(), msg).await {
                        eprintln!("error: {:?}", err);
                    }
                } else {
                    eprintln!("msg error: {:?}", msg.err());
                }
            })
            .await;

        Ok(())
    }
}

impl Connection {
    pub async fn connect(url: &Url) -> Result<(SharedConnection, Handler), Error> {
        eprintln!("connecting to {}", url);
        let (ws_stream, _) = connect_async(url).await?;
        eprintln!("connected");
        let (write, read) = ws_stream.split();
        let connection = Rc::new(RefCell::new(Connection{write}));
        Ok((connection.clone(), Handler { read, connection }))
    }
}
