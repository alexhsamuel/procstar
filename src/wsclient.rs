use futures::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::sleep;
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::{
    connect_async_tls_with_config, Connector, MaybeTlsStream, WebSocketStream,
};
use url::Url;

use crate::procinfo::ProcessInfo;
use crate::procs::{ProcNotification, ProcNotificationReceiver, SharedProcs};
use crate::proto;

// FIXME: Replace `eprintln` for errors with something more appropriate.

//------------------------------------------------------------------------------

/// The read end of a split websocket.
pub type SocketReceiver = SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>;

/// The write end of a split websocket.
pub type SocketSender = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;

pub struct Connection {
    /// The remote server URL to which we connect.
    url: Url,
    /// Information about the connection.
    conn: proto::ConnectionInfo,
    /// Information about this process running procstar.
    proc: ProcessInfo,
}

impl Connection {
    pub fn new(url: &Url, conn_id: Option<&str>, group_id: Option<&str>) -> Self {
        let url = url.clone();
        let conn_id = conn_id.map_or_else(|| proto::get_default_conn_id(), |n| n.to_string());
        let group_id = group_id.map_or(proto::DEFAULT_GROUP.to_string(), |n| n.to_string());
        let conn = proto::ConnectionInfo { conn_id, group_id };
        let proc = ProcessInfo::new_self();
        Connection { url, conn, proc }
    }
}

/// Handler for incoming messages on a websocket client connection.
async fn handle(procs: SharedProcs, msg: Message) -> Result<Option<Message>, proto::Error> {
    match msg {
        Message::Binary(json) => {
            let msg = serde_json::from_slice::<proto::IncomingMessage>(&json)?;
            eprintln!("msg: {:?}", msg);
            if let Some(rsp) = proto::handle_incoming(procs, msg).await {
                eprintln!("rsp: {:?}", rsp);
                let json = serde_json::to_vec(&rsp)?;
                Ok(Some(Message::Binary(json)))
            } else {
                Ok(None)
            }
        }
        Message::Ping(payload) => Ok(Some(Message::Pong(payload))),
        Message::Close(_) => Err(proto::Error::Close),
        _ => Err(proto::Error::WrongMessageType(format!(
            "unexpected ws msg: {:?}",
            msg
        ))),
    }
}

async fn send(sender: &mut SocketSender, msg: proto::OutgoingMessage) -> Result<(), proto::Error> {
    let json = serde_json::to_vec(&msg)?;
    sender.send(Message::Binary(json)).await.unwrap();
    Ok(())
}

async fn connect(
    connection: &mut Connection,
) -> Result<(SocketSender, SocketReceiver), proto::Error> {
    eprintln!("connecting to {}", connection.url);

    let mut builder = native_tls::TlsConnector::builder();
    builder.danger_accept_invalid_certs(true);
    builder.danger_accept_invalid_hostnames(true);
    builder.min_protocol_version(Some(native_tls::Protocol::Tlsv12));
    let connector = Connector::NativeTls(builder.build().unwrap()); // FIXME: Unwrap.

    let (ws_stream, _) =
        connect_async_tls_with_config(&connection.url, None, false, Some(connector)).await.unwrap();
    eprintln!("connected");
    let (mut sender, receiver) = ws_stream.split();

    // Send a register message.
    let register = proto::OutgoingMessage::Register {
        conn: connection.conn.clone(),
        proc: connection.proc.clone(),
    };
    send(&mut sender, register).await?;

    Ok((sender, receiver))
}

/// Constructs an outgoing message corresponding to a notification message.
fn notification_to_message(
    procs: &SharedProcs,
    noti: ProcNotification,
) -> Option<proto::OutgoingMessage> {
    match noti {
        ProcNotification::Start(proc_id) | ProcNotification::Complete(proc_id) => {
            // Look up the proc.
            if let Some(proc) = procs.get(&proc_id) {
                // Got it.  Send its result.
                let res = proc.borrow().to_result();
                Some(proto::OutgoingMessage::ProcResult { proc_id, res })
            } else {
                // The proc has disappeared since the notification was sent;
                // it must have been deleted.
                None
            }
        }

        ProcNotification::Delete(proc_id) => Some(proto::OutgoingMessage::ProcDelete { proc_id }),
    }
}

/// Background task that receives notification messages through `noti_sender`,
/// converts them to outgoing messages, and sends them via `sender`.
async fn send_notifications(
    procs: SharedProcs,
    mut noti_receiver: ProcNotificationReceiver,
    sender: Rc<RefCell<Option<SocketSender>>>,
) {
    loop {
        // Wait for a notification to arrive on the channel.
        match noti_receiver.recv().await {
            Some(noti) => {
                // Borrow the websocket sender.
                if let Some(sender) = sender.borrow_mut().as_mut() {
                    // Generate the outgoing message corresponding to the
                    // notification.
                    if let Some(msg) = notification_to_message(&procs, noti) {
                        // Send the outgoing message.
                        if let Err(err) = send(sender, msg).await {
                            eprintln!("msg send error: {:?}", err);
                            // Close the websocket.
                            if let Err(err) = sender.close().await {
                                eprintln!("websocket close error: {:?}", err);
                            }
                        }
                    } else {
                        // No outgoing message corresponding to this
                        // notification.
                    }
                } else {
                    // No current websocket sender; we are not currently
                    // connected.  Drop this notification.
                }
            }
            // End of channel.
            None => break,
        }
    }
}

/// Wait time before reconnection attempts.
const RECONNECT_INTERVAL_START: Duration = Duration::from_millis(100);
const RECONNECT_INTERVAL_MULT: f64 = 2.;
const RECONNECT_INTERVAL_MAX: Duration = Duration::from_secs(30);

pub async fn run(mut connection: Connection, procs: SharedProcs) -> Result<(), proto::Error> {
    // Create a shared websocket sender, which is shared between the
    // notification sender and the main message loop.
    let sender: Rc<RefCell<Option<SocketSender>>> = Rc::new(RefCell::new(None));

    // Subscribe to receive asynchronous notifications, such as when a process
    // completes.
    let noti_receiver = procs.subscribe();
    // Start a task that sends notifications as outgoing messages to the
    // websocket.
    let _noti_task = tokio::task::spawn_local(send_notifications(
        procs.clone(),
        noti_receiver,
        sender.clone(),
    ));

    let mut interval = RECONNECT_INTERVAL_START;
    loop {
        // (Re)connect to the service.
        let (new_sender, mut receiver) = match connect(&mut connection).await {
            Ok(pair) => pair,
            Err(err) => {
                eprintln!("error: {:?}", err);
                // Reconnect, after a moment.
                // FIXME: Is this the right policy?
                sleep(interval).await;
                interval = interval.mul_f64(RECONNECT_INTERVAL_MULT);
                if RECONNECT_INTERVAL_MAX < interval {
                    interval = RECONNECT_INTERVAL_MAX;
                }
                std::process::exit(1);
                // continue;
            }
        };
        // Connected.  There's now a websocket sender available.
        sender.replace(Some(new_sender));

        loop {
            match receiver.next().await {
                Some(Ok(msg)) => match handle(procs.clone(), msg).await {
                    Ok(Some(rsp))
                        // Handling the incoming message produced a response;
                        // send it back.
                        => if let Err(err) = sender.borrow_mut().as_mut().unwrap().send(rsp).await {
                            eprintln!("msg send error: {:?}", err);
                            break;
                        },
                    Ok(None)
                        // Handling the message produced no response.
                        => {},
                    Err(err)
                        // Error while handling the message.
                        => {
                            eprintln!("msg handle error: {:?}", err);
                            break;
                        },
                },
                Some(Err(err)) => {
                    eprintln!("msg receive error: {:?}", err);
                    break;
                }
                None => {
                    eprintln!("msg stream end");
                    break;
                }
            }
        }

        // The connection is closed.  No sender is available.
        sender.replace(None);

        // Go back and reconnect.
    }
}
