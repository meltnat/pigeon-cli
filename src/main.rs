//! pigeon: WebRTC DataChannel で 1 ファイルを P2P 送信する CLI。
//!
//! シグナリングサーバーは使わず、SDP を base64 文字列にして手でコピペする。
//!   送信側: `pigeon send <FILE>` → 表示されたコードを相手に渡し、相手のコードを貼る
//!   受信側: `pigeon recv`        → 相手のコードを貼り、表示されたコードを相手に返す

use anyhow::{Context, Result, anyhow, bail};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use bytes::BytesMut;
use clap::{Parser, Subcommand};
use serde_json::json;
use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use webrtc::data_channel::{DataChannel, DataChannelEvent};
use webrtc::peer_connection::{
    PeerConnection, PeerConnectionBuilder, PeerConnectionEventHandler, RTCConfigurationBuilder,
    RTCIceGatheringState, RTCIceServer, RTCPeerConnectionState, RTCSessionDescription,
};
use webrtc::runtime::default_runtime;

const CHUNK: usize = 16 * 1024; // DataChannel の 1 メッセージ上限に合わせる
const SEND_BUFFER_LIMIT: usize = 1024 * 1024; // これを超えると send() が待つ
const DEFAULT_STUN: &str = "stun:stun.l.google.com:19302";

#[derive(Parser)]
#[command(version, about = "WebRTC で P2P ファイル転送")]
struct Cli {
    /// 使用する STUN サーバー
    #[arg(long, global = true, default_value = DEFAULT_STUN)]
    stun: String,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// ファイルを送信する
    Send {
        /// 送るファイル
        file: PathBuf,
    },
    /// ファイルを受信する
    Recv {
        /// 保存先ディレクトリ
        #[arg(short, long, default_value = ".")]
        out: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Send { file } => send(&cli.stun, &file).await,
        Cmd::Recv { out } => recv(&cli.stun, &out).await,
    }
}

// ───────────────────────── PeerConnection の準備 ─────────────────────────

struct Handler {
    gathered: mpsc::Sender<()>,
    incoming_dc: mpsc::Sender<Arc<dyn DataChannel>>,
}

#[async_trait::async_trait]
impl PeerConnectionEventHandler for Handler {
    async fn on_ice_gathering_state_change(&self, state: RTCIceGatheringState) {
        if state == RTCIceGatheringState::Complete {
            let _ = self.gathered.try_send(());
        }
    }

    async fn on_connection_state_change(&self, state: RTCPeerConnectionState) {
        eprintln!("[接続状態] {state}");
        if state == RTCPeerConnectionState::Failed {
            eprintln!("接続に失敗しました（NAT 越えできなかった可能性があります）");
            std::process::exit(1);
        }
    }

    async fn on_data_channel(&self, dc: Arc<dyn DataChannel>) {
        let _ = self.incoming_dc.try_send(dc);
    }
}

struct Peer {
    pc: Arc<dyn PeerConnection>,
    gathered: mpsc::Receiver<()>,
    incoming_dc: mpsc::Receiver<Arc<dyn DataChannel>>,
}

async fn new_peer(stun: &str) -> Result<Peer> {
    let (g_tx, g_rx) = mpsc::channel(1);
    let (dc_tx, dc_rx) = mpsc::channel(1);
    let config = RTCConfigurationBuilder::new()
        .with_ice_servers(vec![RTCIceServer {
            urls: vec![stun.to_string()],
            ..Default::default()
        }])
        .build();
    let pc = PeerConnectionBuilder::new()
        .with_configuration(config)
        .with_handler(Arc::new(Handler {
            gathered: g_tx,
            incoming_dc: dc_tx,
        }))
        .with_runtime(default_runtime().ok_or_else(|| anyhow!("runtime がありません"))?)
        .with_udp_addrs(vec!["0.0.0.0:0".to_string()])
        .with_data_channel_send_buffer_limit(SEND_BUFFER_LIMIT)
        .build()
        .await?;
    Ok(Peer {
        pc: Arc::new(pc),
        gathered: g_rx,
        incoming_dc: dc_rx,
    })
}

/// ICE 候補の収集が終わるのを待ち、ローカル SDP をコピペ用文字列にする
async fn local_code(peer: &mut Peer) -> Result<String> {
    peer.gathered.recv().await;
    let desc = peer
        .pc
        .local_description()
        .await
        .ok_or_else(|| anyhow!("local description がありません"))?;
    Ok(B64.encode(serde_json::to_string(&desc)?))
}

fn read_code(prompt: &str) -> Result<RTCSessionDescription> {
    eprintln!("{prompt}");
    eprint!("> ");
    std::io::stderr().flush()?;
    let mut line = String::new();
    std::io::stdin().lock().read_line(&mut line)?;
    let json = B64.decode(line.trim()).context("コードの形式が不正です")?;
    Ok(serde_json::from_slice(&json)?)
}

/// DataChannel のイベントを別タスクで受けて mpsc に流す
/// （送信中も poll し続けないとイベントキューが詰まるため）
fn pump_events(dc: Arc<dyn DataChannel>) -> mpsc::UnboundedReceiver<DataChannelEvent> {
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Some(ev) = dc.poll().await {
            if tx.send(ev).is_err() {
                break;
            }
        }
    });
    rx
}

fn progress(done: u64, total: u64) {
    let pct = if total == 0 { 100 } else { done * 100 / total };
    eprint!("\r{pct:3}%  {done}/{total} bytes");
    let _ = std::io::stderr().flush();
}

// ───────────────────────── 送信 ─────────────────────────

async fn send(stun: &str, path: &Path) -> Result<()> {
    let mut file = tokio::fs::File::open(path)
        .await
        .with_context(|| format!("{} を開けません", path.display()))?;
    let size = file.metadata().await?.len();
    let name = path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| anyhow!("ファイル名が不正です"))?
        .to_string();

    let mut peer = new_peer(stun).await?;
    let dc = peer.pc.create_data_channel("file", None).await?;
    let mut events = pump_events(dc.clone());

    let offer = peer.pc.create_offer(None).await?;
    peer.pc.set_local_description(offer).await?;
    println!("\n▼ このコードを受信側に渡してください（pigeon recv）\n");
    println!("{}\n", local_code(&mut peer).await?);

    let answer = read_code("▼ 受信側が表示したコードを貼り付けてください")?;
    peer.pc.set_remote_description(answer).await?;

    // チャネルが開くまで待つ
    loop {
        match events.recv().await {
            Some(DataChannelEvent::OnOpen) => break,
            Some(DataChannelEvent::OnClose) | None => bail!("チャネルが閉じられました"),
            _ => {}
        }
    }

    eprintln!("送信開始: {name} ({size} bytes)");
    dc.send_text(&json!({ "name": name, "size": size }).to_string())
        .await?;

    let mut buf = vec![0u8; CHUNK];
    let mut sent = 0u64;
    loop {
        let n = file.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        dc.send(BytesMut::from(&buf[..n])).await?;
        sent += n as u64;
        progress(sent, size);
    }
    eprintln!();

    // 受信側が全部書き終えて "done" を返すまで待つ
    loop {
        match events.recv().await {
            Some(DataChannelEvent::OnMessage(m)) if m.is_string && &m.data[..] == b"done" => break,
            Some(DataChannelEvent::OnClose) | None => bail!("完了通知の前に切断されました"),
            _ => {}
        }
    }
    eprintln!("送信完了");
    peer.pc.close().await?;
    Ok(())
}

// ───────────────────────── 受信 ─────────────────────────

async fn recv(stun: &str, out_dir: &Path) -> Result<()> {
    let mut peer = new_peer(stun).await?;

    let offer = read_code("▼ 送信側が表示したコードを貼り付けてください")?;
    peer.pc.set_remote_description(offer).await?;
    let answer = peer.pc.create_answer(None).await?;
    peer.pc.set_local_description(answer).await?;
    println!("\n▼ このコードを送信側に返してください\n");
    println!("{}\n", local_code(&mut peer).await?);

    let dc = peer
        .incoming_dc
        .recv()
        .await
        .ok_or_else(|| anyhow!("DataChannel が届きませんでした"))?;
    let mut events = pump_events(dc.clone());

    let mut file: Option<(tokio::fs::File, PathBuf)> = None;
    let mut size = 0u64;
    let mut got = 0u64;

    while let Some(ev) = events.recv().await {
        let DataChannelEvent::OnMessage(msg) = ev else {
            if matches!(ev, DataChannelEvent::OnClose) {
                break;
            }
            continue;
        };
        if msg.is_string {
            // ヘッダ: {"name": ..., "size": ...}
            let header: serde_json::Value = serde_json::from_slice(&msg.data)?;
            let name = header["name"].as_str().unwrap_or("received.bin");
            // パス区切りを含む名前で out_dir の外に書かれないようにする
            let name = Path::new(name)
                .file_name()
                .ok_or_else(|| anyhow!("ファイル名が不正です"))?;
            size = header["size"].as_u64().unwrap_or(0);
            let path = out_dir.join(name);
            eprintln!("受信開始: {} ({size} bytes)", path.display());
            file = Some((tokio::fs::File::create(&path).await?, path));
        } else {
            let (f, _) = file.as_mut().ok_or_else(|| anyhow!("ヘッダ前にデータが届きました"))?;
            f.write_all(&msg.data).await?;
            got += msg.data.len() as u64;
            progress(got, size);
        }

        if let Some((f, path)) = &mut file
            && got >= size
        {
            f.flush().await?;
            eprintln!("\n保存しました: {}", path.display());
            dc.send_text("done").await?;
            // 送信側が close するのを待つ（数秒で諦める）
            let _ = tokio::time::timeout(std::time::Duration::from_secs(3), async {
                while let Some(ev) = events.recv().await {
                    if matches!(ev, DataChannelEvent::OnClose) {
                        break;
                    }
                }
            })
            .await;
            peer.pc.close().await?;
            return Ok(());
        }
    }
    bail!("転送が途中で切断されました ({got}/{size} bytes)")
}
