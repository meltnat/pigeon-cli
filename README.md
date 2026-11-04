# pigeon

WebRTC (DataChannel) でファイルを P2P 送信する CLI。シグナリングサーバー不要で、接続情報はコピペで交換します。STUN は Google の無料サーバー (`stun:stun.l.google.com:19302`) がデフォルトです。

## 使い方

```bash
cargo build --release
```

1. 送信側: `pigeon send ./photo.jpg` → 表示された長いコードを相手に送る（チャット等で）
2. 受信側: `pigeon recv -o ./downloads` → そのコードを貼り付け → 表示されたコードを送信側に返す
3. 送信側: 返ってきたコードを貼り付け → 転送開始

STUN サーバーは `--stun stun:stun.cloudflare.com:3478` のように変更できます。

## 制限

- TURN を使わないため、双方が厳しい NAT（対称 NAT など）の内側だと接続できないことがあります。
- 1 回の実行で 1 ファイルのみ。
