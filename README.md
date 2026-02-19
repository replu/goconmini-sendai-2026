[Go Conference mini 2026 in Sendai](https://sendaigo.jp//) の「database/sql/driverを理解してカスタムデータベースドライバーを作る」発表で使用したサンプルコードです。

## 必要なもの

- Go 1.26+
- Docker (Docker Compose)

## リポジトリ構成

```
.
├── constdriver/          # 固定値を返すだけの最小限のドライバー実装
├── customdriver/         # 既存ドライバーをラップしてログ出力を追加するカスタムドライバー
├── cmd/
│   ├── constdriver/      # constdriver の実行サンプル
│   ├── customdriver/
│   │   ├── mysql/        # customdriver + MySQL の実行サンプル
│   │   └── postgresql/   # customdriver + PostgreSQL の実行サンプル
│   └── implcheck/        # go-sql-driver/mysql, lib/pq のインターフェース実装状況を確認するテスト
├── db/
│   ├── mysql/            # MySQL 用スキーマ・シードデータ・クエリ
│   └── postgresql/       # PostgreSQL 用スキーマ・シードデータ・クエリ
├── sqlc/                 # sqlc の設定と生成コード
└── compose.yaml          # MySQL・PostgreSQL のコンテナ定義
```

## constdriver を動かす

`constdriver` は `database/sql/driver` の必須インターフェースだけを実装した最小限のドライバーです。実際のデータベースに接続せず、固定値 (`1, "Alice"` / `2, "Bob"`) を返します。

```sh
go run ./cmd/constdriver
```

出力:

```
1 Alice
2 Bob
```

## customdriver を動かす

`customdriver` は既存のデータベースドライバーをラップし、SQL の実行ログやトランザクションのログを `slog` で出力するカスタムドライバーです。

### 1. データベースを起動する

```sh
docker compose up -d
```

MySQL (localhost:43306) と PostgreSQL (localhost:55432) が起動します。

### 2. MySQL で実行する

```sh
go run ./cmd/customdriver/mysql
```

### 3. PostgreSQL で実行する

```sh
go run ./cmd/customdriver/postgresql
```

どちらも `users` テーブルから `Alice` を取得し、実行された SQL のログとともに結果を出力します。

### 4. データベースを停止する

```sh
docker compose down
```

## テストを実行する

テストは Docker を使って一時的なコンテナを起動するため、Docker が動いている環境で実行してください。

### インターフェース実装チェック (implcheck)

`go-sql-driver/mysql` と `lib/pq` が `database/sql/driver` の各インターフェースを実装しているかを確認するテストです。

```sh
go test -v ./cmd/implcheck/
```

### customdriver のテスト

MySQL と PostgreSQL の両方に対して CRUD・トランザクションの動作を確認するテストです。

```sh
go test -v ./customdriver/
```

### ベンチマーク

カスタムドライバーのオーバーヘッドを計測するベンチマークです。

```sh
go test -bench . -benchmem ./customdriver/
```
