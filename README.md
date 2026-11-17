# pgrelay

PostgreSQL 트랜잭셔널 아웃박스(Transactional Outbox) 및 웹훅 전달 엔진입니다. 비즈니스 쿼리와 웹훅 발송 이벤트를 단일 DB 트랜잭션에 원자적으로 기록해, DB 커밋 후 웹훅 호출이 유실되거나 DB 롤백 후 유령 웹훅이 발송되는 이중 쓰기(Dual-Write) 문제를 해결합니다. Kafka나 Debezium 같은 별도 메시지 브로커 없이 Go 라이브러리(`pkg/relay`) 임베딩 또는 독립 데몬(`cmd/pgrelay`)으로 동작합니다.

## 스키마 (`migrations/001_outbox_schema.sql`)

```sql
CREATE TABLE IF NOT EXISTS outbox_events (
    id VARCHAR(64) PRIMARY KEY,
    topic VARCHAR(255) NOT NULL,
    endpoint TEXT NOT NULL,
    payload JSONB NOT NULL,
    headers JSONB NOT NULL DEFAULT '{}'::jsonb,
    status VARCHAR(32) NOT NULL DEFAULT 'pending',
    attempts INT NOT NULL DEFAULT 0,
    max_attempts INT NOT NULL DEFAULT 5,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    locked_at TIMESTAMPTZ,
    locked_by VARCHAR(128),
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    delivered_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_outbox_pending
ON outbox_events (next_attempt_at, id)
WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS idx_outbox_processing
ON outbox_events (locked_at)
WHERE status = 'processing';
```

`status = 'pending'` 및 `status = 'processing'` 조건에 부분 인덱스(Partial Index)를 걸어 전체 테이블이 커져도 배치 선점과 고아 락 회수 조회 속도가 유지되도록 했습니다.

## 배치 선점 및 전달 흐름

```mermaid
flowchart LR
    Tx["App Business Tx\n(EnqueueTx)"] --> DB[("PostgreSQL\noutbox_events")]
    DB -->|"SKIP LOCKED"| Workers["Worker Pool &\nStale Lock Reaper"]
    Workers --> Signer["HMAC-SHA256 &\nFull Jitter Backoff"]
    Signer --> Dest["Partner Webhook\nor Dead Letter"]
```

### `FOR UPDATE SKIP LOCKED` 배치 선점 (`pkg/relay/pgstore.go`)
여러 워커가 동시에 큐를 폴링할 때 잠금 경합이 생기지 않도록 `SKIP LOCKED`로 이미 선점된 행을 건너뛰고 배치를 가져옵니다.

```sql
WITH candidates AS (
    SELECT id FROM outbox_events
    WHERE status = 'pending' AND next_attempt_at <= $1
    ORDER BY next_attempt_at ASC, id ASC
    LIMIT $2
    FOR UPDATE SKIP LOCKED
)
UPDATE outbox_events e
SET status = 'processing', locked_at = $3, locked_by = $4, attempts = e.attempts + 1
FROM candidates c
WHERE e.id = c.id
RETURNING e.id, e.topic, e.endpoint, ...;
```

### `LISTEN/NOTIFY` 신호 전파와 고아 락 회수 (`notifier.go`, `engine.go`)
`LISTEN/NOTIFY`로 새 이벤트 적재 신호를 즉시 받아 폴링 지연을 줄이고, 알림 유실에 대비해 폴백 폴링 티커를 함께 둡니다. 워커가 `processing` 상태에서 `OOMKilled` 등으로 비정상 종료되면 백그라운드 Reaper가 `locked_at < NOW() - lease_timeout` 조건의 행을 찾아 `pending`으로 되돌립니다.

### HMAC-SHA256 서명 및 Full Jitter 백오프 (`signer.go`, `backoff.go`)
발송 요청마다 페이로드와 유닉스 타임스탬프(`X-PGRelay-Timestamp`)를 결합한 `X-PGRelay-Signature`(`v1=<hex>`) 헤더를 첨부해 변조와 리플레이 공격(기본 5분 허용 오차)을 방어합니다. 실패 시 `cenkalti/backoff/v4`의 `ExponentialBackOff`로 재시도 간격을 넓히며, `max_attempts`(기본 5회)를 넘기면 `dead`(DLQ) 상태로 격리합니다.

### 라이브러리 임베딩 예시 (`pkg/relay`)

```go
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"time"

	"github.com/seungwoo7050/pgrelay/pkg/relay"
)

func main() {
	db, _ := sql.Open("postgres", "postgres://user:pass@localhost:5432/mydb?sslmode=disable")
	defer db.Close()

	store := relay.NewPGStore(db)
	dispatcher := relay.NewHTTPDispatcher(nil, relay.NewSigner([]byte("webhook-secret")))

	engine, err := relay.NewEngine(relay.EngineConfig{
		Store:        store,
		Dispatcher:   dispatcher,
		WorkerCount:  4,
		BatchSize:    16,
		PollInterval: 500 * time.Millisecond,
		LeaseTimeout: 30 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	tx, _ := db.BeginTx(ctx, nil)
	_, _ = tx.ExecContext(ctx, "UPDATE orders SET status = 'paid' WHERE id = $1", "ord_123")

	payload, _ := json.Marshal(map[string]any{"order_id": "ord_123", "amount": 50000})
	_ = relay.EnqueueTx(ctx, tx, relay.Event{
		Topic:    "order.paid",
		Endpoint: "https://api.partner.com/webhook",
		Payload:  payload,
	})
	_ = tx.Commit()

	go engine.Start(ctx)
	defer engine.Stop()
}
```

## 데몬 실행 옵션

```sh
make build

./bin/pgrelay \
  -postgres-dsn "postgres://user:pass@localhost:5432/mydb?sslmode=disable" \
  -listen :8080 \
  -signing-secret "my-secret-key" \
  -workers 8 \
  -batch-size 32 \
  -poll-interval 500ms \
  -lease-timeout 30s \
  -reap-interval 10s
```

- `-postgres-dsn`을 생략하거나 `-demo`를 지정하면 인메모리 스토어 데모 모드로 기동합니다.
- 상태 확인 및 이벤트 API: `GET /healthz`, `POST /v1/events`, `GET /v1/events`
- Prometheus 메트릭(`GET /metrics`): `pgrelay_enqueued_total`, `pgrelay_delivered_total`, `pgrelay_retried_total`, `pgrelay_dead_total`, `pgrelay_reclaimed_total`

## 동시성 테스트

외부 PostgreSQL 없이 인메모리 스토어(`pkg/relay/memstore.go`)로 트랜잭션 롤백 취소, 다중 워커 중복 선점 방지, 고아 락 회수, DLQ 격리, HMAC 서명 및 리플레이 창 검증을 테스트할 수 있습니다.

```sh
go vet ./...
go test -race ./...
```

전달 모델은 At-least-once이므로 수신 서버는 `X-PGRelay-Event-ID` 헤더 기준으로 멱등 처리를 해야 하며, 실패 재시도 건은 백오프로 지연되므로 동일 토픽 내 이벤트 도착 순서가 생성 순서와 달라질 수 있습니다.
