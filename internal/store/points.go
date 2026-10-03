package store

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"recycle/internal/domain"
)

// ---- settlements ----

func CreateSettlement(ctx context.Context, q Querier, s *domain.Settlement) error {
	return q.QueryRow(ctx,
		`INSERT INTO settlements (order_id, type, amount_cents, points, status, idempotency_key)
		 VALUES ($1,$2,$3,$4,$5,$6) RETURNING id, created_at, updated_at`,
		s.OrderID, s.Type, s.AmountCents, s.Points, s.Status, s.IdempotencyKey).
		Scan(&s.ID, &s.CreatedAt, &s.UpdatedAt)
}

func SettlementByID(ctx context.Context, q Querier, id int64) (*domain.Settlement, error) {
	s := &domain.Settlement{}
	err := q.QueryRow(ctx,
		`SELECT id, order_id, type, amount_cents, points, status, fail_reason, idempotency_key, retry_count, created_at, updated_at
		 FROM settlements WHERE id=$1`, id).
		Scan(&s.ID, &s.OrderID, &s.Type, &s.AmountCents, &s.Points, &s.Status, &s.FailReason,
			&s.IdempotencyKey, &s.RetryCount, &s.CreatedAt, &s.UpdatedAt)
	if err == pgx.ErrNoRows {
		return nil, ErrNotFound
	}
	return s, err
}

func LatestSettlement(ctx context.Context, q Querier, orderID int64) (*domain.Settlement, error) {
	s := &domain.Settlement{}
	err := q.QueryRow(ctx,
		`SELECT id, order_id, type, amount_cents, points, status, fail_reason, idempotency_key, retry_count, created_at, updated_at
		 FROM settlements WHERE order_id=$1 ORDER BY id DESC LIMIT 1`, orderID).
		Scan(&s.ID, &s.OrderID, &s.Type, &s.AmountCents, &s.Points, &s.Status, &s.FailReason,
			&s.IdempotencyKey, &s.RetryCount, &s.CreatedAt, &s.UpdatedAt)
	if err == pgx.ErrNoRows {
		return nil, ErrNotFound
	}
	return s, err
}

func ListSettlements(ctx context.Context, q Querier, orderID int64) ([]*domain.Settlement, error) {
	rows, err := q.Query(ctx,
		`SELECT id, order_id, type, amount_cents, points, status, fail_reason, idempotency_key, retry_count, created_at, updated_at
		 FROM settlements WHERE order_id=$1 ORDER BY id`, orderID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*domain.Settlement
	for rows.Next() {
		s := &domain.Settlement{}
		if err := rows.Scan(&s.ID, &s.OrderID, &s.Type, &s.AmountCents, &s.Points, &s.Status, &s.FailReason,
			&s.IdempotencyKey, &s.RetryCount, &s.CreatedAt, &s.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

func MarkSettlementSuccess(ctx context.Context, q Querier, id int64) error {
	_, err := q.Exec(ctx,
		`UPDATE settlements SET status='SUCCESS', updated_at=now() WHERE id=$1 AND status IN ('PENDING','FAILED')`, id)
	return err
}

func MarkSettlementFailed(ctx context.Context, q Querier, id int64, reason string) error {
	_, err := q.Exec(ctx,
		`UPDATE settlements SET status='FAILED', fail_reason=$2, retry_count=retry_count+1, updated_at=now()
		 WHERE id=$1 AND status='PENDING'`, id, reason)
	return err
}

// ---- points ledger ----

// CreditPoints 幂等入账: idempotency_key 唯一约束保证重放安全
// 返回 (entry, created, err); 已存在同键流水时返回已有记录且 created=false
func CreditPoints(ctx context.Context, q Querier, residentID int64, orderID, settlementID *int64,
	delta int64, kind, idemKey string) (*domain.PointsEntry, bool, error) {
	return CreditPointsWithSource(ctx, q, residentID, orderID, settlementID, delta, kind, idemKey,
		domain.PointsSourceNormal, "", "")
}

// CreditPointsWithSource 幂等入账并记录补发来源/原因/原订单编号
func CreditPointsWithSource(ctx context.Context, q Querier, residentID int64, orderID, settlementID *int64,
	delta int64, kind, idemKey, source, reissueReason, originOrderNo string) (*domain.PointsEntry, bool, error) {

	// 先查幂等键, 已入账则直接返回
	if e, err := PointsEntryByKey(ctx, q, idemKey); err == nil {
		return e, false, nil
	} else if err != ErrNotFound {
		return nil, false, err
	}

	// 锁账户行并累加余额
	var balance int64
	_, err := q.Exec(ctx,
		`INSERT INTO points_accounts (resident_id, balance) VALUES ($1, 0) ON CONFLICT (resident_id) DO NOTHING`,
		residentID)
	if err != nil {
		return nil, false, err
	}
	if err := q.QueryRow(ctx,
		`UPDATE points_accounts SET balance=balance+$2, version=version+1, updated_at=now()
		 WHERE resident_id=$1 RETURNING balance`, residentID, delta).Scan(&balance); err != nil {
		return nil, false, err
	}

	e := &domain.PointsEntry{}
	err = q.QueryRow(ctx,
		`INSERT INTO points_entries
		 (resident_id, order_id, settlement_id, delta, balance_after, kind, idempotency_key, source, reissue_reason, origin_order_no)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING id, created_at`,
		residentID, orderID, settlementID, delta, balance, kind, idemKey,
		source, reissueReason, originOrderNo).
		Scan(&e.ID, &e.CreatedAt)
	if err != nil {
		if IsUniqueViolation(err) {
			// 并发重放: 回退余额由事务回滚保证; 直接读已有记录
			if e2, gerr := PointsEntryByKey(ctx, q, idemKey); gerr == nil {
				return e2, false, nil
			}
		}
		return nil, false, err
	}
	e.ResidentID = residentID
	e.OrderID = orderID
	e.SettlementID = settlementID
	e.Delta = delta
	e.BalanceAfter = balance
	e.Kind = kind
	e.IdempotencyKey = idemKey
	e.Source = source
	e.ReissueReason = reissueReason
	e.OriginOrderNo = originOrderNo
	return e, true, nil
}

const pointsEntryCols = `id, resident_id, order_id, settlement_id, delta, balance_after, kind,
	idempotency_key, source, reissue_reason, origin_order_no, created_at`

func scanPointsEntry(row pgx.Row) (*domain.PointsEntry, error) {
	e := &domain.PointsEntry{}
	err := row.Scan(&e.ID, &e.ResidentID, &e.OrderID, &e.SettlementID, &e.Delta, &e.BalanceAfter,
		&e.Kind, &e.IdempotencyKey, &e.Source, &e.ReissueReason, &e.OriginOrderNo, &e.CreatedAt)
	if err == pgx.ErrNoRows {
		return nil, ErrNotFound
	}
	return e, err
}

func PointsEntryByKey(ctx context.Context, q Querier, key string) (*domain.PointsEntry, error) {
	return scanPointsEntry(q.QueryRow(ctx,
		`SELECT `+pointsEntryCols+` FROM points_entries WHERE idempotency_key=$1`, key))
}

func PointsBalance(ctx context.Context, q Querier, residentID int64) (int64, error) {
	var bal int64
	err := q.QueryRow(ctx, `SELECT balance FROM points_accounts WHERE resident_id=$1`, residentID).Scan(&bal)
	if err == pgx.ErrNoRows {
		return 0, nil
	}
	return bal, err
}

func ListPointsEntries(ctx context.Context, q Querier, residentID int64, limit int) ([]*domain.PointsEntry, error) {
	rows, err := q.Query(ctx,
		`SELECT `+pointsEntryCols+` FROM points_entries WHERE resident_id=$1 ORDER BY id DESC LIMIT $2`,
		residentID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*domain.PointsEntry
	for rows.Next() {
		e, err := scanPointsEntry(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// ---- points claims (积分不到账申诉) ----

func CreatePointsClaim(ctx context.Context, q Querier, residentID, orderID int64, note string) (int64, error) {
	var id int64
	err := q.QueryRow(ctx,
		`INSERT INTO points_claims (resident_id, order_id, note, status) VALUES ($1,$2,$3,'OPEN') RETURNING id`,
		residentID, orderID, note).Scan(&id)
	return id, err
}

func ResolvePointsClaim(ctx context.Context, q Querier, id int64, status, note string) error {
	_, err := q.Exec(ctx,
		`UPDATE points_claims SET status=$2, note=$3, resolved_at=now() WHERE id=$1`, id, status, note)
	return err
}

func ListPointsClaims(ctx context.Context, q Querier, residentID int64) ([]map[string]any, error) {
	rows, err := q.Query(ctx,
		`SELECT id, resident_id, order_id, status, note, created_at, resolved_at
		 FROM points_claims WHERE resident_id=$1 ORDER BY id DESC`, residentID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []map[string]any
	for rows.Next() {
		var id, rid, oid int64
		var status string
		var note *string
		var createdAt, resolvedAt *time.Time
		if err := rows.Scan(&id, &rid, &oid, &status, &note, &createdAt, &resolvedAt); err != nil {
			return nil, err
		}
		out = append(out, map[string]any{
			"id": id, "resident_id": rid, "order_id": oid, "status": status,
			"note": strVal(note), "created_at": createdAt, "resolved_at": resolvedAt,
		})
	}
	return out, rows.Err()
}

func strVal(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
