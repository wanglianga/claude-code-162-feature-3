package store

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5"
	"recycle/internal/domain"
)

// ---- points reissues (积分补发) ----

func CreatePointsReissue(ctx context.Context, q Querier, r *domain.PointsReissue) error {
	return q.QueryRow(ctx,
		`INSERT INTO points_reissues
		 (resident_id, order_id, settlement_id, points, reason, source, original_order_no, points_entry_id, idempotency_key)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		 RETURNING id, credited_at, created_at`,
		r.ResidentID, r.OrderID, r.SettlementID, r.Points, r.Reason, r.Source,
		r.OriginalOrderNo, r.PointsEntryID, r.IdempotencyKey).
		Scan(&r.ID, &r.CreditedAt, &r.CreatedAt)
}

func PointsReissueBySettlement(ctx context.Context, q Querier, settlementID int64) (*domain.PointsReissue, error) {
	return scanReissue(q.QueryRow(ctx,
		`SELECT `+reissueCols+` FROM points_reissues WHERE settlement_id=$1`, settlementID))
}

const reissueCols = `id, resident_id, order_id, settlement_id, points, reason, source,
	original_order_no, points_entry_id, idempotency_key, credited_at, created_at`

func scanReissue(row pgx.Row) (*domain.PointsReissue, error) {
	r := &domain.PointsReissue{}
	err := row.Scan(&r.ID, &r.ResidentID, &r.OrderID, &r.SettlementID, &r.Points,
		&r.Reason, &r.Source, &r.OriginalOrderNo, &r.PointsEntryID,
		&r.IdempotencyKey, &r.CreditedAt, &r.CreatedAt)
	if err == pgx.ErrNoRows {
		return nil, ErrNotFound
	}
	return r, err
}

func ListPointsReissuesByResident(ctx context.Context, q Querier, residentID int64, limit int) ([]*domain.PointsReissue, error) {
	rows, err := q.Query(ctx,
		`SELECT `+reissueCols+` FROM points_reissues WHERE resident_id=$1 ORDER BY id DESC LIMIT $2`,
		residentID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*domain.PointsReissue
	for rows.Next() {
		r, err := scanReissue(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// ---- collector tasks (回收员待处理任务) ----

const taskCols = `id, collector_id, order_id, kind, status, reason, impact, detail, created_at, resolved_at`

func scanTask(row pgx.Row) (*domain.CollectorTask, error) {
	t := &domain.CollectorTask{}
	var detail []byte
	err := row.Scan(&t.ID, &t.CollectorID, &t.OrderID, &t.Kind, &t.Status,
		&t.Reason, &t.Impact, &detail, &t.CreatedAt, &t.ResolvedAt)
	if err == pgx.ErrNoRows {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	t.Detail = map[string]any{}
	_ = json.Unmarshal(detail, &t.Detail)
	return t, nil
}

// CreateCollectorTaskIfAbsent 仅当不存在同(回收员,订单,类型) OPEN 任务时插入
func CreateCollectorTaskIfAbsent(ctx context.Context, q Querier, t *domain.CollectorTask) (bool, error) {
	detail, _ := json.Marshal(t.Detail)
	err := q.QueryRow(ctx,
		`INSERT INTO collector_tasks (collector_id, order_id, kind, reason, impact, detail)
		 VALUES ($1,$2,$3,$4,$5,$6)
		 ON CONFLICT DO NOTHING
		 RETURNING id, created_at`,
		t.CollectorID, t.OrderID, t.Kind, t.Reason, t.Impact, detail).
		Scan(&t.ID, &t.CreatedAt)
	if err == pgx.ErrNoRows {
		return false, nil // 已有 OPEN 任务
	}
	if err != nil {
		return false, err
	}
	t.Status = domain.TaskStatusOpen
	return true, nil
}

// ResolveCollectorTasks 订单补提交后关闭该单未结任务
func ResolveCollectorTasks(ctx context.Context, q Querier, orderID int64) error {
	_, err := q.Exec(ctx,
		`UPDATE collector_tasks SET status=$2, resolved_at=now()
		 WHERE order_id=$1 AND status=$3`, orderID, domain.TaskStatusResolved, domain.TaskStatusOpen)
	return err
}

func ListCollectorTasks(ctx context.Context, q Querier, collectorID int64, status string) ([]*domain.CollectorTask, error) {
	sql := `SELECT ` + taskCols + ` FROM collector_tasks WHERE collector_id=$1`
	args := []any{collectorID}
	if status != "" {
		sql += ` AND status=$2`
		args = append(args, status)
	}
	sql += ` ORDER BY id DESC`
	rows, err := q.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*domain.CollectorTask
	for rows.Next() {
		t, err := scanTask(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func CountOpenCollectorTasks(ctx context.Context, q Querier, collectorID int64) (int, error) {
	var n int
	err := q.QueryRow(ctx,
		`SELECT count(*) FROM collector_tasks WHERE collector_id=$1 AND status=$2`,
		collectorID, domain.TaskStatusOpen).Scan(&n)
	return n, err
}

// ListConfirmedOrdersWithoutSettlement 已称重确认但回收员从未提交结算的订单(未提交场景)
func ListConfirmedOrdersWithoutSettlement(ctx context.Context, q Querier, olderThan time.Time, limit int) ([]*domain.Order, error) {
	rows, err := q.Query(ctx,
		`SELECT `+orderCols+` FROM orders o
		 WHERE o.status=$1 AND o.updated_at < $2
		   AND NOT EXISTS (SELECT 1 FROM settlements s WHERE s.order_id=o.id)
		 ORDER BY o.id LIMIT $3`,
		domain.StatusConfirmed, olderThan, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanOrders(rows)
}

// ListStalePendingPointSettlements 提交了积分结算但超过宽限期仍 PENDING(系统延迟场景)
func ListStalePendingPointSettlements(ctx context.Context, q Querier, olderThan time.Time, limit int) ([]*domain.Settlement, error) {
	rows, err := q.Query(ctx,
		`SELECT s.id, s.order_id, s.type, s.amount_cents, s.points, s.status, s.fail_reason,
				s.idempotency_key, s.retry_count, s.created_at, s.updated_at
		 FROM settlements s JOIN orders o ON o.id=s.order_id
		 WHERE s.type=$1 AND s.status=$2 AND o.status=$3 AND s.created_at < $4
		 ORDER BY s.id LIMIT $5`,
		domain.SettlePoints, domain.SettlePending, domain.StatusConfirmed, olderThan, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*domain.Settlement
	for rows.Next() {
		s := &domain.Settlement{}
		if err := rows.Scan(&s.ID, &s.OrderID, &s.Type, &s.AmountCents, &s.Points, &s.Status,
			&s.FailReason, &s.IdempotencyKey, &s.RetryCount, &s.CreatedAt, &s.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// ---- callback failures (消息回调失败) ----

func AddCallbackFailure(ctx context.Context, q Querier, f *domain.CallbackFailure) error {
	return q.QueryRow(ctx,
		`INSERT INTO callback_failures (topic, consumer, msg_key, err, resident_id, order_id)
		 VALUES ($1,$2,$3,$4,$5,$6) RETURNING id, created_at`,
		f.Topic, f.Consumer, f.MsgKey, f.Err, f.ResidentID, f.OrderID).
		Scan(&f.ID, &f.CreatedAt)
}

func CountCallbackFailuresSince(ctx context.Context, q Querier, topic, consumer string, since time.Time) (int, error) {
	var n int
	err := q.QueryRow(ctx,
		`SELECT count(*) FROM callback_failures WHERE topic=$1 AND consumer=$2 AND created_at>=$3`,
		topic, consumer, since).Scan(&n)
	return n, err
}

func LatestCallbackFailure(ctx context.Context, q Querier, topic, consumer string) (*domain.CallbackFailure, error) {
	f := &domain.CallbackFailure{}
	err := q.QueryRow(ctx,
		`SELECT id, topic, consumer, msg_key, err, resident_id, order_id, created_at
		 FROM callback_failures WHERE topic=$1 AND consumer=$2 ORDER BY id DESC LIMIT 1`,
		topic, consumer).
		Scan(&f.ID, &f.Topic, &f.Consumer, &f.MsgKey, &f.Err, &f.ResidentID, &f.OrderID, &f.CreatedAt)
	if err == pgx.ErrNoRows {
		return nil, ErrNotFound
	}
	return f, err
}

// ---- tech incidents (技术排查记录) ----

const incidentCols = `id, kind, topic, consumer, status, fail_count, window_secs, threshold,
	summary, first_fail_at, last_fail_at, resolved_at, created_at`

func scanIncident(row pgx.Row) (*domain.TechIncident, error) {
	t := &domain.TechIncident{}
	err := row.Scan(&t.ID, &t.Kind, &t.Topic, &t.Consumer, &t.Status, &t.FailCount,
		&t.WindowSecs, &t.Threshold, &t.Summary, &t.FirstFailAt, &t.LastFailAt,
		&t.ResolvedAt, &t.CreatedAt)
	if err == pgx.ErrNoRows {
		return nil, ErrNotFound
	}
	return t, err
}

// OpenTechIncident 打开一条技术排查记录(同 topic+consumer 仅一条 OPEN), 返回是否新建
func OpenTechIncident(ctx context.Context, q Querier, topic, consumer string,
	failCount, windowSecs, threshold int, firstFail, lastFail time.Time, summary string) (*domain.TechIncident, bool, error) {

	t := &domain.TechIncident{}
	err := q.QueryRow(ctx,
		`INSERT INTO tech_incidents
		 (kind, topic, consumer, status, fail_count, window_secs, threshold, summary, first_fail_at, last_fail_at)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		 ON CONFLICT DO NOTHING
		 RETURNING `+incidentCols,
		domain.IncidentKindCallbackFailure, topic, consumer, domain.IncidentStatusOpen,
		failCount, windowSecs, threshold, summary, firstFail, lastFail).
		Scan(&t.ID, &t.Kind, &t.Topic, &t.Consumer, &t.Status, &t.FailCount,
			&t.WindowSecs, &t.Threshold, &t.Summary, &t.FirstFailAt, &t.LastFailAt,
			&t.ResolvedAt, &t.CreatedAt)
	if err == pgx.ErrNoRows {
		existing, gerr := GetOpenTechIncident(ctx, q, topic, consumer)
		return existing, false, gerr
	}
	if err != nil {
		return nil, false, err
	}
	return t, true, nil
}

// UpdateTechIncidentCounts 刷新进行中排查记录的失败计数与时间窗
func UpdateTechIncidentCounts(ctx context.Context, q Querier, id int64, failCount, windowSecs int, lastFail time.Time) error {
	_, err := q.Exec(ctx,
		`UPDATE tech_incidents SET fail_count=$2, window_secs=$3, last_fail_at=$4 WHERE id=$1`,
		id, failCount, windowSecs, lastFail)
	return err
}

func GetOpenTechIncident(ctx context.Context, q Querier, topic, consumer string) (*domain.TechIncident, error) {
	return scanIncident(q.QueryRow(ctx,
		`SELECT `+incidentCols+` FROM tech_incidents WHERE topic=$1 AND consumer=$2 AND status=$3`,
		topic, consumer, domain.IncidentStatusOpen))
}

func ResolveOpenTechIncident(ctx context.Context, q Querier, topic, consumer, summary string) (bool, error) {
	tag, err := q.Exec(ctx,
		`UPDATE tech_incidents SET status=$2, resolved_at=now(), summary=$4
		 WHERE topic=$1 AND consumer=$3 AND status=$5`,
		topic, domain.IncidentStatusResolved, consumer, summary, domain.IncidentStatusOpen)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

func ListTechIncidents(ctx context.Context, q Querier, status string) ([]*domain.TechIncident, error) {
	sql := `SELECT ` + incidentCols + ` FROM tech_incidents`
	args := []any{}
	if status != "" {
		sql += ` WHERE status=$1`
		args = append(args, status)
	}
	sql += ` ORDER BY id DESC`
	rows, err := q.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*domain.TechIncident
	for rows.Next() {
		t, err := scanIncident(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// AffectedResident 回调失败受影响居民(运营名单)
type AffectedResident struct {
	ResidentID     int64
	Account        string
	Name           string
	Phone          string
	FailCount      int
	OrdersAffected int
	OrderNos       string
	ReissuedCount  int
	LastFailAt     time.Time
}

// ListAffectedResidents 汇总指定回调通道在某时间点之后受影响的居民
func ListAffectedResidents(ctx context.Context, q Querier, topic, consumer string, since time.Time) ([]*AffectedResident, error) {
	rows, err := q.Query(ctx,
		`SELECT f.resident_id, u.account, u.name, u.phone,
		        count(*) AS fails,
		        count(DISTINCT f.order_id) AS orders_affected,
		        COALESCE(string_agg(DISTINCT o.order_no, ', '), '') AS order_nos,
		        (SELECT count(*) FROM points_reissues r
		          WHERE r.resident_id=f.resident_id AND r.created_at >= $3) AS reissued_count,
		        max(f.created_at)
		 FROM callback_failures f
		 JOIN users u ON u.id=f.resident_id
		 LEFT JOIN orders o ON o.id=f.order_id
		 WHERE f.topic=$1 AND f.consumer=$2 AND f.resident_id IS NOT NULL AND f.created_at >= $3
		 GROUP BY f.resident_id, u.account, u.name, u.phone
		 ORDER BY fails DESC, f.resident_id`,
		topic, consumer, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*AffectedResident
	for rows.Next() {
		a := &AffectedResident{}
		if err := rows.Scan(&a.ResidentID, &a.Account, &a.Name, &a.Phone,
			&a.FailCount, &a.OrdersAffected, &a.OrderNos, &a.ReissuedCount, &a.LastFailAt); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}
