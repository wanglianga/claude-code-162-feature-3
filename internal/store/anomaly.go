package store

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5"
	"recycle/internal/domain"
)

// ---- 回收员待处理任务 ----

// CreateCollectorTaskIfAbsent 幂等创建任务, 返回 (task, created)
func CreateCollectorTaskIfAbsent(ctx context.Context, q Querier, t *domain.CollectorTask) (*domain.CollectorTask, bool, error) {
	detail, _ := json.Marshal(t.Detail)
	err := q.QueryRow(ctx,
		`INSERT INTO collector_tasks
		 (task_no, collector_id, order_id, resident_id, kind, status, settlement_impact, detail)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
		 ON CONFLICT (collector_id, order_id, kind) DO NOTHING
		 RETURNING id, created_at`,
		t.TaskNo, t.CollectorID, t.OrderID, t.ResidentID, t.Kind,
		t.Status, t.SettlementImpact, detail).Scan(&t.ID, &t.CreatedAt)
	if err == pgx.ErrNoRows {
		existing, gerr := CollectorTaskByOrder(ctx, q, t.CollectorID, t.OrderID, t.Kind)
		if gerr != nil {
			return nil, false, gerr
		}
		return existing, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return t, true, nil
}

const collectorTaskCols = `id, task_no, collector_id, order_id, resident_id, kind, status,
	settlement_impact, detail, created_at, submitted_at, resolved_at, resolution`

func scanCollectorTask(row pgx.Row) (*domain.CollectorTask, error) {
	t := &domain.CollectorTask{}
	var detail []byte
	var submittedAt, resolvedAt *time.Time
	err := row.Scan(&t.ID, &t.TaskNo, &t.CollectorID, &t.OrderID, &t.ResidentID, &t.Kind,
		&t.Status, &t.SettlementImpact, &detail, &t.CreatedAt, &submittedAt, &resolvedAt, &t.Resolution)
	if err == pgx.ErrNoRows {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	t.SubmittedAt, t.ResolvedAt = submittedAt, resolvedAt
	t.Detail = map[string]any{}
	_ = json.Unmarshal(detail, &t.Detail)
	return t, nil
}

func CollectorTaskByID(ctx context.Context, q Querier, id int64) (*domain.CollectorTask, error) {
	return scanCollectorTask(q.QueryRow(ctx,
		`SELECT `+collectorTaskCols+` FROM collector_tasks WHERE id=$1`, id))
}

func CollectorTaskByOrder(ctx context.Context, q Querier, collectorID, orderID int64, kind string) (*domain.CollectorTask, error) {
	return scanCollectorTask(q.QueryRow(ctx,
		`SELECT `+collectorTaskCols+` FROM collector_tasks
		 WHERE collector_id=$1 AND order_id=$2 AND kind=$3`, collectorID, orderID, kind))
}

// OpenCollectorTaskByOrder 查询订单上未结案的任务(任意状态 != RESOLVED/CANCELLED)
func OpenCollectorTaskByOrder(ctx context.Context, q Querier, orderID int64) (*domain.CollectorTask, error) {
	return scanCollectorTask(q.QueryRow(ctx,
		`SELECT `+collectorTaskCols+` FROM collector_tasks
		 WHERE order_id=$1 AND status IN ('OPEN','SUBMITTED') ORDER BY id DESC LIMIT 1`, orderID))
}

func ListCollectorTasks(ctx context.Context, q Querier, collectorID int64, status string) ([]*domain.CollectorTask, error) {
	sql := `SELECT ` + collectorTaskCols + ` FROM collector_tasks`
	args := []any{}
	switch {
	case collectorID != 0 && status != "":
		sql += ` WHERE collector_id=$1 AND status=$2`
		args = append(args, collectorID, status)
	case collectorID != 0:
		sql += ` WHERE collector_id=$1`
		args = append(args, collectorID)
	case status != "":
		sql += ` WHERE status=$1`
		args = append(args, status)
	}
	sql += ` ORDER BY id DESC LIMIT 200`
	rows, err := q.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*domain.CollectorTask
	for rows.Next() {
		t, err := scanCollectorTask(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// MarkCollectorTaskSubmitted 回收员补提交: 任务转入 SUBMITTED/HELD(收益冻结待运营核对)
func MarkCollectorTaskSubmitted(ctx context.Context, q Querier, id int64) error {
	_, err := q.Exec(ctx,
		`UPDATE collector_tasks SET status='SUBMITTED', settlement_impact='HELD', submitted_at=now()
		 WHERE id=$1 AND status='OPEN'`, id)
	return err
}

func ResolveCollectorTask(ctx context.Context, q Querier, id int64, resolution string, impact string) error {
	if impact == "" {
		impact = domain.TaskImpactReleased
	}
	_, err := q.Exec(ctx,
		`UPDATE collector_tasks SET status='RESOLVED', settlement_impact=$2, resolution=$3, resolved_at=now()
		 WHERE id=$1 AND status IN ('OPEN','SUBMITTED')`, id, impact, resolution)
	return err
}

// ---- 消息回调失败 ----

// ResidentExists 该用户是否为居民档案(用于过滤非居民的失败通知)
func ResidentExists(ctx context.Context, q Querier, residentID int64) (bool, error) {
	var ok bool
	err := q.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM residents WHERE user_id=$1)`, residentID).Scan(&ok)
	return ok, err
}

func RecordCallbackFailure(ctx context.Context, q Querier,
	topic, consumer, msgKey string, attempt int, residentID, orderID int64, reason string, payload map[string]any) (int64, error) {
	b, _ := json.Marshal(payload)
	var id int64
	err := q.QueryRow(ctx,
		`INSERT INTO callback_failures (topic, consumer, msg_key, attempt, resident_id, order_id, reason, payload)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING id`,
		topic, consumer, msgKey, attempt, residentID, orderID, reason, b).Scan(&id)
	return id, err
}

// CountCallbackFailures 统计时间窗内某消费者的失败次数
func CountCallbackFailures(ctx context.Context, q Querier, consumer string, window time.Duration) (int, error) {
	var n int
	err := q.QueryRow(ctx,
		`SELECT count(*) FROM callback_failures
		 WHERE consumer=$1 AND created_at > now() - make_interval(secs => $2)`,
		consumer, window.Seconds()).Scan(&n)
	return n, err
}

func HasOpenIncident(ctx context.Context, q Querier, consumer string) (bool, error) {
	var ok bool
	err := q.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM tech_incidents WHERE consumer=$1 AND status='OPEN')`, consumer).Scan(&ok)
	return ok, err
}

// OpenTechIncidentByConsumer 取消费者当前未关闭的排查记录(无则 ErrNotFound)
func OpenTechIncidentByConsumer(ctx context.Context, q Querier, consumer string) (*domain.TechIncident, error) {
	return scanTechIncident(q.QueryRow(ctx,
		`SELECT `+techIncidentCols+` FROM tech_incidents WHERE consumer=$1 AND status='OPEN' ORDER BY id DESC LIMIT 1`,
		consumer))
}

// BumpIncidentFailures 排查记录持续累加失败次数, 并刷新受影响居民数(去重)
func BumpIncidentFailures(ctx context.Context, q Querier, incidentID int64, failCount int) error {
	_, err := q.Exec(ctx,
		`UPDATE tech_incidents
		 SET fail_count=$2,
		     affected_resident_count=(
		       SELECT count(DISTINCT resident_id) FROM incident_affected_residents WHERE incident_id=$1)
		 WHERE id=$1`, incidentID, failCount)
	return err
}

type FailureResidentRow struct {
	ResidentID int64
	OrderID    int64
	FailCount  int
	LastFailAt time.Time
}

// ListFailureResidents 时间窗内失败影响的居民(按居民+订单聚合; 仅统计真实居民账号)
func ListFailureResidents(ctx context.Context, q Querier, consumer string, window time.Duration) ([]FailureResidentRow, error) {
	rows, err := q.Query(ctx,
		`SELECT cf.resident_id, cf.order_id, count(*) AS cnt, max(cf.created_at)
		 FROM callback_failures cf
		 JOIN residents res ON res.user_id = cf.resident_id
		 WHERE cf.consumer=$1 AND cf.created_at > now() - make_interval(secs => $2)
		 GROUP BY cf.resident_id, cf.order_id ORDER BY cnt DESC, cf.resident_id`,
		consumer, window.Seconds())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []FailureResidentRow
	for rows.Next() {
		var r FailureResidentRow
		if err := rows.Scan(&r.ResidentID, &r.OrderID, &r.FailCount, &r.LastFailAt); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func ListCallbackFailures(ctx context.Context, q Querier, consumer string, limit int) ([]*domain.CallbackFailure, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := q.Query(ctx,
		`SELECT id, topic, consumer, msg_key, attempt, resident_id, order_id, reason, payload, created_at
		 FROM callback_failures WHERE ($1='' OR consumer=$1) ORDER BY id DESC LIMIT $2`, consumer, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*domain.CallbackFailure
	for rows.Next() {
		f := &domain.CallbackFailure{}
		var payload []byte
		if err := rows.Scan(&f.ID, &f.Topic, &f.Consumer, &f.MsgKey, &f.Attempt,
			&f.ResidentID, &f.OrderID, &f.Reason, &payload, &f.CreatedAt); err != nil {
			return nil, err
		}
		f.Payload = map[string]any{}
		_ = json.Unmarshal(payload, &f.Payload)
		out = append(out, f)
	}
	return out, rows.Err()
}

// ---- 技术排查记录 ----

func CreateTechIncident(ctx context.Context, q Querier, in *domain.TechIncident) error {
	detail, _ := json.Marshal(in.Detail)
	return q.QueryRow(ctx,
		`INSERT INTO tech_incidents
		 (incident_no, kind, topic, consumer, status, fail_count, window_seconds, threshold, affected_resident_count, detail)
		 VALUES ($1,$2,$3,$4,'OPEN',$5,$6,$7,$8,$9) RETURNING id, created_at`,
		in.IncidentNo, in.Kind, in.Topic, in.Consumer, in.FailCount, in.WindowSeconds,
		in.Threshold, in.AffectedResidentCount, detail).Scan(&in.ID, &in.CreatedAt)
}

const techIncidentCols = `id, incident_no, kind, topic, consumer, status, fail_count, window_seconds,
	threshold, affected_resident_count, detail, created_at, resolved_at, resolution`

func scanTechIncident(row pgx.Row) (*domain.TechIncident, error) {
	in := &domain.TechIncident{}
	var detail []byte
	var resolvedAt *time.Time
	err := row.Scan(&in.ID, &in.IncidentNo, &in.Kind, &in.Topic, &in.Consumer, &in.Status,
		&in.FailCount, &in.WindowSeconds, &in.Threshold, &in.AffectedResidentCount,
		&detail, &in.CreatedAt, &resolvedAt, &in.Resolution)
	if err == pgx.ErrNoRows {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	in.ResolvedAt = resolvedAt
	in.Detail = map[string]any{}
	_ = json.Unmarshal(detail, &in.Detail)
	return in, nil
}

func TechIncidentByID(ctx context.Context, q Querier, id int64) (*domain.TechIncident, error) {
	return scanTechIncident(q.QueryRow(ctx,
		`SELECT `+techIncidentCols+` FROM tech_incidents WHERE id=$1`, id))
}

func ListTechIncidents(ctx context.Context, q Querier, status string) ([]*domain.TechIncident, error) {
	rows, err := q.Query(ctx,
		`SELECT `+techIncidentCols+` FROM tech_incidents
		 WHERE ($1='' OR status=$1) ORDER BY id DESC LIMIT 200`, status)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*domain.TechIncident
	for rows.Next() {
		in, err := scanTechIncident(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, in)
	}
	return out, rows.Err()
}

func ResolveTechIncident(ctx context.Context, q Querier, id int64, resolution string) error {
	_, err := q.Exec(ctx,
		`UPDATE tech_incidents SET status='RESOLVED', resolution=$2, resolved_at=now()
		 WHERE id=$1 AND status='OPEN'`, id, resolution)
	return err
}

// ---- 受影响居民名单 ----

func UpsertIncidentResident(ctx context.Context, q Querier,
	incidentID, residentID, orderID int64, failAt time.Time) error {
	if orderID == 0 {
		orderID = 0
	}
	_, err := q.Exec(ctx,
		`INSERT INTO incident_affected_residents (incident_id, resident_id, order_id, fail_count, last_fail_at)
		 VALUES ($1,$2,$3,1,$4)
		 ON CONFLICT (incident_id, resident_id, order_id)
		 DO UPDATE SET fail_count=incident_affected_residents.fail_count+1,
		               last_fail_at=GREATEST(incident_affected_residents.last_fail_at, EXCLUDED.last_fail_at)`,
		incidentID, residentID, orderID, failAt)
	return err
}

type IncidentResidentRow struct {
	ResidentID int64
	Account    string
	Name       string
	Phone      string
	OrderID    int64
	OrderNo    string
	FailCount  int
	LastFailAt time.Time
}

func ListIncidentResidents(ctx context.Context, q Querier, incidentID int64) ([]IncidentResidentRow, error) {
	rows, err := q.Query(ctx,
		`SELECT a.resident_id, u.account, u.name, u.phone, a.order_id,
		        COALESCE(o.order_no,''), a.fail_count, a.last_fail_at
		 FROM incident_affected_residents a
		 JOIN users u ON u.id=a.resident_id
		 LEFT JOIN orders o ON o.id=a.order_id
		 WHERE a.incident_id=$1
		 ORDER BY a.fail_count DESC, a.resident_id, a.order_id`, incidentID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []IncidentResidentRow
	for rows.Next() {
		var r IncidentResidentRow
		if err := rows.Scan(&r.ResidentID, &r.Account, &r.Name, &r.Phone, &r.OrderID,
			&r.OrderNo, &r.FailCount, &r.LastFailAt); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// ---- 异常对账扫描 ----

// DelayedPointsSettlement 系统延迟候选: CONFIRMED 订单存在 PENDING 积分结算单且无对应积分流水
type DelayedPointsSettlement struct {
	OrderID      int64
	SettlementID int64
	ResidentID   int64
	Points       int64
}

func ListDelayedPointsSettlements(ctx context.Context, q Querier, olderThan time.Duration) ([]DelayedPointsSettlement, error) {
	rows, err := q.Query(ctx,
		`SELECT o.id, s.id, o.resident_id, s.points
		 FROM orders o
		 JOIN settlements s ON s.order_id=o.id
		 WHERE o.status='CONFIRMED' AND s.type='points' AND s.status='PENDING'
		   AND s.created_at < now() - make_interval(secs => $1)
		   AND NOT EXISTS (
		       SELECT 1 FROM points_entries pe WHERE pe.idempotency_key = 'settlement:'||s.id)
		 ORDER BY s.id`, olderThan.Seconds())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DelayedPointsSettlement
	for rows.Next() {
		var d DelayedPointsSettlement
		if err := rows.Scan(&d.OrderID, &d.SettlementID, &d.ResidentID, &d.Points); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// MissingSettlementOrder 回收员未提交候选: CONFIRMED 订单无任何结算单
type MissingSettlementOrder struct {
	OrderID     int64
	ResidentID  int64
	CollectorID int64
}

func ListMissingSettlementOrders(ctx context.Context, q Querier, olderThan time.Duration) ([]MissingSettlementOrder, error) {
	rows, err := q.Query(ctx,
		`SELECT o.id, o.resident_id, o.collector_id
		 FROM orders o
		 WHERE o.status='CONFIRMED' AND o.collector_id IS NOT NULL
		   AND o.updated_at < now() - make_interval(secs => $1)
		   AND NOT EXISTS (SELECT 1 FROM settlements s WHERE s.order_id=o.id)
		 ORDER BY o.id`, olderThan.Seconds())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []MissingSettlementOrder
	for rows.Next() {
		var m MissingSettlementOrder
		if err := rows.Scan(&m.OrderID, &m.ResidentID, &m.CollectorID); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// ---- 收益冻结 ----

func SetEarningHeld(ctx context.Context, q Querier, orderID int64, held bool, reason string) error {
	_, err := q.Exec(ctx,
		`UPDATE collector_earnings SET held=$2, held_reason=$3 WHERE order_id=$1`,
		orderID, held, reason)
	return err
}
