package store

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5"
	"recycle/internal/domain"
)

// ---- outbox (事务性发件箱) ----

type OutboxMessage struct {
	ID      int64
	Topic   string
	Key     string
	Payload []byte
}

// AddOutbox 与业务写入同事务
func AddOutbox(ctx context.Context, q Querier, topic, key string, payload any) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = q.Exec(ctx,
		`INSERT INTO outbox (topic, key, payload) VALUES ($1,$2,$3)`, topic, key, b)
	return err
}

func FetchUnpublished(ctx context.Context, q Querier, limit int) ([]OutboxMessage, error) {
	rows, err := q.Query(ctx,
		`SELECT id, topic, key, payload FROM outbox WHERE published_at IS NULL ORDER BY id LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []OutboxMessage
	for rows.Next() {
		var m OutboxMessage
		if err := rows.Scan(&m.ID, &m.Topic, &m.Key, &m.Payload); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

func MarkPublished(ctx context.Context, q Querier, id int64) error {
	_, err := q.Exec(ctx, `UPDATE outbox SET published_at=now() WHERE id=$1`, id)
	return err
}

// ---- processed_events (消费者幂等) ----

// TryMarkProcessed 返回 true 表示首次处理
func TryMarkProcessed(ctx context.Context, q Querier, consumer, eventKey string) (bool, error) {
	tag, err := q.Exec(ctx,
		`INSERT INTO processed_events (consumer, event_key) VALUES ($1,$2) ON CONFLICT DO NOTHING`,
		consumer, eventKey)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// ---- schedules ----

func UpsertSchedule(ctx context.Context, q Querier, collectorID int64, date string, startHour, endHour, capacity int) error {
	_, err := q.Exec(ctx,
		`INSERT INTO schedules (collector_id, work_date, start_hour, end_hour, capacity)
		 VALUES ($1,$2,$3,$4,$5)
		 ON CONFLICT (collector_id, work_date, start_hour)
		 DO UPDATE SET end_hour=EXCLUDED.end_hour, capacity=EXCLUDED.capacity`,
		collectorID, date, startHour, endHour, capacity)
	return err
}

// CollectorScheduled 回收员在指定时间是否有排班且容量未满
func CollectorScheduled(ctx context.Context, q Querier, collectorID int64, at time.Time) (bool, error) {
	var ok bool
	err := q.QueryRow(ctx,
		`SELECT EXISTS(
			SELECT 1 FROM schedules s
			WHERE s.collector_id=$1 AND s.work_date=$2::date
			  AND s.start_hour <= $3 AND s.end_hour > $3
			  AND (SELECT count(*) FROM orders o
			       WHERE o.collector_id=$1
			         AND o.time_window_start::date = s.work_date
			         AND o.status IN ('ASSIGNED','ACCEPTED','EN_ROUTE','ARRIVED','RESIDENT_VERIFIED','WEIGHED','DISPUTED','CONFIRMED','SETTLED','DELIVERED','INBOUND_CONFIRMED','COMPLETED')
			         AND EXTRACT(hour FROM o.time_window_start) >= s.start_hour
			         AND EXTRACT(hour FROM o.time_window_start) < s.end_hour) < s.capacity)`,
		collectorID, at.Format("2006-01-02"), at.Hour()).Scan(&ok)
	return ok, err
}

func SetCollectorDuty(ctx context.Context, q Querier, collectorID int64, onDuty bool, lat, lng float64) error {
	_, err := q.Exec(ctx,
		`UPDATE collectors SET on_duty=$2, lat=$3, lng=$4 WHERE user_id=$1`,
		collectorID, onDuty, lat, lng)
	return err
}

// ---- inbound / certificates / earnings ----

func CreateInboundRecord(ctx context.Context, q Querier, r *domain.InboundRecord) error {
	return q.QueryRow(ctx,
		`INSERT INTO inbound_records (order_id, station_id, operator_id, status, variance_g)
		 VALUES ($1,$2,$3,$4,$5) RETURNING id, created_at`,
		r.OrderID, r.StationID, r.OperatorID, r.Status, r.VarianceG).
		Scan(&r.ID, &r.CreatedAt)
}

func InboundByOrder(ctx context.Context, q Querier, orderID int64) (*domain.InboundRecord, error) {
	r := &domain.InboundRecord{}
	err := q.QueryRow(ctx,
		`SELECT id, order_id, station_id, operator_id, status, variance_g, created_at
		 FROM inbound_records WHERE order_id=$1`, orderID).
		Scan(&r.ID, &r.OrderID, &r.StationID, &r.OperatorID, &r.Status, &r.VarianceG, &r.CreatedAt)
	if err == pgx.ErrNoRows {
		return nil, ErrNotFound
	}
	return r, err
}

func CreateCertificate(ctx context.Context, q Querier, c *domain.Certificate) error {
	return q.QueryRow(ctx,
		`INSERT INTO certificates (cert_no, order_id, resident_id, category, weight_g, co2_saved_g)
		 VALUES ($1,$2,$3,$4,$5,$6) RETURNING id, issued_at`,
		c.CertNo, c.OrderID, c.ResidentID, c.Category, c.WeightG, c.CO2SavedG).
		Scan(&c.ID, &c.IssuedAt)
}

func ListCertificates(ctx context.Context, q Querier, orderID int64) ([]*domain.Certificate, error) {
	rows, err := q.Query(ctx,
		`SELECT id, cert_no, order_id, resident_id, category, weight_g, co2_saved_g, issued_at
		 FROM certificates WHERE order_id=$1 ORDER BY id`, orderID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*domain.Certificate
	for rows.Next() {
		c := &domain.Certificate{}
		if err := rows.Scan(&c.ID, &c.CertNo, &c.OrderID, &c.ResidentID, &c.Category,
			&c.WeightG, &c.CO2SavedG, &c.IssuedAt); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func ListCertificatesByResident(ctx context.Context, q Querier, residentID int64) ([]*domain.Certificate, error) {
	rows, err := q.Query(ctx,
		`SELECT id, cert_no, order_id, resident_id, category, weight_g, co2_saved_g, issued_at
		 FROM certificates WHERE resident_id=$1 ORDER BY id DESC`, residentID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*domain.Certificate
	for rows.Next() {
		c := &domain.Certificate{}
		if err := rows.Scan(&c.ID, &c.CertNo, &c.OrderID, &c.ResidentID, &c.Category,
			&c.WeightG, &c.CO2SavedG, &c.IssuedAt); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func CreateEarning(ctx context.Context, q Querier, e *domain.CollectorEarning) error {
	return q.QueryRow(ctx,
		`INSERT INTO collector_earnings (collector_id, order_id, base_cents, weight_cents, floor_cents, distance_cents, penalty_cents, total_cents)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
		 ON CONFLICT (order_id) DO UPDATE SET penalty_cents=EXCLUDED.penalty_cents, total_cents=EXCLUDED.total_cents
		 RETURNING id, created_at`,
		e.CollectorID, e.OrderID, e.BaseCents, e.WeightCents, e.FloorCents, e.DistanceCents, e.PenaltyCents, e.TotalCents).
		Scan(&e.ID, &e.CreatedAt)
}

func EarningByOrder(ctx context.Context, q Querier, orderID int64) (*domain.CollectorEarning, error) {
	e := &domain.CollectorEarning{}
	err := q.QueryRow(ctx,
		`SELECT id, collector_id, order_id, base_cents, weight_cents, floor_cents, distance_cents,
		        penalty_cents, total_cents, held, held_reason, created_at
		 FROM collector_earnings WHERE order_id=$1`, orderID).
		Scan(&e.ID, &e.CollectorID, &e.OrderID, &e.BaseCents, &e.WeightCents, &e.FloorCents,
			&e.DistanceCents, &e.PenaltyCents, &e.TotalCents, &e.Held, &e.HeldReason, &e.CreatedAt)
	if err == pgx.ErrNoRows {
		return nil, ErrNotFound
	}
	return e, err
}

func ListEarnings(ctx context.Context, q Querier, collectorID int64, limit int) ([]*domain.CollectorEarning, error) {
	rows, err := q.Query(ctx,
		`SELECT id, collector_id, order_id, base_cents, weight_cents, floor_cents, distance_cents,
		        penalty_cents, total_cents, held, held_reason, created_at
		 FROM collector_earnings WHERE collector_id=$1 ORDER BY id DESC LIMIT $2`, collectorID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*domain.CollectorEarning
	for rows.Next() {
		e := &domain.CollectorEarning{}
		if err := rows.Scan(&e.ID, &e.CollectorID, &e.OrderID, &e.BaseCents, &e.WeightCents,
			&e.FloorCents, &e.DistanceCents, &e.PenaltyCents, &e.TotalCents,
			&e.Held, &e.HeldReason, &e.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// ---- disputes ----

func CreateDispute(ctx context.Context, q Querier, d *domain.Dispute) error {
	return q.QueryRow(ctx,
		`INSERT INTO disputes (order_id, item_id, raised_by, kind, detail, status) VALUES ($1,$2,$3,$4,$5,'OPEN')
		 RETURNING id, created_at`,
		d.OrderID, d.ItemID, d.RaisedBy, d.Kind, d.Detail).Scan(&d.ID, &d.CreatedAt)
}

func ResolveDispute(ctx context.Context, q Querier, id int64, status string, resolution map[string]any) error {
	b, _ := json.Marshal(resolution)
	_, err := q.Exec(ctx,
		`UPDATE disputes SET status=$2, resolution=$3, resolved_at=now() WHERE id=$1 AND status='OPEN'`,
		id, status, b)
	return err
}

func ListDisputes(ctx context.Context, q Querier, status string) ([]*domain.Dispute, error) {
	sql := `SELECT id, order_id, item_id, raised_by, kind, detail, status, resolution, created_at, resolved_at FROM disputes`
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
	var out []*domain.Dispute
	for rows.Next() {
		d := &domain.Dispute{}
		var res []byte
		if err := rows.Scan(&d.ID, &d.OrderID, &d.ItemID, &d.RaisedBy, &d.Kind, &d.Detail,
			&d.Status, &res, &d.CreatedAt, &d.ResolvedAt); err != nil {
			return nil, err
		}
		d.Resolution = map[string]any{}
		if res != nil {
			_ = json.Unmarshal(res, &d.Resolution)
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

func DisputeByID(ctx context.Context, q Querier, id int64) (*domain.Dispute, error) {
	d := &domain.Dispute{}
	var res []byte
	err := q.QueryRow(ctx,
		`SELECT id, order_id, item_id, raised_by, kind, detail, status, resolution, created_at, resolved_at
		 FROM disputes WHERE id=$1`, id).
		Scan(&d.ID, &d.OrderID, &d.ItemID, &d.RaisedBy, &d.Kind, &d.Detail, &d.Status, &res, &d.CreatedAt, &d.ResolvedAt)
	if err == pgx.ErrNoRows {
		return nil, ErrNotFound
	}
	d.Resolution = map[string]any{}
	if res != nil {
		_ = json.Unmarshal(res, &d.Resolution)
	}
	return d, err
}

// ---- complaints ----

func CreateComplaint(ctx context.Context, q Querier, c *domain.Complaint) error {
	return q.QueryRow(ctx,
		`INSERT INTO complaints (order_id, from_role, from_id, content, status) VALUES ($1,$2,$3,$4,'OPEN')
		 RETURNING id, created_at`,
		c.OrderID, c.FromRole, c.FromID, c.Content).Scan(&c.ID, &c.CreatedAt)
}

func ResolveComplaint(ctx context.Context, q Querier, id int64, resolution string) error {
	_, err := q.Exec(ctx,
		`UPDATE complaints SET status='RESOLVED', resolution=$2, resolved_at=now() WHERE id=$1 AND status='OPEN'`,
		id, resolution)
	return err
}

func ListComplaints(ctx context.Context, q Querier, status string) ([]*domain.Complaint, error) {
	sql := `SELECT id, order_id, from_role, from_id, content, status, resolution, created_at, resolved_at FROM complaints`
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
	var out []*domain.Complaint
	for rows.Next() {
		c := &domain.Complaint{}
		if err := rows.Scan(&c.ID, &c.OrderID, &c.FromRole, &c.FromID, &c.Content,
			&c.Status, &c.Resolution, &c.CreatedAt, &c.ResolvedAt); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// ---- notifications ----

func AddNotification(ctx context.Context, q Querier, userID int64, kind, content string) error {
	_, err := q.Exec(ctx,
		`INSERT INTO notifications (user_id, kind, content) VALUES ($1,$2,$3)`, userID, kind, content)
	return err
}

func ListNotifications(ctx context.Context, q Querier, userID int64, limit int) ([]map[string]any, error) {
	rows, err := q.Query(ctx,
		`SELECT id, kind, content, created_at FROM notifications WHERE user_id=$1 ORDER BY id DESC LIMIT $2`,
		userID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []map[string]any
	for rows.Next() {
		var id int64
		var kind, content string
		var createdAt time.Time
		if err := rows.Scan(&id, &kind, &content, &createdAt); err != nil {
			return nil, err
		}
		out = append(out, map[string]any{"id": id, "kind": kind, "content": content, "created_at": createdAt})
	}
	return out, rows.Err()
}

// ---- street stats ----

func UpsertStreetStat(ctx context.Context, q Querier, street, date, category string,
	orderDelta int, weightG, amountCents, points int64) error {
	_, err := q.Exec(ctx,
		`INSERT INTO street_stats (street, stat_date, category, order_count, weight_g, amount_cents, points)
		 VALUES ($1,$2,$3,$4,$5,$6,$7)
		 ON CONFLICT (street, stat_date, category) DO UPDATE SET
		   order_count = street_stats.order_count + EXCLUDED.order_count,
		   weight_g    = street_stats.weight_g + EXCLUDED.weight_g,
		   amount_cents= street_stats.amount_cents + EXCLUDED.amount_cents,
		   points      = street_stats.points + EXCLUDED.points`,
		street, date, category, orderDelta, weightG, amountCents, points)
	return err
}

func ListStreetStats(ctx context.Context, q Querier, street string) ([]map[string]any, error) {
	sql := `SELECT street, stat_date, category, order_count, weight_g, amount_cents, points FROM street_stats`
	args := []any{}
	if street != "" {
		sql += ` WHERE street=$1`
		args = append(args, street)
	}
	sql += ` ORDER BY stat_date DESC, street, category`
	rows, err := q.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []map[string]any
	for rows.Next() {
		var st, cat string
		var date time.Time
		var cnt int
		var w, amt, pts int64
		if err := rows.Scan(&st, &date, &cat, &cnt, &w, &amt, &pts); err != nil {
			return nil, err
		}
		out = append(out, map[string]any{
			"street": st, "date": date.Format("2006-01-02"), "category": cat,
			"order_count": cnt, "weight_kg": domain.GramsToKg(w),
			"amount_yuan": domain.CentsToYuan(amt), "points": pts,
		})
	}
	return out, rows.Err()
}

// ResourceFlows 资源流向: 居民->回收员->站点 全链路(按品类聚合)
func ResourceFlows(ctx context.Context, q Querier, street string) ([]map[string]any, error) {
	sql := `
		SELECT o.street, i.category,
		       count(DISTINCT o.id) AS orders,
		       count(DISTINCT o.resident_id) AS residents,
		       COALESCE(sum(i.onsite_weight_g),0) AS onsite_g,
		       COALESCE(sum(i.station_weight_g),0) AS station_g,
		       COALESCE(sum(i.station_cents),0) AS cents,
		       count(DISTINCT o.collector_id) AS collectors,
		       count(DISTINCT o.station_id) AS stations
		FROM orders o JOIN order_items i ON i.order_id=o.id
		WHERE o.status IN ('SETTLED','DELIVERED','INBOUND_CONFIRMED','COMPLETED')`
	args := []any{}
	if street != "" {
		sql += ` AND o.street=$1`
		args = append(args, street)
	}
	sql += ` GROUP BY o.street, i.category ORDER BY o.street, i.category`
	rows, err := q.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []map[string]any
	for rows.Next() {
		var st, cat string
		var orders, residents, collectors, stations int
		var onsiteG, stationG, cents int64
		if err := rows.Scan(&st, &cat, &orders, &residents, &onsiteG, &stationG, &cents, &collectors, &stations); err != nil {
			return nil, err
		}
		out = append(out, map[string]any{
			"street": st, "category": cat, "orders": orders, "residents": residents,
			"onsite_weight_kg": domain.GramsToKg(onsiteG), "station_weight_kg": domain.GramsToKg(stationG),
			"amount_yuan": domain.CentsToYuan(cents), "collectors": collectors, "stations": stations,
		})
	}
	return out, rows.Err()
}
