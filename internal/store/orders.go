package store

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"

	"github.com/jackc/pgx/v5"
	"recycle/internal/domain"
)

var ErrNotFound = errors.New("not found")
var ErrVersionConflict = errors.New("order version conflict")

// ---- users ----

func UserByToken(ctx context.Context, q Querier, token string) (*domain.User, error) {
	u := &domain.User{}
	err := q.QueryRow(ctx,
		`SELECT id, account, name, role, token, phone FROM users WHERE token=$1`, token).
		Scan(&u.ID, &u.Account, &u.Name, &u.Role, &u.Token, &u.Phone)
	if err == pgx.ErrNoRows {
		return nil, ErrNotFound
	}
	return u, err
}

func UserByID(ctx context.Context, q Querier, id int64) (*domain.User, error) {
	u := &domain.User{}
	err := q.QueryRow(ctx,
		`SELECT id, account, name, role, token, phone FROM users WHERE id=$1`, id).
		Scan(&u.ID, &u.Account, &u.Name, &u.Role, &u.Token, &u.Phone)
	if err == pgx.ErrNoRows {
		return nil, ErrNotFound
	}
	return u, err
}

// UsersByRole 某角色全部账号(如运营通知: street)
func UsersByRole(ctx context.Context, q Querier, role string) ([]*domain.User, error) {
	rows, err := q.Query(ctx,
		`SELECT id, account, name, role, token, phone FROM users WHERE role=$1`, role)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*domain.User
	for rows.Next() {
		u := &domain.User{}
		if err := rows.Scan(&u.ID, &u.Account, &u.Name, &u.Role, &u.Token, &u.Phone); err != nil {
			return nil, err
		}
		out = append(out, u)
	}
	return out, rows.Err()
}

// ---- residents / collectors / stations ----

func ResidentByID(ctx context.Context, q Querier, id int64) (*domain.Resident, error) {
	r := &domain.Resident{}
	err := q.QueryRow(ctx,
		`SELECT user_id, street, address, lat, lng, floor, has_elevator, is_elderly FROM residents WHERE user_id=$1`, id).
		Scan(&r.UserID, &r.Street, &r.Address, &r.Lat, &r.Lng, &r.Floor, &r.HasElevator, &r.IsElderly)
	if err == pgx.ErrNoRows {
		return nil, ErrNotFound
	}
	return r, err
}

func CollectorByID(ctx context.Context, q Querier, id int64) (*domain.Collector, error) {
	c := &domain.Collector{}
	err := q.QueryRow(ctx,
		`SELECT user_id, station_id, on_duty, lat, lng, max_concurrent FROM collectors WHERE user_id=$1`, id).
		Scan(&c.UserID, &c.StationID, &c.OnDuty, &c.Lat, &c.Lng, &c.MaxConcurrent)
	if err == pgx.ErrNoRows {
		return nil, ErrNotFound
	}
	return c, err
}

func StationByID(ctx context.Context, q Querier, id int64) (*domain.Station, error) {
	s := &domain.Station{}
	err := q.QueryRow(ctx,
		`SELECT id, user_id, name, street, lat, lng FROM stations WHERE id=$1`, id).
		Scan(&s.ID, &s.UserID, &s.Name, &s.Street, &s.Lat, &s.Lng)
	if err == pgx.ErrNoRows {
		return nil, ErrNotFound
	}
	return s, err
}

func StationByUserID(ctx context.Context, q Querier, userID int64) (*domain.Station, error) {
	s := &domain.Station{}
	err := q.QueryRow(ctx,
		`SELECT id, user_id, name, street, lat, lng FROM stations WHERE user_id=$1`, userID).
		Scan(&s.ID, &s.UserID, &s.Name, &s.Street, &s.Lat, &s.Lng)
	if err == pgx.ErrNoRows {
		return nil, ErrNotFound
	}
	return s, err
}

// ---- market prices ----

// CurrentPrice 取品类当前生效单价(分/kg)
func CurrentPrice(ctx context.Context, q Querier, category string) (int64, error) {
	var cents int64
	err := q.QueryRow(ctx,
		`SELECT price_cents_kg FROM market_prices WHERE category=$1 AND effective_from<=now()
		 ORDER BY effective_from DESC, id DESC LIMIT 1`, category).Scan(&cents)
	if err == pgx.ErrNoRows {
		return 0, ErrNotFound
	}
	return cents, err
}

func CategoryExists(ctx context.Context, q Querier, code string) (bool, error) {
	var ok bool
	err := q.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM categories WHERE code=$1)`, code).Scan(&ok)
	return ok, err
}

func CategoryInfo(ctx context.Context, q Querier, code string) (pointsPerYuan int, co2GPerKg int, err error) {
	err = q.QueryRow(ctx, `SELECT points_per_yuan, co2_g_per_kg FROM categories WHERE code=$1`, code).
		Scan(&pointsPerYuan, &co2GPerKg)
	return
}

// ---- orders ----

const orderCols = `id, order_no, resident_id, collector_id, station_id, status, street, address,
	lat, lng, floor, has_elevator, time_window_start, time_window_end, photos, verify_code,
	estimate_cents, onsite_cents, final_cents, settlement_type, late_minutes, cancel_reason,
	version, created_at, updated_at`

func scanOrder(row pgx.Row) (*domain.Order, error) {
	o := &domain.Order{}
	var photos []byte
	err := row.Scan(&o.ID, &o.OrderNo, &o.ResidentID, &o.CollectorID, &o.StationID, &o.Status,
		&o.Street, &o.Address, &o.Lat, &o.Lng, &o.Floor, &o.HasElevator,
		&o.TimeWindowStart, &o.TimeWindowEnd, &photos, &o.VerifyCode,
		&o.EstimateCents, &o.OnsiteCents, &o.FinalCents, &o.SettlementType,
		&o.LateMinutes, &o.CancelReason, &o.Version, &o.CreatedAt, &o.UpdatedAt)
	if err != nil {
		return nil, err
	}
	o.Photos = []string{}
	_ = json.Unmarshal(photos, &o.Photos)
	return o, nil
}

func CreateOrder(ctx context.Context, q Querier, o *domain.Order) error {
	photos, _ := json.Marshal(o.Photos)
	return q.QueryRow(ctx,
		`INSERT INTO orders (order_no, resident_id, status, street, address, lat, lng, floor,
			has_elevator, time_window_start, time_window_end, photos, verify_code, estimate_cents, idempotency_key)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
		 RETURNING id, created_at, updated_at`,
		o.OrderNo, o.ResidentID, o.Status, o.Street, o.Address, o.Lat, o.Lng, o.Floor,
		o.HasElevator, o.TimeWindowStart, o.TimeWindowEnd, photos, o.VerifyCode, o.EstimateCents, nil).
		Scan(&o.ID, &o.CreatedAt, &o.UpdatedAt)
}

// CreateOrderIdempotent 带幂等键创建; 冲突时返回已有订单
func CreateOrderIdempotent(ctx context.Context, q Querier, o *domain.Order, idemKey string) (created bool, err error) {
	photos, _ := json.Marshal(o.Photos)
	err = q.QueryRow(ctx,
		`INSERT INTO orders (order_no, resident_id, status, street, address, lat, lng, floor,
			has_elevator, time_window_start, time_window_end, photos, verify_code, estimate_cents, idempotency_key)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
		 ON CONFLICT (idempotency_key) DO NOTHING
		 RETURNING id`,
		o.OrderNo, o.ResidentID, o.Status, o.Street, o.Address, o.Lat, o.Lng, o.Floor,
		o.HasElevator, o.TimeWindowStart, o.TimeWindowEnd, photos, o.VerifyCode, o.EstimateCents, idemKey).
		Scan(&o.ID)
	if err == pgx.ErrNoRows {
		// 已存在: 读取
		existing, gerr := OrderByIdemKey(ctx, q, idemKey)
		if gerr != nil {
			return false, gerr
		}
		*o = *existing
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func OrderByIdemKey(ctx context.Context, q Querier, key string) (*domain.Order, error) {
	o, err := scanOrder(q.QueryRow(ctx, `SELECT `+orderCols+` FROM orders WHERE idempotency_key=$1`, key))
	if err == pgx.ErrNoRows {
		return nil, ErrNotFound
	}
	return o, err
}

func OrderByID(ctx context.Context, q Querier, id int64) (*domain.Order, error) {
	o, err := scanOrder(q.QueryRow(ctx, `SELECT `+orderCols+` FROM orders WHERE id=$1`, id))
	if err == pgx.ErrNoRows {
		return nil, ErrNotFound
	}
	return o, err
}

// OrderByIDForUpdate 行锁读取(事务内)
func OrderByIDForUpdate(ctx context.Context, q Querier, id int64) (*domain.Order, error) {
	o, err := scanOrder(q.QueryRow(ctx, `SELECT `+orderCols+` FROM orders WHERE id=$1 FOR UPDATE`, id))
	if err == pgx.ErrNoRows {
		return nil, ErrNotFound
	}
	return o, err
}

// UpdateOrderStatus 乐观锁状态推进: 仅当当前 version 匹配时更新
func UpdateOrderStatus(ctx context.Context, q Querier, id int64, expectVersion int, newStatus string) error {
	tag, err := q.Exec(ctx,
		`UPDATE orders SET status=$1, version=version+1, updated_at=now() WHERE id=$2 AND version=$3`,
		newStatus, id, expectVersion)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrVersionConflict
	}
	return nil
}

// UpdateOrderFields 推进状态并同时更新若干字段(乐观锁)
// sets 的键必须是白名单内的列名(由 svc 层硬编码传入, 不接受用户输入)
func UpdateOrderFields(ctx context.Context, q Querier, id int64, expectVersion int, newStatus string, sets map[string]any) error {
	sql := `UPDATE orders SET status=$1, version=version+1, updated_at=now()`
	args := []any{newStatus}
	i := 2
	for k, v := range sets {
		sql += `, ` + k + `=$` + strconv.Itoa(i)
		args = append(args, v)
		i++
	}
	sql += ` WHERE id=$` + strconv.Itoa(i) + ` AND version=$` + strconv.Itoa(i+1)
	args = append(args, id, expectVersion)
	tag, err := q.Exec(ctx, sql, args...)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrVersionConflict
	}
	return nil
}

func ListOrdersByResident(ctx context.Context, q Querier, residentID int64) ([]*domain.Order, error) {
	rows, err := q.Query(ctx, `SELECT `+orderCols+` FROM orders WHERE resident_id=$1 ORDER BY id DESC`, residentID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanOrders(rows)
}

func ListOrdersByCollector(ctx context.Context, q Querier, collectorID int64, activeOnly bool) ([]*domain.Order, error) {
	sql := `SELECT ` + orderCols + ` FROM orders WHERE collector_id=$1`
	if activeOnly {
		sql += ` AND status IN ('ASSIGNED','ACCEPTED','EN_ROUTE','ARRIVED','RESIDENT_VERIFIED','WEIGHED','DISPUTED','CONFIRMED')`
	}
	sql += ` ORDER BY time_window_start, id`
	rows, err := q.Query(ctx, sql, collectorID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanOrders(rows)
}

// ListPendingOrders 待派单
func ListPendingOrders(ctx context.Context, q Querier, limit int) ([]*domain.Order, error) {
	rows, err := q.Query(ctx, `SELECT `+orderCols+` FROM orders WHERE status='PENDING' ORDER BY id LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanOrders(rows)
}

// ListLateCandidates 可能迟到的在途单
func ListLateCandidates(ctx context.Context, q Querier) ([]*domain.Order, error) {
	rows, err := q.Query(ctx,
		`SELECT `+orderCols+` FROM orders WHERE status IN ('ASSIGNED','ACCEPTED','EN_ROUTE') AND time_window_end < now()`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanOrders(rows)
}

// ListSettledUndelivered 已结算未送达(供站点/回收员查询)
func ListOrdersByStatus(ctx context.Context, q Querier, status string, limit int) ([]*domain.Order, error) {
	rows, err := q.Query(ctx, `SELECT `+orderCols+` FROM orders WHERE status=$1 ORDER BY id DESC LIMIT $2`, status, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanOrders(rows)
}

func scanOrders(rows pgx.Rows) ([]*domain.Order, error) {
	var out []*domain.Order
	for rows.Next() {
		o := &domain.Order{}
		var photos []byte
		if err := rows.Scan(&o.ID, &o.OrderNo, &o.ResidentID, &o.CollectorID, &o.StationID, &o.Status,
			&o.Street, &o.Address, &o.Lat, &o.Lng, &o.Floor, &o.HasElevator,
			&o.TimeWindowStart, &o.TimeWindowEnd, &photos, &o.VerifyCode,
			&o.EstimateCents, &o.OnsiteCents, &o.FinalCents, &o.SettlementType,
			&o.LateMinutes, &o.CancelReason, &o.Version, &o.CreatedAt, &o.UpdatedAt); err != nil {
			return nil, err
		}
		o.Photos = []string{}
		_ = json.Unmarshal(photos, &o.Photos)
		out = append(out, o)
	}
	return out, rows.Err()
}

// OrderIDByOrderNo 按订单编号查订单 ID(不存在返回 0)
func OrderIDByOrderNo(ctx context.Context, q Querier, orderNo string) int64 {
	var id int64
	_ = q.QueryRow(ctx, `SELECT id FROM orders WHERE order_no=$1`, orderNo).Scan(&id)
	return id
}

// CountActiveByCollector 回收员当前在途单数
func CountActiveByCollector(ctx context.Context, q Querier, collectorID int64) (int, error) {
	var n int
	err := q.QueryRow(ctx,
		`SELECT count(*) FROM orders WHERE collector_id=$1 AND status IN
		 ('ASSIGNED','ACCEPTED','EN_ROUTE','ARRIVED','RESIDENT_VERIFIED','WEIGHED','DISPUTED','CONFIRMED')`,
		collectorID).Scan(&n)
	return n, err
}

// ---- order items ----

func CreateOrderItem(ctx context.Context, q Querier, it *domain.OrderItem) error {
	return q.QueryRow(ctx,
		`INSERT INTO order_items (order_id, category, source, est_weight_g, est_price_cents, est_cents)
		 VALUES ($1,$2,$3,$4,$5,$6) RETURNING id`,
		it.OrderID, it.Category, it.Source, it.EstWeightG, it.EstPriceCents, it.EstCents).
		Scan(&it.ID)
}

func ListOrderItems(ctx context.Context, q Querier, orderID int64) ([]*domain.OrderItem, error) {
	rows, err := q.Query(ctx,
		`SELECT id, order_id, category, source, est_weight_g, est_price_cents, est_cents,
			onsite_weight_g, contamination_pm, onsite_price_cents, onsite_cents,
			station_weight_g, station_cents, variance_g, variance_status
		 FROM order_items WHERE order_id=$1 ORDER BY id`, orderID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*domain.OrderItem
	for rows.Next() {
		it := &domain.OrderItem{}
		if err := rows.Scan(&it.ID, &it.OrderID, &it.Category, &it.Source,
			&it.EstWeightG, &it.EstPriceCents, &it.EstCents,
			&it.OnsiteWeightG, &it.ContaminationPM, &it.OnsitePriceCents, &it.OnsiteCents,
			&it.StationWeightG, &it.StationCents, &it.VarianceG, &it.VarianceStatus); err != nil {
			return nil, err
		}
		out = append(out, it)
	}
	return out, rows.Err()
}

// ---- events / confirmations ----

func AddOrderEvent(ctx context.Context, q Querier, ev *domain.OrderEvent) error {
	detail, _ := json.Marshal(ev.Detail)
	return q.QueryRow(ctx,
		`INSERT INTO order_events (order_id, from_status, to_status, actor_role, actor_id, detail)
		 VALUES ($1,$2,$3,$4,$5,$6) RETURNING id, created_at`,
		ev.OrderID, ev.FromStatus, ev.ToStatus, ev.ActorRole, ev.ActorID, detail).
		Scan(&ev.ID, &ev.CreatedAt)
}

func ListOrderEvents(ctx context.Context, q Querier, orderID int64) ([]*domain.OrderEvent, error) {
	rows, err := q.Query(ctx,
		`SELECT id, order_id, from_status, to_status, actor_role, actor_id, detail, created_at
		 FROM order_events WHERE order_id=$1 ORDER BY id`, orderID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*domain.OrderEvent
	for rows.Next() {
		ev := &domain.OrderEvent{}
		var detail []byte
		if err := rows.Scan(&ev.ID, &ev.OrderID, &ev.FromStatus, &ev.ToStatus,
			&ev.ActorRole, &ev.ActorID, &detail, &ev.CreatedAt); err != nil {
			return nil, err
		}
		ev.Detail = map[string]any{}
		_ = json.Unmarshal(detail, &ev.Detail)
		out = append(out, ev)
	}
	return out, rows.Err()
}

func AddConfirmation(ctx context.Context, q Querier, c *domain.Confirmation) error {
	payload, _ := json.Marshal(c.Payload)
	return q.QueryRow(ctx,
		`INSERT INTO confirmations (order_id, kind, actor_role, actor_id, method, payload)
		 VALUES ($1,$2,$3,$4,$5,$6) RETURNING id, created_at`,
		c.OrderID, c.Kind, c.ActorRole, c.ActorID, c.Method, payload).
		Scan(&c.ID, &c.CreatedAt)
}

func ListConfirmations(ctx context.Context, q Querier, orderID int64) ([]*domain.Confirmation, error) {
	rows, err := q.Query(ctx,
		`SELECT id, order_id, kind, actor_role, actor_id, method, payload, created_at
		 FROM confirmations WHERE order_id=$1 ORDER BY id`, orderID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*domain.Confirmation
	for rows.Next() {
		c := &domain.Confirmation{}
		var payload []byte
		if err := rows.Scan(&c.ID, &c.OrderID, &c.Kind, &c.ActorRole, &c.ActorID, &c.Method, &payload, &c.CreatedAt); err != nil {
			return nil, err
		}
		c.Payload = map[string]any{}
		_ = json.Unmarshal(payload, &c.Payload)
		out = append(out, c)
	}
	return out, rows.Err()
}
