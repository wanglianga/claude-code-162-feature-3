package httpapi

import (
	"context"
	"net/http"
	"time"

	"recycle/internal/domain"
	"recycle/internal/store"
	"recycle/internal/svc"
)

// ---- 序列化 ----

func orderJSON(o *domain.Order) map[string]any {
	out := map[string]any{
		"id":                o.ID,
		"order_no":          o.OrderNo,
		"resident_id":       o.ResidentID,
		"status":            o.Status,
		"street":            o.Street,
		"address":           o.Address,
		"lat":               o.Lat,
		"lng":               o.Lng,
		"floor":             o.Floor,
		"has_elevator":      o.HasElevator,
		"time_window_start": o.TimeWindowStart,
		"time_window_end":   o.TimeWindowEnd,
		"photos":            o.Photos,
		"estimate_yuan":     domain.CentsToYuan(o.EstimateCents),
		"onsite_yuan":       domain.CentsToYuan(o.OnsiteCents),
		"final_yuan":        domain.CentsToYuan(o.FinalCents),
		"late_minutes":      o.LateMinutes,
		"version":           o.Version,
		"created_at":        o.CreatedAt,
		"updated_at":        o.UpdatedAt,
	}
	if o.CollectorID != nil {
		out["collector_id"] = *o.CollectorID
	}
	if o.StationID != nil {
		out["station_id"] = *o.StationID
	}
	if o.SettlementType != nil {
		out["settlement_type"] = *o.SettlementType
	}
	if o.CancelReason != nil {
		out["cancel_reason"] = *o.CancelReason
	}
	return out
}

// orderForResident 居民视角含核验码
func orderForResident(o *domain.Order) map[string]any {
	out := orderJSON(o)
	out["verify_code"] = o.VerifyCode
	return out
}

func itemJSON(it *domain.OrderItem) map[string]any {
	out := map[string]any{
		"id":              it.ID,
		"category":        it.Category,
		"source":          it.Source,
		"est_weight_kg":   domain.GramsToKg(it.EstWeightG),
		"est_price_yuan":  domain.CentsToYuan(it.EstPriceCents),
		"est_yuan":        domain.CentsToYuan(it.EstCents),
		"variance_status": it.VarianceStatus,
	}
	if it.OnsiteWeightG != nil {
		out["onsite_weight_kg"] = domain.GramsToKg(*it.OnsiteWeightG)
		out["contamination_pm"] = it.ContaminationPM
		out["onsite_yuan"] = domain.CentsToYuan(*it.OnsiteCents)
	}
	if it.StationWeightG != nil {
		out["station_weight_kg"] = domain.GramsToKg(*it.StationWeightG)
		out["station_yuan"] = domain.CentsToYuan(*it.StationCents)
		out["variance_kg"] = domain.GramsToKg(*it.VarianceG)
	}
	return out
}

func itemsJSON(items []*domain.OrderItem) []map[string]any {
	out := make([]map[string]any, 0, len(items))
	for _, it := range items {
		out = append(out, itemJSON(it))
	}
	return out
}

func settlementJSON(s *domain.Settlement) map[string]any {
	out := map[string]any{
		"id": s.ID, "order_id": s.OrderID, "type": s.Type,
		"amount_yuan": domain.CentsToYuan(s.AmountCents), "points": s.Points,
		"status": s.Status, "retry_count": s.RetryCount,
		"created_at": s.CreatedAt, "updated_at": s.UpdatedAt,
	}
	if s.FailReason != nil {
		out["fail_reason"] = *s.FailReason
	}
	return out
}

// ---- 通用 ----

func (s *Server) handleMe(w http.ResponseWriter, r *http.Request) {
	u := userOf(r)
	writeJSON(w, 200, map[string]any{
		"id": u.ID, "account": u.Account, "name": u.Name, "role": u.Role,
	})
}

func (s *Server) handlePrices(w http.ResponseWriter, r *http.Request) {
	rows, err := s.st.Pool.Query(r.Context(),
		`SELECT DISTINCT ON (category) category, price_cents_kg, effective_from
		 FROM market_prices WHERE effective_from<=now()
		 ORDER BY category, effective_from DESC, id DESC`)
	if err != nil {
		writeErr(w, err)
		return
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		var cat string
		var cents int64
		var eff time.Time
		if err := rows.Scan(&cat, &cents, &eff); err != nil {
			writeErr(w, err)
			return
		}
		out = append(out, map[string]any{
			"category": cat, "price_yuan_per_kg": domain.CentsToYuan(cents), "effective_from": eff,
		})
	}
	writeJSON(w, 200, map[string]any{"prices": out})
}

func (s *Server) handleNotifications(w http.ResponseWriter, r *http.Request) {
	u := userOf(r)
	list, err := store.ListNotifications(r.Context(), s.st.Pool, u.ID, 50)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"notifications": list})
}

// ---- 居民 ----

type createOrderReq struct {
	Items []struct {
		Category    string  `json:"category"`
		EstWeightKg float64 `json:"est_weight_kg"`
	} `json:"items"`
	Address        string   `json:"address"`
	Lat            float64  `json:"lat"`
	Lng            float64  `json:"lng"`
	Floor          int      `json:"floor"`
	HasElevator    bool     `json:"has_elevator"`
	WindowStart    string   `json:"time_window_start"`
	WindowEnd      string   `json:"time_window_end"`
	Photos         []string `json:"photos"`
	IdempotencyKey string   `json:"idempotency_key"`
}

func (s *Server) handleCreateOrder(w http.ResponseWriter, r *http.Request) {
	u := userOf(r)
	var req createOrderReq
	if err := decode(r, &req); err != nil {
		writeErr(w, err)
		return
	}
	ws, err := time.Parse(time.RFC3339, req.WindowStart)
	if err != nil {
		writeErr(w, &svc.ApiError{Code: 400, Message: "time_window_start must be RFC3339"})
		return
	}
	we, err := time.Parse(time.RFC3339, req.WindowEnd)
	if err != nil {
		writeErr(w, &svc.ApiError{Code: 400, Message: "time_window_end must be RFC3339"})
		return
	}
	creq := svc.CreateOrderReq{
		Address: req.Address, Lat: req.Lat, Lng: req.Lng,
		Floor: req.Floor, HasElevator: req.HasElevator,
		WindowStart: ws, WindowEnd: we, Photos: req.Photos,
		IdempotencyKey: req.IdempotencyKey,
	}
	for _, it := range req.Items {
		creq.Items = append(creq.Items, svc.CreateOrderItem{Category: it.Category, EstWeightKg: it.EstWeightKg})
	}
	o, items, err := s.svc.CreateOrder(r.Context(), u.ID, creq)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 201, map[string]any{"order": orderForResident(o), "items": itemsJSON(items)})
}

func (s *Server) handleMyOrders(w http.ResponseWriter, r *http.Request) {
	u := userOf(r)
	orders, err := store.ListOrdersByResident(r.Context(), s.st.Pool, u.ID)
	if err != nil {
		writeErr(w, err)
		return
	}
	out := []map[string]any{}
	for _, o := range orders {
		out = append(out, orderForResident(o))
	}
	writeJSON(w, 200, map[string]any{"orders": out})
}

// handleGetOrder 订单详情: 仅相关方可见
func (s *Server) handleGetOrder(w http.ResponseWriter, r *http.Request) {
	u := userOf(r)
	id, err := pathID(r, "id")
	if err != nil {
		writeErr(w, err)
		return
	}
	o, err := store.OrderByID(r.Context(), s.st.Pool, id)
	if err != nil {
		writeErr(w, err)
		return
	}
	if !s.canView(r.Context(), u, o) {
		writeErr(w, &svc.ApiError{Code: 403, Message: "not allowed"})
		return
	}
	items, err := store.ListOrderItems(r.Context(), s.st.Pool, id)
	if err != nil {
		writeErr(w, err)
		return
	}
	vo := orderJSON(o)
	if u.ID == o.ResidentID {
		vo = orderForResident(o)
	}
	writeJSON(w, 200, map[string]any{"order": vo, "items": itemsJSON(items)})
}

func (s *Server) canView(ctx context.Context, u *domain.User, o *domain.Order) bool {
	if u.Role == domain.RoleStreet {
		return true
	}
	if u.ID == o.ResidentID {
		return true
	}
	if o.CollectorID != nil && u.ID == *o.CollectorID {
		return true
	}
	if u.Role == domain.RoleStation && o.StationID != nil {
		if st, err := store.StationByUserID(ctx, s.st.Pool, u.ID); err == nil && st.ID == *o.StationID {
			return true
		}
	}
	return false
}

func (s *Server) handleTrace(w http.ResponseWriter, r *http.Request) {
	u := userOf(r)
	id, err := pathID(r, "id")
	if err != nil {
		writeErr(w, err)
		return
	}
	o, err := store.OrderByID(r.Context(), s.st.Pool, id)
	if err != nil {
		writeErr(w, err)
		return
	}
	if !s.canView(r.Context(), u, o) {
		writeErr(w, &svc.ApiError{Code: 403, Message: "not allowed"})
		return
	}
	trace, err := s.svc.Trace(r.Context(), id)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 200, trace)
}

func (s *Server) handleCancelOrder(w http.ResponseWriter, r *http.Request) {
	u := userOf(r)
	id, err := pathID(r, "id")
	if err != nil {
		writeErr(w, err)
		return
	}
	var req struct {
		Reason string `json:"reason"`
	}
	_ = decode(r, &req)
	o, err := s.svc.CancelOrder(r.Context(), u.ID, id, req.Reason)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"order": orderForResident(o)})
}

func (s *Server) handleConfirmWeigh(w http.ResponseWriter, r *http.Request) {
	u := userOf(r)
	id, err := pathID(r, "id")
	if err != nil {
		writeErr(w, err)
		return
	}
	o, err := s.svc.ConfirmWeigh(r.Context(), id, domain.RoleResident, u.ID, domain.MethodSelf, "")
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"order": orderForResident(o)})
}

func (s *Server) handleRaiseDispute(w http.ResponseWriter, r *http.Request) {
	u := userOf(r)
	id, err := pathID(r, "id")
	if err != nil {
		writeErr(w, err)
		return
	}
	var req struct {
		Kind   string `json:"kind"`
		Detail string `json:"detail"`
		ItemID *int64 `json:"item_id"`
	}
	if err := decode(r, &req); err != nil {
		writeErr(w, err)
		return
	}
	if req.Detail == "" {
		writeErr(w, &svc.ApiError{Code: 400, Message: "detail required"})
		return
	}
	kind := req.Kind
	if kind == "" {
		kind = "WEIGHT"
	}
	d, err := s.svc.RaiseDispute(r.Context(), u.ID, id, kind, req.Detail, req.ItemID)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 201, map[string]any{"dispute": d})
}

func (s *Server) handleFileComplaint(w http.ResponseWriter, r *http.Request) {
	u := userOf(r)
	id, err := pathID(r, "id")
	if err != nil {
		writeErr(w, err)
		return
	}
	var req struct {
		Content string `json:"content"`
	}
	if err := decode(r, &req); err != nil {
		writeErr(w, err)
		return
	}
	c, err := s.svc.FileComplaint(r.Context(), id, u.Role, u.ID, req.Content)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 201, map[string]any{"complaint": c})
}

func (s *Server) handlePointsBalance(w http.ResponseWriter, r *http.Request) {
	u := userOf(r)
	bal, entries, err := s.svc.PointsBalance(r.Context(), u.ID)
	if err != nil {
		writeErr(w, err)
		return
	}
	// 补发流水附带: 补发来源 / 到账时间 / 原订单编号
	reissues, err := store.ListPointsReissuesByResident(r.Context(), s.st.Pool, u.ID, 100)
	if err != nil {
		writeErr(w, err)
		return
	}
	reBySettlement := map[int64]*domain.PointsReissue{}
	for _, ri := range reissues {
		reBySettlement[ri.SettlementID] = ri
	}
	out := []map[string]any{}
	for _, e := range entries {
		row := map[string]any{
			"id": e.ID, "delta": e.Delta, "balance_after": e.BalanceAfter,
			"kind": e.Kind, "order_id": e.OrderID, "created_at": e.CreatedAt,
		}
		if e.SettlementID != nil {
			if ri, ok := reBySettlement[*e.SettlementID]; ok {
				row["reissue_source"] = ri.Source
				row["credited_at"] = ri.CreditedAt
				row["original_order_no"] = ri.OriginalOrderNo
			}
		}
		out = append(out, row)
	}
	writeJSON(w, 200, map[string]any{"balance": bal, "entries": out})
}

func (s *Server) handlePointsClaim(w http.ResponseWriter, r *http.Request) {
	u := userOf(r)
	var req struct {
		OrderID int64  `json:"order_id"`
		Note    string `json:"note"`
	}
	if err := decode(r, &req); err != nil {
		writeErr(w, err)
		return
	}
	if req.OrderID == 0 {
		writeErr(w, &svc.ApiError{Code: 400, Message: "order_id required"})
		return
	}
	result, err := s.svc.ClaimPoints(r.Context(), u.ID, req.OrderID, req.Note)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 200, result)
}

func (s *Server) handlePointsClaims(w http.ResponseWriter, r *http.Request) {
	u := userOf(r)
	list, err := store.ListPointsClaims(r.Context(), s.st.Pool, u.ID)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"claims": list})
}

func (s *Server) handleMyCertificates(w http.ResponseWriter, r *http.Request) {
	u := userOf(r)
	certs, err := store.ListCertificatesByResident(r.Context(), s.st.Pool, u.ID)
	if err != nil {
		writeErr(w, err)
		return
	}
	out := []map[string]any{}
	for _, c := range certs {
		out = append(out, map[string]any{
			"cert_no": c.CertNo, "order_id": c.OrderID, "category": c.Category,
			"weight_kg": domain.GramsToKg(c.WeightG), "co2_saved_kg": domain.GramsToKg(c.CO2SavedG),
			"issued_at": c.IssuedAt,
		})
	}
	writeJSON(w, 200, map[string]any{"certificates": out})
}
