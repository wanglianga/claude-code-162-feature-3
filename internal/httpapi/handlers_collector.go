package httpapi

import (
	"context"
	"net/http"

	"recycle/internal/domain"
	"recycle/internal/store"
	"recycle/internal/svc"
)

// ---- 回收员 ----

func (s *Server) handleDuty(w http.ResponseWriter, r *http.Request) {
	u := userOf(r)
	var req struct {
		OnDuty bool    `json:"on_duty"`
		Lat    float64 `json:"lat"`
		Lng    float64 `json:"lng"`
	}
	if err := decode(r, &req); err != nil {
		writeErr(w, err)
		return
	}
	if err := s.svc.UpdateCollectorDuty(r.Context(), u.ID, req.OnDuty, req.Lat, req.Lng); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"on_duty": req.OnDuty})
}

// handleCollectorOrders 回收员路线: 在途单按预约窗口排序
func (s *Server) handleCollectorOrders(w http.ResponseWriter, r *http.Request) {
	u := userOf(r)
	orders, err := store.ListOrdersByCollector(r.Context(), s.st.Pool, u.ID, true)
	if err != nil {
		writeErr(w, err)
		return
	}
	out := []map[string]any{}
	for _, o := range orders {
		out = append(out, orderJSON(o))
	}
	writeJSON(w, 200, map[string]any{"orders": out})
}

func (s *Server) handleAccept(w http.ResponseWriter, r *http.Request) {
	s.collectorTransit(w, r, s.svc.CollectorAccept)
}

func (s *Server) handleStart(w http.ResponseWriter, r *http.Request) {
	s.collectorTransit(w, r, s.svc.CollectorStart)
}

func (s *Server) handleArrive(w http.ResponseWriter, r *http.Request) {
	s.collectorTransit(w, r, s.svc.CollectorArrive)
}

func (s *Server) handleDeliver(w http.ResponseWriter, r *http.Request) {
	s.collectorTransit(w, r, s.svc.Deliver)
}

func (s *Server) collectorTransit(w http.ResponseWriter, r *http.Request,
	fn func(ctx context.Context, collectorID, orderID int64) (*domain.Order, error)) {
	u := userOf(r)
	id, err := pathID(r, "id")
	if err != nil {
		writeErr(w, err)
		return
	}
	o, err := fn(r.Context(), u.ID, id)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"order": orderJSON(o)})
}

func (s *Server) handleVerifyResident(w http.ResponseWriter, r *http.Request) {
	u := userOf(r)
	id, err := pathID(r, "id")
	if err != nil {
		writeErr(w, err)
		return
	}
	var req struct {
		Method       string `json:"method"` // SCAN_CODE | ASSISTED
		VerifyCode   string `json:"verify_code"`
		AssistedNote string `json:"assisted_note"`
	}
	if err := decode(r, &req); err != nil {
		writeErr(w, err)
		return
	}
	o, err := s.svc.VerifyResident(r.Context(), u.ID, id, req.Method, req.VerifyCode, req.AssistedNote)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"order": orderJSON(o)})
}

func (s *Server) handleAddItem(w http.ResponseWriter, r *http.Request) {
	u := userOf(r)
	id, err := pathID(r, "id")
	if err != nil {
		writeErr(w, err)
		return
	}
	var req struct {
		Category    string  `json:"category"`
		EstWeightKg float64 `json:"est_weight_kg"`
	}
	if err := decode(r, &req); err != nil {
		writeErr(w, err)
		return
	}
	item, err := s.svc.AddOnsiteItem(r.Context(), u.ID, id, req.Category, req.EstWeightKg)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 201, map[string]any{"item": itemJSON(item)})
}

func (s *Server) handleWeigh(w http.ResponseWriter, r *http.Request) {
	u := userOf(r)
	id, err := pathID(r, "id")
	if err != nil {
		writeErr(w, err)
		return
	}
	var req struct {
		Weights []struct {
			Category        string  `json:"category"`
			WeightKg        float64 `json:"weight_kg"`
			ContaminationPM int     `json:"contamination_pm"`
		} `json:"weights"`
		Photos []string `json:"photos"`
	}
	if err := decode(r, &req); err != nil {
		writeErr(w, err)
		return
	}
	var weights []svc.WeighItem
	for _, wi := range req.Weights {
		weights = append(weights, svc.WeighItem{
			Category: wi.Category, WeightKg: wi.WeightKg, ContaminationPM: wi.ContaminationPM,
		})
	}
	o, items, err := s.svc.Weigh(r.Context(), u.ID, id, weights, req.Photos)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"order": orderJSON(o), "items": itemsJSON(items)})
}

// handleConfirmAssisted 回收员协助老人确认称重
func (s *Server) handleConfirmAssisted(w http.ResponseWriter, r *http.Request) {
	u := userOf(r)
	id, err := pathID(r, "id")
	if err != nil {
		writeErr(w, err)
		return
	}
	var req struct {
		AssistedNote string `json:"assisted_note"`
	}
	if err := decode(r, &req); err != nil {
		writeErr(w, err)
		return
	}
	o, err := s.svc.ConfirmWeigh(r.Context(), id, domain.RoleCollector, u.ID, domain.MethodAssisted, req.AssistedNote)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"order": orderJSON(o)})
}

func (s *Server) handleSettle(w http.ResponseWriter, r *http.Request) {
	u := userOf(r)
	id, err := pathID(r, "id")
	if err != nil {
		writeErr(w, err)
		return
	}
	var req struct {
		Type         string `json:"type"`          // cash | points
		SimulateFail bool   `json:"simulate_fail"` // 测试注入: 模拟现金支付失败
	}
	if err := decode(r, &req); err != nil {
		writeErr(w, err)
		return
	}
	st, o, err := s.svc.Settle(r.Context(), u.ID, id, req.Type, req.SimulateFail)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"settlement": settlementJSON(st), "order": orderJSON(o)})
}

func (s *Server) handleSettleRetry(w http.ResponseWriter, r *http.Request) {
	u := userOf(r)
	id, err := pathID(r, "id")
	if err != nil {
		writeErr(w, err)
		return
	}
	var req struct {
		SimulateFail bool `json:"simulate_fail"`
	}
	_ = decode(r, &req)
	st, o, err := s.svc.RetryCashSettlement(r.Context(), u.ID, id, req.SimulateFail)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"settlement": settlementJSON(st), "order": orderJSON(o)})
}

func (s *Server) handleEarnings(w http.ResponseWriter, r *http.Request) {
	u := userOf(r)
	list, err := store.ListEarnings(r.Context(), s.st.Pool, u.ID, 50)
	if err != nil {
		writeErr(w, err)
		return
	}
	out := []map[string]any{}
	var total int64
	for _, e := range list {
		out = append(out, map[string]any{
			"order_id": e.OrderID, "base_yuan": domain.CentsToYuan(e.BaseCents),
			"weight_yuan": domain.CentsToYuan(e.WeightCents), "floor_yuan": domain.CentsToYuan(e.FloorCents),
			"distance_yuan": domain.CentsToYuan(e.DistanceCents), "penalty_yuan": domain.CentsToYuan(e.PenaltyCents),
			"total_yuan": domain.CentsToYuan(e.TotalCents),
			"held":       e.Held, "held_reason": e.HeldReason, "created_at": e.CreatedAt,
		})
		if !e.Held {
			total += e.TotalCents // 冻结收益不计入可发放合计
		}
	}
	writeJSON(w, 200, map[string]any{"earnings": out, "total_yuan": domain.CentsToYuan(total)})
}
