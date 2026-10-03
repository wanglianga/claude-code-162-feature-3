package httpapi

import (
	"net/http"

	"recycle/internal/domain"
	"recycle/internal/store"
	"recycle/internal/svc"
)

// ---- 居民: 积分异常对账与补发明细 ----

// handlePointsReconcile 居民报告积分未到账: 四方核对并处置
func (s *Server) handlePointsReconcile(w http.ResponseWriter, r *http.Request) {
	u := userOf(r)
	var req struct {
		OrderID int64 `json:"order_id"`
	}
	if err := decode(r, &req); err != nil {
		writeErr(w, err)
		return
	}
	if req.OrderID == 0 {
		writeErr(w, &svc.ApiError{Code: 400, Message: "order_id required"})
		return
	}
	result, err := s.svc.ReconcileResidentPoints(r.Context(), u.ID, req.OrderID)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 200, result)
}

// handlePointsReissues 居民查看积分补发记录: 补发来源/到账时间/原订单编号
func (s *Server) handlePointsReissues(w http.ResponseWriter, r *http.Request) {
	u := userOf(r)
	entries, err := store.ListPointsEntries(r.Context(), s.st.Pool, u.ID, 200)
	if err != nil {
		writeErr(w, err)
		return
	}
	out := []map[string]any{}
	for _, e := range entries {
		if e.Source == domain.PointsSourceNormal {
			continue
		}
		sourceLabel := map[string]string{
			domain.PointsSourceAutoReissue:   "系统延迟自动补发",
			domain.PointsSourceManualReissue: "申诉核对补发",
		}[e.Source]
		out = append(out, map[string]any{
			"id": e.ID, "delta": e.Delta, "balance_after": e.BalanceAfter,
			"kind": e.Kind, "source": e.Source, "source_label": sourceLabel,
			"reissue_reason": e.ReissueReason, "origin_order_no": e.OriginOrderNo,
			"order_id": e.OrderID, "credited_at": e.CreatedAt,
		})
	}
	writeJSON(w, 200, map[string]any{"reissues": out})
}

// ---- 回收员: 待处理任务 ----

func (s *Server) handleCollectorTasks(w http.ResponseWriter, r *http.Request) {
	u := userOf(r)
	status := r.URL.Query().Get("status")
	tasks, err := store.ListCollectorTasks(r.Context(), s.st.Pool, u.ID, status)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"tasks": tasks})
}

// handleCollectorTaskSubmit 回收员按任务补提交结算(默认积分结算)
func (s *Server) handleCollectorTaskSubmit(w http.ResponseWriter, r *http.Request) {
	u := userOf(r)
	id, err := pathID(r, "id")
	if err != nil {
		writeErr(w, err)
		return
	}
	var req struct {
		Type string `json:"type"`
	}
	_ = decode(r, &req)
	settleType := req.Type
	if settleType == "" {
		settleType = domain.SettlePoints
	}
	task, err := store.CollectorTaskByID(r.Context(), s.st.Pool, id)
	if err != nil {
		writeErr(w, err)
		return
	}
	if task.CollectorID != u.ID {
		writeErr(w, &svc.ApiError{Code: 403, Message: "not your task"})
		return
	}
	st, o, err := s.svc.Settle(r.Context(), u.ID, task.OrderID, settleType, false)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{
		"task_id": id, "settlement": settlementJSON(st), "order": orderJSON(o),
	})
}

// ---- 运营(街道): 待处理任务/技术排查/受影响名单 ----

func (s *Server) handleStreetCollectorTasks(w http.ResponseWriter, r *http.Request) {
	status := r.URL.Query().Get("status")
	tasks, err := store.ListCollectorTasks(r.Context(), s.st.Pool, 0, status)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"tasks": tasks})
}

func (s *Server) handleStreetResolveCollectorTask(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r, "id")
	if err != nil {
		writeErr(w, err)
		return
	}
	var req struct {
		Action     string `json:"action"` // release | cancel
		Resolution string `json:"resolution"`
	}
	if err := decode(r, &req); err != nil {
		writeErr(w, err)
		return
	}
	t, err := s.svc.ResolveCollectorTask(r.Context(), id, req.Action, req.Resolution)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"task": t})
}

func (s *Server) handleStreetTechIncidents(w http.ResponseWriter, r *http.Request) {
	status := r.URL.Query().Get("status")
	list, err := store.ListTechIncidents(r.Context(), s.st.Pool, status)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"incidents": list})
}

func (s *Server) handleStreetIncidentResidents(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r, "id")
	if err != nil {
		writeErr(w, err)
		return
	}
	if _, err := store.TechIncidentByID(r.Context(), s.st.Pool, id); err != nil {
		writeErr(w, err)
		return
	}
	rows, err := store.ListIncidentResidents(r.Context(), s.st.Pool, id)
	if err != nil {
		writeErr(w, err)
		return
	}
	out := []map[string]any{}
	for _, x := range rows {
		out = append(out, map[string]any{
			"resident_id": x.ResidentID, "account": x.Account, "name": x.Name, "phone": x.Phone,
			"order_id": x.OrderID, "order_no": x.OrderNo,
			"fail_count": x.FailCount, "last_fail_at": x.LastFailAt,
		})
	}
	writeJSON(w, 200, map[string]any{"incident_id": id, "residents": out})
}

func (s *Server) handleStreetResolveIncident(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r, "id")
	if err != nil {
		writeErr(w, err)
		return
	}
	var req struct {
		Resolution string `json:"resolution"`
	}
	if err := decode(r, &req); err != nil {
		writeErr(w, err)
		return
	}
	if req.Resolution == "" {
		writeErr(w, &svc.ApiError{Code: 400, Message: "resolution required"})
		return
	}
	if err := s.svc.ResolveTechIncident(r.Context(), id, req.Resolution); err != nil {
		writeErr(w, err)
		return
	}
	inc, err := store.TechIncidentByID(r.Context(), s.st.Pool, id)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"incident": inc})
}

func (s *Server) handleStreetCallbackFailures(w http.ResponseWriter, r *http.Request) {
	consumer := r.URL.Query().Get("consumer")
	list, err := store.ListCallbackFailures(r.Context(), s.st.Pool, consumer, 200)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"failures": list})
}
