package httpapi

import (
	"net/http"
	"strconv"
	"time"

	"recycle/internal/domain"
	"recycle/internal/store"
)

// ---- 居民: 积分补发明细(补发来源 / 到账时间 / 原订单编号) ----

func (s *Server) handlePointsReissues(w http.ResponseWriter, r *http.Request) {
	u := userOf(r)
	list, err := store.ListPointsReissuesByResident(r.Context(), s.st.Pool, u.ID, 100)
	if err != nil {
		writeErr(w, err)
		return
	}
	out := []map[string]any{}
	for _, ri := range list {
		out = append(out, map[string]any{
			"id":                ri.ID,
			"points":            ri.Points,
			"source":            ri.Source,          // 补发来源
			"credited_at":       ri.CreditedAt,      // 到账时间
			"original_order_no": ri.OriginalOrderNo, // 原订单编号
			"original_order_id": ri.OrderID,
			"reason":            ri.Reason,
			"created_at":        ri.CreatedAt,
		})
	}
	writeJSON(w, 200, map[string]any{"reissues": out})
}

// ---- 回收员: 待处理任务(未提交结算, 影响结算/收益) ----

func (s *Server) handleCollectorTasks(w http.ResponseWriter, r *http.Request) {
	u := userOf(r)
	status := r.URL.Query().Get("status")
	tasks, err := store.ListCollectorTasks(r.Context(), s.st.Pool, u.ID, status)
	if err != nil {
		writeErr(w, err)
		return
	}
	openCount, err := store.CountOpenCollectorTasks(r.Context(), s.st.Pool, u.ID)
	if err != nil {
		writeErr(w, err)
		return
	}
	// 关联订单号, 便于回收员识别
	out := []map[string]any{}
	for _, t := range tasks {
		item := map[string]any{
			"id": t.ID, "order_id": t.OrderID, "kind": t.Kind, "status": t.Status,
			"reason": t.Reason, "impact": t.Impact, "detail": t.Detail,
			"created_at": t.CreatedAt, "resolved_at": t.ResolvedAt,
		}
		if o, err := store.OrderByID(r.Context(), s.st.Pool, t.OrderID); err == nil {
			item["order_no"] = o.OrderNo
		}
		out = append(out, item)
	}
	writeJSON(w, 200, map[string]any{"tasks": out, "open_count": openCount})
}

// ---- 街道(运营): 手动触发积分核对补发 ----

func (s *Server) handleStreetReconcile(w http.ResponseWriter, r *http.Request) {
	report, err := s.svc.ReconcilePoints(r.Context())
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 200, report)
}

// ---- 街道(运营): 单笔订单四要素核对 ----

func (s *Server) handleStreetOrderCheck(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r, "id")
	if err != nil {
		writeErr(w, err)
		return
	}
	check, err := s.svc.CheckOrder(r.Context(), id)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"check": check})
}

// ---- 街道(运营): 技术排查记录 ----

func (s *Server) handleStreetIncidents(w http.ResponseWriter, r *http.Request) {
	status := r.URL.Query().Get("status")
	list, err := store.ListTechIncidents(r.Context(), s.st.Pool, status)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"incidents": list})
}

// ---- 街道(运营): 回调失败受影响居民名单(便于集中解释) ----

func (s *Server) handleStreetAffectedResidents(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	topic := q.Get("topic")
	if topic == "" {
		topic = domain.TopicPointsCommands
	}
	consumer := q.Get("consumer")
	if consumer == "" {
		consumer = "points-consumer"
	}
	// 默认统计最近 1 小时
	since := time.Now().Add(-time.Hour)
	if m := q.Get("since_minutes"); m != "" {
		if mins, err := strconv.Atoi(m); err == nil && mins > 0 {
			since = time.Now().Add(-time.Duration(mins) * time.Minute)
		}
	}
	list, err := store.ListAffectedResidents(r.Context(), s.st.Pool, topic, consumer, since)
	if err != nil {
		writeErr(w, err)
		return
	}
	out := []map[string]any{}
	for _, a := range list {
		out = append(out, map[string]any{
			"resident_id": a.ResidentID, "account": a.Account, "name": a.Name,
			"phone": a.Phone, "fail_count": a.FailCount,
			"orders_affected": a.OrdersAffected, "order_nos": a.OrderNos,
			"reissued_count": a.ReissuedCount, "last_fail_at": a.LastFailAt,
		})
	}
	writeJSON(w, 200, map[string]any{
		"topic": topic, "consumer": consumer, "since": since,
		"affected_count": len(out), "residents": out,
	})
}
