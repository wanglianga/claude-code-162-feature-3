package httpapi

import (
	"net/http"

	"recycle/internal/domain"
	"recycle/internal/store"
	"recycle/internal/svc"
)

// ---- 回收站 ----

func (s *Server) handleReweigh(w http.ResponseWriter, r *http.Request) {
	u := userOf(r)
	orderID, err := pathID(r, "orderId")
	if err != nil {
		writeErr(w, err)
		return
	}
	var req struct {
		Weights []struct {
			Category string  `json:"category"`
			WeightKg float64 `json:"weight_kg"`
		} `json:"weights"`
	}
	if err := decode(r, &req); err != nil {
		writeErr(w, err)
		return
	}
	var weights []svc.ReweighItem
	for _, wi := range req.Weights {
		weights = append(weights, svc.ReweighItem{Category: wi.Category, WeightKg: wi.WeightKg})
	}
	o, items, err := s.svc.StationReweigh(r.Context(), u.ID, orderID, weights)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"order": orderJSON(o), "items": itemsJSON(items)})
}

// handleStationInbound 站点待入库/已入库列表
func (s *Server) handleStationInbound(w http.ResponseWriter, r *http.Request) {
	u := userOf(r)
	st, err := store.StationByUserID(r.Context(), s.st.Pool, u.ID)
	if err != nil {
		writeErr(w, err)
		return
	}
	orders, err := store.ListOrdersByStatus(r.Context(), s.st.Pool, domain.StatusDelivered, 100)
	if err != nil {
		writeErr(w, err)
		return
	}
	pending := []map[string]any{}
	for _, o := range orders {
		if o.StationID != nil && *o.StationID == st.ID {
			pending = append(pending, orderJSON(o))
		}
	}
	writeJSON(w, 200, map[string]any{"pending_inbound": pending})
}

// handleStationQuality 入库质量: 复称差异统计
func (s *Server) handleStationQuality(w http.ResponseWriter, r *http.Request) {
	u := userOf(r)
	st, err := store.StationByUserID(r.Context(), s.st.Pool, u.ID)
	if err != nil {
		writeErr(w, err)
		return
	}
	rows, err := s.st.Pool.Query(r.Context(),
		`SELECT i.category, count(*) AS items,
			COALESCE(sum(i.onsite_weight_g),0), COALESCE(sum(i.station_weight_g),0),
			COALESCE(sum(ABS(i.variance_g)),0),
			sum(CASE WHEN i.variance_status='OVER_TOLERANCE' THEN 1 ELSE 0 END)
		 FROM order_items i
		 JOIN orders o ON o.id=i.order_id
		 WHERE o.station_id=$1 AND i.station_weight_g IS NOT NULL
		 GROUP BY i.category`, st.ID)
	if err != nil {
		writeErr(w, err)
		return
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		var cat string
		var items, onsiteG, stationG, absVar, overCnt int64
		if err := rows.Scan(&cat, &items, &onsiteG, &stationG, &absVar, &overCnt); err != nil {
			writeErr(w, err)
			return
		}
		out = append(out, map[string]any{
			"category": cat, "items": items,
			"onsite_weight_kg": domain.GramsToKg(onsiteG), "station_weight_kg": domain.GramsToKg(stationG),
			"abs_variance_kg": domain.GramsToKg(absVar), "over_tolerance_items": overCnt,
		})
	}
	writeJSON(w, 200, map[string]any{"station": st.Name, "quality": out})
}

// ---- 街道 ----

func (s *Server) handleStreetStats(w http.ResponseWriter, r *http.Request) {
	u := userOf(r)
	street := r.URL.Query().Get("street")
	if street == "" {
		// 街道账号默认看本街道
		street = u.Account
	}
	stats, err := store.ListStreetStats(r.Context(), s.st.Pool, street)
	if err != nil {
		writeErr(w, err)
		return
	}
	// 参与情况: 预约居民数/订单数
	var residents, orders int
	_ = s.st.Pool.QueryRow(r.Context(),
		`SELECT count(DISTINCT resident_id), count(*) FROM orders WHERE street=$1`, street).
		Scan(&residents, &orders)
	writeJSON(w, 200, map[string]any{
		"street": street, "participating_residents": residents, "total_orders": orders,
		"category_stats": stats,
	})
}

func (s *Server) handleStreetFlows(w http.ResponseWriter, r *http.Request) {
	u := userOf(r)
	street := r.URL.Query().Get("street")
	if street == "" {
		street = u.Account
	}
	flows, err := store.ResourceFlows(r.Context(), s.st.Pool, street)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"flows": flows})
}

func (s *Server) handleListDisputes(w http.ResponseWriter, r *http.Request) {
	status := r.URL.Query().Get("status")
	list, err := store.ListDisputes(r.Context(), s.st.Pool, status)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"disputes": list})
}

func (s *Server) handleResolveDispute(w http.ResponseWriter, r *http.Request) {
	u := userOf(r)
	id, err := pathID(r, "id")
	if err != nil {
		writeErr(w, err)
		return
	}
	var req struct {
		Action          string `json:"action"` // adjust | confirm | reject
		Note            string `json:"note"`
		AdjustedWeights []struct {
			Category        string  `json:"category"`
			WeightKg        float64 `json:"weight_kg"`
			ContaminationPM int     `json:"contamination_pm"`
		} `json:"adjusted_weights"`
	}
	if err := decode(r, &req); err != nil {
		writeErr(w, err)
		return
	}
	var weights []svc.WeighItem
	for _, wi := range req.AdjustedWeights {
		weights = append(weights, svc.WeighItem{
			Category: wi.Category, WeightKg: wi.WeightKg, ContaminationPM: wi.ContaminationPM,
		})
	}
	d, err := s.svc.ResolveDispute(r.Context(), u.ID, id, req.Action, weights, req.Note)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"dispute": d})
}

func (s *Server) handleListComplaints(w http.ResponseWriter, r *http.Request) {
	status := r.URL.Query().Get("status")
	list, err := store.ListComplaints(r.Context(), s.st.Pool, status)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"complaints": list})
}

func (s *Server) handleResolveComplaint(w http.ResponseWriter, r *http.Request) {
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
	if err := s.svc.ResolveComplaint(r.Context(), id, req.Resolution); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"resolved": id})
}

func (s *Server) handleUpdatePrice(w http.ResponseWriter, r *http.Request) {
	u := userOf(r)
	var req struct {
		Category  string  `json:"category"`
		PriceYuan float64 `json:"price_yuan_per_kg"`
	}
	if err := decode(r, &req); err != nil {
		writeErr(w, err)
		return
	}
	if err := s.svc.UpdateMarketPrice(r.Context(), req.Category, domain.YuanToCents(req.PriceYuan), u.Account); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"category": req.Category, "price_yuan_per_kg": req.PriceYuan})
}
