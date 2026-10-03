// Package httpapi 提供 REST API
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"
	"recycle/internal/domain"
	"recycle/internal/openapi"
	"recycle/internal/store"
	"recycle/internal/svc"
)

type Server struct {
	svc *svc.Service
	st  *store.Store
}

func NewServer(s *svc.Service, st *store.Store) *Server {
	return &Server{svc: s, st: st}
}

type ctxKey string

const ctxUser ctxKey = "user"

// ---- 响应与错误 ----

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, err error) {
	var ae *svc.ApiError
	if errors.As(err, &ae) {
		writeJSON(w, ae.Code, map[string]any{"error": ae.Message})
		return
	}
	if errors.Is(err, store.ErrNotFound) {
		writeJSON(w, 404, map[string]any{"error": "not found"})
		return
	}
	if errors.Is(err, store.ErrVersionConflict) {
		writeJSON(w, 409, map[string]any{"error": "concurrent modification, please retry"})
		return
	}
	writeJSON(w, 500, map[string]any{"error": err.Error()})
}

func decode(r *http.Request, v any) error {
	if r.Body == nil {
		return nil
	}
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(v); err != nil {
		return &svc.ApiError{Code: 400, Message: "invalid json: " + err.Error()}
	}
	return nil
}

func pathID(r *http.Request, name string) (int64, error) {
	id, err := strconv.ParseInt(chi.URLParam(r, name), 10, 64)
	if err != nil || id <= 0 {
		return 0, &svc.ApiError{Code: 400, Message: "invalid path id"}
	}
	return id, nil
}

func userOf(r *http.Request) *domain.User {
	u, _ := r.Context().Value(ctxUser).(*domain.User)
	return u
}

// ---- 中间件 ----

func (s *Server) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := r.Header.Get("Authorization")
		token := strings.TrimPrefix(h, "Bearer ")
		if token == "" || token == h {
			writeJSON(w, 401, map[string]any{"error": "missing bearer token"})
			return
		}
		u, err := store.UserByToken(r.Context(), s.st.Pool, token)
		if err != nil {
			writeJSON(w, 401, map[string]any{"error": "invalid token"})
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ctxUser, u)))
	})
}

func requireRole(role string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		u := userOf(r)
		if u == nil || u.Role != role {
			writeJSON(w, 403, map[string]any{"error": "require role " + role})
			return
		}
		next(w, r)
	}
}

// ---- 路由 ----

func (s *Server) Router() http.Handler {
	r := chi.NewRouter()
	// 公开 API 文档: /docs/(Swagger UI) 与 /openapi.json(OpenAPI 3 规范, 均无需鉴权)
	openapi.Register(r)
	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, 200, map[string]string{"status": "ok"})
	})
	r.Route("/api/v1", func(r chi.Router) {
		r.With(s.auth).Get("/me", s.handleMe)
		r.With(s.auth).Get("/prices", s.handlePrices)
		r.With(s.auth).Get("/notifications", s.handleNotifications)

		// 居民
		r.With(s.auth).Post("/orders", requireRole(domain.RoleResident, s.handleCreateOrder))
		r.With(s.auth).Get("/orders", requireRole(domain.RoleResident, s.handleMyOrders))
		r.With(s.auth).Get("/orders/{id}", s.handleGetOrder)
		r.With(s.auth).Get("/orders/{id}/trace", s.handleTrace)
		r.With(s.auth).Post("/orders/{id}/cancel", requireRole(domain.RoleResident, s.handleCancelOrder))
		r.With(s.auth).Post("/orders/{id}/confirm", requireRole(domain.RoleResident, s.handleConfirmWeigh))
		r.With(s.auth).Post("/orders/{id}/disputes", requireRole(domain.RoleResident, s.handleRaiseDispute))
		r.With(s.auth).Post("/orders/{id}/complaints", s.handleFileComplaint)
		r.With(s.auth).Get("/points/balance", requireRole(domain.RoleResident, s.handlePointsBalance))
		r.With(s.auth).Post("/points/claims", requireRole(domain.RoleResident, s.handlePointsClaim))
		r.With(s.auth).Get("/points/claims", requireRole(domain.RoleResident, s.handlePointsClaims))
		r.With(s.auth).Get("/points/reissues", requireRole(domain.RoleResident, s.handlePointsReissues))
		r.With(s.auth).Get("/certificates", requireRole(domain.RoleResident, s.handleMyCertificates))

		// 回收员
		r.With(s.auth).Post("/collector/duty", requireRole(domain.RoleCollector, s.handleDuty))
		r.With(s.auth).Get("/collector/orders", requireRole(domain.RoleCollector, s.handleCollectorOrders))
		r.With(s.auth).Get("/collector/tasks", requireRole(domain.RoleCollector, s.handleCollectorTasks))
		r.With(s.auth).Post("/collector/orders/{id}/accept", requireRole(domain.RoleCollector, s.handleAccept))
		r.With(s.auth).Post("/collector/orders/{id}/start", requireRole(domain.RoleCollector, s.handleStart))
		r.With(s.auth).Post("/collector/orders/{id}/arrive", requireRole(domain.RoleCollector, s.handleArrive))
		r.With(s.auth).Post("/collector/orders/{id}/verify-resident", requireRole(domain.RoleCollector, s.handleVerifyResident))
		r.With(s.auth).Post("/collector/orders/{id}/items", requireRole(domain.RoleCollector, s.handleAddItem))
		r.With(s.auth).Post("/collector/orders/{id}/weigh", requireRole(domain.RoleCollector, s.handleWeigh))
		r.With(s.auth).Post("/collector/orders/{id}/confirm-assisted", requireRole(domain.RoleCollector, s.handleConfirmAssisted))
		r.With(s.auth).Post("/collector/orders/{id}/settle", requireRole(domain.RoleCollector, s.handleSettle))
		r.With(s.auth).Post("/collector/orders/{id}/settle/retry", requireRole(domain.RoleCollector, s.handleSettleRetry))
		r.With(s.auth).Post("/collector/orders/{id}/deliver", requireRole(domain.RoleCollector, s.handleDeliver))
		r.With(s.auth).Get("/collector/earnings", requireRole(domain.RoleCollector, s.handleEarnings))

		// 站点
		r.With(s.auth).Post("/station/inbound/{orderId}/reweigh", requireRole(domain.RoleStation, s.handleReweigh))
		r.With(s.auth).Get("/station/inbound", requireRole(domain.RoleStation, s.handleStationInbound))
		r.With(s.auth).Get("/station/quality", requireRole(domain.RoleStation, s.handleStationQuality))

		// 街道/管理
		r.With(s.auth).Get("/street/stats", requireRole(domain.RoleStreet, s.handleStreetStats))
		r.With(s.auth).Get("/street/flows", requireRole(domain.RoleStreet, s.handleStreetFlows))
		r.With(s.auth).Get("/street/disputes", requireRole(domain.RoleStreet, s.handleListDisputes))
		r.With(s.auth).Post("/street/disputes/{id}/resolve", requireRole(domain.RoleStreet, s.handleResolveDispute))
		r.With(s.auth).Get("/street/complaints", requireRole(domain.RoleStreet, s.handleListComplaints))
		r.With(s.auth).Post("/street/complaints/{id}/resolve", requireRole(domain.RoleStreet, s.handleResolveComplaint))
		r.With(s.auth).Post("/street/prices", requireRole(domain.RoleStreet, s.handleUpdatePrice))

		// 运营(街道): 积分到账异常核对/补发/技术排查/受影响居民名单
		r.With(s.auth).Post("/street/points/reconcile", requireRole(domain.RoleStreet, s.handleStreetReconcile))
		r.With(s.auth).Get("/street/orders/{id}/points-check", requireRole(domain.RoleStreet, s.handleStreetOrderCheck))
		r.With(s.auth).Get("/street/tech-incidents", requireRole(domain.RoleStreet, s.handleStreetIncidents))
		r.With(s.auth).Get("/street/callback-affected", requireRole(domain.RoleStreet, s.handleStreetAffectedResidents))
	})
	return r
}
