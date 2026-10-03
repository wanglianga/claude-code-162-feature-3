package svc

import (
	"context"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"recycle/internal/domain"
	"recycle/internal/store"
)

// CreateOrderItem 预约提交的品类行
type CreateOrderItem struct {
	Category    string  `json:"category"`
	EstWeightKg float64 `json:"est_weight_kg"`
}

type CreateOrderReq struct {
	Items          []CreateOrderItem
	Address        string
	Lat, Lng       float64
	Floor          int
	HasElevator    bool
	WindowStart    time.Time
	WindowEnd      time.Time
	Photos         []string
	IdempotencyKey string
}

// CreateOrder 居民提交回收预约: 校验 -> 市场价快照 -> 预估价 -> 落库 -> 触发派单
func (s *Service) CreateOrder(ctx context.Context, residentID int64, req CreateOrderReq) (*domain.Order, []*domain.OrderItem, error) {
	if len(req.Items) == 0 {
		return nil, nil, badReq("at least one category item is required")
	}
	if !req.WindowEnd.After(req.WindowStart) {
		return nil, nil, badReq("time_window_end must be after time_window_start")
	}
	if req.WindowEnd.Before(time.Now().Add(-time.Hour)) {
		return nil, nil, badReq("time window already passed")
	}
	seen := map[string]bool{}
	for _, it := range req.Items {
		if it.EstWeightKg <= 0 || it.EstWeightKg > 10000 {
			return nil, nil, badReq("invalid est_weight_kg for category %s", it.Category)
		}
		if seen[it.Category] {
			return nil, nil, badReq("duplicate category %s", it.Category)
		}
		seen[it.Category] = true
		ok, err := store.CategoryExists(ctx, s.Store.Pool, it.Category)
		if err != nil {
			return nil, nil, err
		}
		if !ok {
			return nil, nil, badReq("unknown category %s", it.Category)
		}
	}

	res, err := store.ResidentByID(ctx, s.Store.Pool, residentID)
	if err != nil {
		return nil, nil, notFound("resident")
	}
	// 地址缺省取居民档案
	address, lat, lng := req.Address, req.Lat, req.Lng
	if address == "" {
		address = res.Address
	}
	if lat == 0 && lng == 0 {
		lat, lng = res.Lat, res.Lng
	}
	floor, elevator := req.Floor, req.HasElevator
	if floor == 0 {
		floor = res.Floor
		elevator = res.HasElevator
	}

	// 市场价快照 + 预估价
	items := make([]*domain.OrderItem, 0, len(req.Items))
	var estimate int64
	for _, it := range req.Items {
		price, err := s.currentPrice(ctx, it.Category)
		if err != nil {
			return nil, nil, badReq("no market price for category %s", it.Category)
		}
		weightG := domain.KgToGrams(it.EstWeightKg)
		amount := domain.AmountForWeight(weightG, price)
		items = append(items, &domain.OrderItem{
			Category: it.Category, Source: domain.SourceBooking,
			EstWeightG: weightG, EstPriceCents: price, EstCents: amount,
		})
		estimate += amount
	}

	o := &domain.Order{
		OrderNo:         orderNo(),
		ResidentID:      residentID,
		Status:          domain.StatusPending,
		Street:          res.Street,
		Address:         address,
		Lat:             lat,
		Lng:             lng,
		Floor:           floor,
		HasElevator:     elevator,
		TimeWindowStart: req.WindowStart,
		TimeWindowEnd:   req.WindowEnd,
		Photos:          req.Photos,
		VerifyCode:      randDigits(6),
		EstimateCents:   estimate,
	}
	if o.Photos == nil {
		o.Photos = []string{}
	}

	idemKey := req.IdempotencyKey
	if idemKey == "" {
		idemKey = "auto:" + o.OrderNo
	}

	created := true
	err = s.Store.InTx(ctx, func(tx pgx.Tx) error {
		var err error
		created, err = store.CreateOrderIdempotent(ctx, tx, o, idemKey)
		if err != nil {
			return err
		}
		if !created {
			return nil // 幂等命中: 直接返回已有订单
		}
		for _, it := range items {
			it.OrderID = o.ID
			if err := store.CreateOrderItem(ctx, tx, it); err != nil {
				return err
			}
		}
		ev := &domain.OrderEvent{
			OrderID: o.ID, FromStatus: "", ToStatus: domain.StatusPending,
			ActorRole: domain.RoleResident, ActorID: residentID,
			Detail: map[string]any{"estimate_cents": estimate, "items": len(items)},
		}
		if err := store.AddOrderEvent(ctx, tx, ev); err != nil {
			return err
		}
		return store.AddOutbox(ctx, tx, domain.TopicOrderEvents, "order:"+itoa64(o.ID), map[string]any{
			"event_id": ev.ID, "order_id": o.ID, "order_no": o.OrderNo,
			"from": "", "to": domain.StatusPending, "actor_role": domain.RoleResident,
			"actor_id": residentID, "resident_id": residentID, "street": o.Street,
			"detail": ev.Detail, "at": time.Now().UTC(),
		})
	})
	if err != nil {
		return nil, nil, err
	}
	if !created {
		existing, err := store.ListOrderItems(ctx, s.Store.Pool, o.ID)
		return o, existing, err
	}

	// 提交后尝试立即派单(失败则由定时器重试)
	_ = s.DispatchOrder(ctx, o.ID)
	return o, items, nil
}

// CancelOrder 居民取消预约
func (s *Service) CancelOrder(ctx context.Context, residentID, orderID int64, reason string) (*domain.Order, error) {
	var o *domain.Order
	err := s.Store.InTx(ctx, func(tx pgx.Tx) error {
		var err error
		o, err = store.OrderByIDForUpdate(ctx, tx, orderID)
		if err != nil {
			return err
		}
		if o.ResidentID != residentID {
			return forbidden("not your order")
		}
		if !domain.CancellableByResident(o.Status) {
			return conflict("order %s in status %s cannot be cancelled", o.OrderNo, o.Status)
		}
		return s.transit(ctx, tx, o, domain.StatusCancelled, domain.RoleResident, residentID,
			map[string]any{"reason": reason}, map[string]any{"cancel_reason": reason})
	})
	return o, err
}

// currentPrice 带 Redis 缓存的市场价
func (s *Service) currentPrice(ctx context.Context, category string) (int64, error) {
	cacheKey := "cache:price:" + category
	if v, err := s.Redis.Get(ctx, cacheKey).Int64(); err == nil && v > 0 {
		return v, nil
	}
	price, err := store.CurrentPrice(ctx, s.Store.Pool, category)
	if err != nil {
		return 0, err
	}
	s.Redis.Set(ctx, cacheKey, price, time.Minute)
	return price, nil
}

func itoa64(v int64) string {
	return strconv.FormatInt(v, 10)
}
