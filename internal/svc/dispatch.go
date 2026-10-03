package svc

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"
	"recycle/internal/domain"
	"recycle/internal/store"
)

const geoKeyCollectors = "geo:collectors"

// UpdateCollectorDuty 上下班并维护 GEO 索引
func (s *Service) UpdateCollectorDuty(ctx context.Context, collectorID int64, onDuty bool, lat, lng float64) error {
	if err := store.SetCollectorDuty(ctx, s.Store.Pool, collectorID, onDuty, lat, lng); err != nil {
		return err
	}
	if onDuty {
		return s.Redis.GeoAdd(ctx, geoKeyCollectors, &redis.GeoLocation{
			Name: itoa64(collectorID), Longitude: lng, Latitude: lat,
		}).Err()
	}
	return s.Redis.ZRem(ctx, geoKeyCollectors, itoa64(collectorID)).Err()
}

// DispatchOrder 为一单派单: GEO 就近 + 排班容量 + 在途并发约束
func (s *Service) DispatchOrder(ctx context.Context, orderID int64) error {
	// Redis 分布式锁, 防止定时器与即时派单并发重复派单
	lockKey := "lock:dispatch:" + itoa64(orderID)
	ok, err := s.Redis.SetNX(ctx, lockKey, 1, 30*time.Second).Result()
	if err != nil || !ok {
		return err
	}
	defer s.Redis.Del(ctx, lockKey)

	o, err := store.OrderByID(ctx, s.Store.Pool, orderID)
	if err != nil {
		return err
	}
	if o.Status != domain.StatusPending {
		return nil
	}

	// 就近候选回收员
	locs, err := s.Redis.GeoSearch(ctx, geoKeyCollectors, &redis.GeoSearchQuery{
		Longitude:  o.Lng,
		Latitude:   o.Lat,
		Radius:     s.Cfg.DispatchRadiusKm,
		RadiusUnit: "km",
		Sort:       "ASC",
		Count:      20,
	}).Result()
	if err != nil {
		return err
	}

	for _, loc := range locs {
		var cid int64
		if _, err := fmt.Sscan(loc, &cid); err != nil {
			continue
		}
		c, err := store.CollectorByID(ctx, s.Store.Pool, cid)
		if err != nil || !c.OnDuty {
			continue
		}
		// 排班覆盖预约窗口开始时间且班次容量未满
		scheduled, err := store.CollectorScheduled(ctx, s.Store.Pool, cid, o.TimeWindowStart)
		if err != nil || !scheduled {
			continue
		}
		// 在途并发上限
		active, err := store.CountActiveByCollector(ctx, s.Store.Pool, cid)
		if err != nil || active >= c.MaxConcurrent {
			continue
		}
		// 派单: 事务内推进状态
		err = s.Store.InTx(ctx, func(tx pgx.Tx) error {
			locked, err := store.OrderByIDForUpdate(ctx, tx, orderID)
			if err != nil {
				return err
			}
			if locked.Status != domain.StatusPending {
				return nil // 已被并发处理
			}
			*o = *locked
			dist := domain.HaversineKm(c.Lat, c.Lng, o.Lat, o.Lng)
			return s.transit(ctx, tx, o, domain.StatusAssigned, domain.RoleSystem, 0,
				map[string]any{"collector_id": cid, "station_id": c.StationID, "distance_km": dist},
				map[string]any{"collector_id": cid, "station_id": c.StationID})
		})
		if err != nil {
			return err
		}
		if o.Status == domain.StatusAssigned {
			return nil
		}
	}
	return nil // 无可用回收员, 保持 PENDING 等待重试
}

// DispatchPending 定时器: 重试所有待派单
func (s *Service) DispatchPending(ctx context.Context) (int, error) {
	orders, err := store.ListPendingOrders(ctx, s.Store.Pool, 50)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, o := range orders {
		if err := s.DispatchOrder(ctx, o.ID); err == nil {
			n++
		}
	}
	return n, nil
}

// DetectLate 定时器: 超过预约窗口+宽限仍未到达 -> 记迟到事件并通知居民
func (s *Service) DetectLate(ctx context.Context) (int, error) {
	candidates, err := store.ListLateCandidates(ctx, s.Store.Pool)
	if err != nil {
		return 0, err
	}
	n := 0
	grace := time.Duration(s.Cfg.LateGraceMinutes) * time.Minute
	for _, o := range candidates {
		lateBy := time.Since(o.TimeWindowEnd.Add(grace))
		if lateBy < 0 {
			continue
		}
		lateMinutes := int(time.Since(o.TimeWindowEnd).Minutes())
		err := s.Store.InTx(ctx, func(tx pgx.Tx) error {
			locked, err := store.OrderByIDForUpdate(ctx, tx, o.ID)
			if err != nil {
				return err
			}
			if locked.LateMinutes > 0 {
				return nil // 已标记过
			}
			// 不改状态, 仅记录迟到事件 + late_minutes
			if _, err := tx.Exec(ctx,
				`UPDATE orders SET late_minutes=$2, version=version+1, updated_at=now() WHERE id=$1`,
				locked.ID, lateMinutes); err != nil {
				return err
			}
			ev := &domain.OrderEvent{
				OrderID: locked.ID, FromStatus: locked.Status, ToStatus: locked.Status,
				ActorRole: domain.RoleSystem, ActorID: 0,
				Detail: map[string]any{"kind": "ORDER_LATE", "late_minutes": lateMinutes},
			}
			if err := store.AddOrderEvent(ctx, tx, ev); err != nil {
				return err
			}
			if err := store.AddOutbox(ctx, tx, domain.TopicOrderEvents, "order:"+itoa64(locked.ID), map[string]any{
				"event_id": ev.ID, "order_id": locked.ID, "order_no": locked.OrderNo,
				"from": locked.Status, "to": locked.Status, "actor_role": domain.RoleSystem,
				"resident_id": locked.ResidentID, "collector_id": locked.CollectorID,
				"street": locked.Street, "detail": ev.Detail, "at": time.Now().UTC(),
			}); err != nil {
				return err
			}
			return s.notify(ctx, tx, locked.ResidentID, "ORDER_LATE",
				"您的回收预约 "+locked.OrderNo+" 回收员已迟到 "+itoa64(int64(lateMinutes))+" 分钟, 可选择继续等待或取消重约")
		})
		if err == nil {
			n++
		}
	}
	return n, nil
}
