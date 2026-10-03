package svc

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"recycle/internal/domain"
	"recycle/internal/store"
)

// loadCollectorOrder 行锁读取订单并校验归属该回收员
func (s *Service) loadCollectorOrder(ctx context.Context, tx pgx.Tx, collectorID, orderID int64) (*domain.Order, error) {
	o, err := store.OrderByIDForUpdate(ctx, tx, orderID)
	if err != nil {
		return nil, err
	}
	if o.CollectorID == nil || *o.CollectorID != collectorID {
		return nil, forbidden("order not assigned to you")
	}
	return o, nil
}

// CollectorAccept 接单
func (s *Service) CollectorAccept(ctx context.Context, collectorID, orderID int64) (*domain.Order, error) {
	return s.collectorSimpleTransit(ctx, collectorID, orderID, domain.StatusAccepted, nil)
}

// CollectorStart 出发
func (s *Service) CollectorStart(ctx context.Context, collectorID, orderID int64) (*domain.Order, error) {
	return s.collectorSimpleTransit(ctx, collectorID, orderID, domain.StatusEnRoute, nil)
}

// CollectorArrive 到达(记录迟到分钟数)
func (s *Service) CollectorArrive(ctx context.Context, collectorID, orderID int64) (*domain.Order, error) {
	var o *domain.Order
	err := s.Store.InTx(ctx, func(tx pgx.Tx) error {
		var err error
		o, err = s.loadCollectorOrder(ctx, tx, collectorID, orderID)
		if err != nil {
			return err
		}
		late := 0
		if time.Now().After(o.TimeWindowEnd) {
			late = int(time.Since(o.TimeWindowEnd).Minutes())
		}
		return s.transit(ctx, tx, o, domain.StatusArrived, domain.RoleCollector, collectorID,
			map[string]any{"late_minutes": late}, map[string]any{"late_minutes": late})
	})
	return o, err
}

func (s *Service) collectorSimpleTransit(ctx context.Context, collectorID, orderID int64, to string, detail map[string]any) (*domain.Order, error) {
	var o *domain.Order
	err := s.Store.InTx(ctx, func(tx pgx.Tx) error {
		var err error
		o, err = s.loadCollectorOrder(ctx, tx, collectorID, orderID)
		if err != nil {
			return err
		}
		return s.transit(ctx, tx, o, to, domain.RoleCollector, collectorID, detail, nil)
	})
	return o, err
}

// VerifyResident 上门核验居民: 核验码(SCAN_CODE) 或 协助确认(ASSISTED, 限老人)
func (s *Service) VerifyResident(ctx context.Context, collectorID, orderID int64,
	method, verifyCode, assistedNote string) (*domain.Order, error) {

	var o *domain.Order
	err := s.Store.InTx(ctx, func(tx pgx.Tx) error {
		var err error
		o, err = s.loadCollectorOrder(ctx, tx, collectorID, orderID)
		if err != nil {
			return err
		}
		if o.Status != domain.StatusArrived {
			return conflict("order %s in status %s, expect ARRIVED", o.OrderNo, o.Status)
		}
		switch method {
		case domain.MethodScanCode:
			if verifyCode == "" || verifyCode != o.VerifyCode {
				return badReq("verify_code mismatch")
			}
		case domain.MethodAssisted:
			// 老人不会操作: 回收员协助确认, 必须留痕
			res, err := store.ResidentByID(ctx, tx, o.ResidentID)
			if err != nil {
				return err
			}
			if !res.IsElderly {
				return badReq("assisted confirm only allowed for elderly residents")
			}
			if assistedNote == "" {
				return badReq("assisted_note is required for assisted confirm")
			}
		default:
			return badReq("method must be SCAN_CODE or ASSISTED")
		}
		if err := store.AddConfirmation(ctx, tx, &domain.Confirmation{
			OrderID: o.ID, Kind: "RESIDENT_VERIFY", ActorRole: domain.RoleCollector,
			ActorID: collectorID, Method: method,
			Payload: map[string]any{"assisted_note": assistedNote},
		}); err != nil {
			return err
		}
		return s.transit(ctx, tx, o, domain.StatusResidentVerified, domain.RoleCollector, collectorID,
			map[string]any{"method": method}, nil)
	})
	return o, err
}

// AddOnsiteItem 居民临时加品类(到达后、称重前)
func (s *Service) AddOnsiteItem(ctx context.Context, collectorID, orderID int64,
	category string, estWeightKg float64) (*domain.OrderItem, error) {

	if estWeightKg < 0 || estWeightKg > 10000 {
		return nil, badReq("invalid est_weight_kg")
	}
	ok, err := store.CategoryExists(ctx, s.Store.Pool, category)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, badReq("unknown category %s", category)
	}

	var item *domain.OrderItem
	err = s.Store.InTx(ctx, func(tx pgx.Tx) error {
		o, err := s.loadCollectorOrder(ctx, tx, collectorID, orderID)
		if err != nil {
			return err
		}
		if o.Status != domain.StatusArrived && o.Status != domain.StatusResidentVerified {
			return conflict("onsite item can only be added before weighing (status %s)", o.Status)
		}
		// 同品类去重
		existing, err := store.ListOrderItems(ctx, tx, o.ID)
		if err != nil {
			return err
		}
		for _, it := range existing {
			if it.Category == category {
				return conflict("category %s already in order", category)
			}
		}
		// 现场价 = 当前市场价快照
		price, err := s.currentPrice(ctx, category)
		if err != nil {
			return badReq("no market price for category %s", category)
		}
		weightG := domain.KgToGrams(estWeightKg)
		item = &domain.OrderItem{
			OrderID: o.ID, Category: category, Source: domain.SourceOnsiteAdded,
			EstWeightG: weightG, EstPriceCents: price,
			EstCents: domain.AmountForWeight(weightG, price),
		}
		if err := store.CreateOrderItem(ctx, tx, item); err != nil {
			return err
		}
		// 事件(不变状态)
		ev := &domain.OrderEvent{
			OrderID: o.ID, FromStatus: o.Status, ToStatus: o.Status,
			ActorRole: domain.RoleCollector, ActorID: collectorID,
			Detail: map[string]any{"kind": "onsite_item_added", "category": category,
				"est_weight_g": weightG, "price_cents": price},
		}
		if err := store.AddOrderEvent(ctx, tx, ev); err != nil {
			return err
		}
		return store.AddOutbox(ctx, tx, domain.TopicOrderEvents, "order:"+itoa64(o.ID), map[string]any{
			"event_id": ev.ID, "order_id": o.ID, "order_no": o.OrderNo,
			"from": o.Status, "to": o.Status, "actor_role": domain.RoleCollector,
			"actor_id": collectorID, "resident_id": o.ResidentID, "collector_id": o.CollectorID,
			"street": o.Street, "detail": ev.Detail, "at": time.Now().UTC(),
		})
	})
	return item, err
}

// WeighItem 现场称重行
type WeighItem struct {
	Category        string
	WeightKg        float64
	ContaminationPM int // 混入不可回收物扣杂千分比
}

// Weigh 现场称重定价: 逐品类称重+扣杂, 用锁定单价计算现场金额
func (s *Service) Weigh(ctx context.Context, collectorID, orderID int64,
	weights []WeighItem, photos []string) (*domain.Order, []*domain.OrderItem, error) {

	if len(weights) == 0 {
		return nil, nil, badReq("weights required")
	}
	var o *domain.Order
	var items []*domain.OrderItem
	err := s.Store.InTx(ctx, func(tx pgx.Tx) error {
		var err error
		o, err = s.loadCollectorOrder(ctx, tx, collectorID, orderID)
		if err != nil {
			return err
		}
		if o.Status != domain.StatusResidentVerified && o.Status != domain.StatusWeighed {
			return conflict("order %s in status %s, expect RESIDENT_VERIFIED", o.OrderNo, o.Status)
		}
		items, err = store.ListOrderItems(ctx, tx, o.ID)
		if err != nil {
			return err
		}
		byCat := map[string]*domain.OrderItem{}
		for _, it := range items {
			byCat[it.Category] = it
		}
		var total int64
		detailItems := []map[string]any{}
		for _, w := range weights {
			it, ok := byCat[w.Category]
			if !ok {
				return badReq("category %s not in order", w.Category)
			}
			if w.WeightKg < 0 || w.WeightKg > 10000 {
				return badReq("invalid weight for %s", w.Category)
			}
			if w.ContaminationPM < 0 || w.ContaminationPM >= 1000 {
				return badReq("invalid contamination_pm for %s", w.Category)
			}
			weightG := domain.KgToGrams(w.WeightKg)
			effG := domain.EffectiveWeightG(weightG, w.ContaminationPM)
			amount := domain.AmountForWeight(effG, it.EstPriceCents)
			// 更新明细行
			if _, err := tx.Exec(ctx,
				`UPDATE order_items SET onsite_weight_g=$2, contamination_pm=$3,
				 onsite_price_cents=$4, onsite_cents=$5 WHERE id=$1`,
				it.ID, weightG, w.ContaminationPM, it.EstPriceCents, amount); err != nil {
				return err
			}
			wg, pc := weightG, it.EstPriceCents
			am := amount
			it.OnsiteWeightG, it.OnsitePriceCents, it.OnsiteCents = &wg, &pc, &am
			it.ContaminationPM = w.ContaminationPM
			total += amount
			detailItems = append(detailItems, map[string]any{
				"category": w.Category, "weight_g": weightG,
				"contamination_pm": w.ContaminationPM, "amount_cents": amount,
			})
		}
		// 未称重的品类按 0 处理(居民临时不出售)
		for _, it := range items {
			if it.OnsiteWeightG == nil {
				zero := int64(0)
				zeroAmt := int64(0)
				if _, err := tx.Exec(ctx,
					`UPDATE order_items SET onsite_weight_g=0, onsite_price_cents=$2, onsite_cents=0 WHERE id=$1`,
					it.ID, it.EstPriceCents); err != nil {
					return err
				}
				it.OnsiteWeightG, it.OnsiteCents = &zero, &zeroAmt
			}
		}
		sets := map[string]any{"onsite_cents": total}
		if len(photos) > 0 {
			sets["photos"] = photosJSON(append(o.Photos, photos...))
		}
		if err := s.transit(ctx, tx, o, domain.StatusWeighed, domain.RoleCollector, collectorID,
			map[string]any{"onsite_cents": total, "items": detailItems}, sets); err != nil {
			return err
		}
		o.OnsiteCents = total
		return nil
	})
	return o, items, err
}

// ConfirmWeigh 居民确认称重与价格; method=SELF(本人) 或 ASSISTED(回收员协助, 限老人)
func (s *Service) ConfirmWeigh(ctx context.Context, orderID int64,
	actorRole string, actorID int64, method, assistedNote string) (*domain.Order, error) {

	var o *domain.Order
	err := s.Store.InTx(ctx, func(tx pgx.Tx) error {
		var err error
		o, err = store.OrderByIDForUpdate(ctx, tx, orderID)
		if err != nil {
			return err
		}
		if o.Status != domain.StatusWeighed {
			return conflict("order %s in status %s, expect WEIGHED", o.OrderNo, o.Status)
		}
		switch method {
		case domain.MethodSelf:
			if actorRole != domain.RoleResident || actorID != o.ResidentID {
				return forbidden("only the resident can self-confirm")
			}
		case domain.MethodAssisted:
			if actorRole != domain.RoleCollector || o.CollectorID == nil || actorID != *o.CollectorID {
				return forbidden("only the assigned collector can assist")
			}
			res, err := store.ResidentByID(ctx, tx, o.ResidentID)
			if err != nil {
				return err
			}
			if !res.IsElderly {
				return badReq("assisted confirm only allowed for elderly residents")
			}
			if assistedNote == "" {
				return badReq("assisted_note is required")
			}
		default:
			return badReq("method must be SELF or ASSISTED")
		}
		if err := store.AddConfirmation(ctx, tx, &domain.Confirmation{
			OrderID: o.ID, Kind: "WEIGH_CONFIRM", ActorRole: actorRole, ActorID: actorID,
			Method: method, Payload: map[string]any{"assisted_note": assistedNote},
		}); err != nil {
			return err
		}
		return s.transit(ctx, tx, o, domain.StatusConfirmed, actorRole, actorID,
			map[string]any{"method": method}, nil)
	})
	return o, err
}

// RaiseDispute 居民对称重/价格提出争议: 订单冻结
func (s *Service) RaiseDispute(ctx context.Context, residentID, orderID int64,
	kind, detail string, itemID *int64) (*domain.Dispute, error) {

	var d *domain.Dispute
	err := s.Store.InTx(ctx, func(tx pgx.Tx) error {
		o, err := store.OrderByIDForUpdate(ctx, tx, orderID)
		if err != nil {
			return err
		}
		if o.ResidentID != residentID {
			return forbidden("not your order")
		}
		if o.Status != domain.StatusWeighed {
			return conflict("dispute can only be raised after weighing (status %s)", o.Status)
		}
		d = &domain.Dispute{
			OrderID: orderID, ItemID: itemID, RaisedBy: domain.RoleResident,
			Kind: kind, Detail: detail, Status: "OPEN",
		}
		if err := store.CreateDispute(ctx, tx, d); err != nil {
			return err
		}
		return s.transit(ctx, tx, o, domain.StatusDisputed, domain.RoleResident, residentID,
			map[string]any{"dispute_id": d.ID, "kind": kind}, nil)
	})
	return d, err
}

// Settle 结算: cash(同步) 或 points(异步, 经 Kafka 积分消费者入账)
// simulateCashFail 为测试注入: 模拟现金支付失败
func (s *Service) Settle(ctx context.Context, collectorID, orderID int64,
	settleType string, simulateCashFail bool) (*domain.Settlement, *domain.Order, error) {

	if settleType != domain.SettleCash && settleType != domain.SettlePoints {
		return nil, nil, badReq("type must be cash or points")
	}
	var st *domain.Settlement
	var o *domain.Order
	err := s.Store.InTx(ctx, func(tx pgx.Tx) error {
		var err error
		o, err = s.loadCollectorOrder(ctx, tx, collectorID, orderID)
		if err != nil {
			return err
		}
		if o.Status != domain.StatusConfirmed {
			return conflict("order %s in status %s, expect CONFIRMED", o.OrderNo, o.Status)
		}
		// 防重复结算
		if existing, err := store.LatestSettlement(ctx, tx, orderID); err == nil {
			if existing.Status == domain.SettleSuccess {
				return conflict("order already settled")
			}
			if existing.Status == domain.SettlePending {
				return conflict("settlement already in progress")
			}
		}
		points := int64(0)
		if settleType == domain.SettlePoints {
			points = domain.PointsForAmount(o.OnsiteCents, s.Cfg.PointsPerYuan, s.Cfg.PointsBonusPM)
		}
		st = &domain.Settlement{
			OrderID: orderID, Type: settleType, AmountCents: o.OnsiteCents,
			Points: points, Status: domain.SettlePending,
			IdempotencyKey: fmt.Sprintf("settle:order:%d", orderID),
		}
		if err := store.CreateSettlement(ctx, tx, st); err != nil {
			if store.IsUniqueViolation(err) {
				return conflict("settlement already exists")
			}
			return err
		}
		if err := store.AddConfirmation(ctx, tx, &domain.Confirmation{
			OrderID: orderID, Kind: "SETTLE_CONFIRM", ActorRole: domain.RoleCollector,
			ActorID: collectorID, Method: domain.MethodScanCode,
			Payload: map[string]any{"type": settleType, "amount_cents": o.OnsiteCents, "points": points},
		}); err != nil {
			return err
		}

		if settleType == domain.SettleCash {
			// 现金: 同步处理
			if simulateCashFail {
				if err := store.MarkSettlementFailed(ctx, tx, st.ID, "cash payment failed (simulated)"); err != nil {
					return err
				}
				st.Status = domain.SettleFailed
				return nil // 订单停留 CONFIRMED, 可重试
			}
			if err := store.MarkSettlementSuccess(ctx, tx, st.ID); err != nil {
				return err
			}
			st.Status = domain.SettleSuccess
			if err := s.createEarning(ctx, tx, o); err != nil {
				return err
			}
			if err := s.ResolveSubmissionTaskOnCash(ctx, tx, o); err != nil {
				return err
			}
			return s.transit(ctx, tx, o, domain.StatusSettled, domain.RoleCollector, collectorID,
				map[string]any{"type": settleType, "amount_cents": o.OnsiteCents},
				map[string]any{"settlement_type": settleType})
		}
		// 积分: 发指令给积分消费者, 入账后由消费者推进 SETTLED
		return store.AddOutbox(ctx, tx, domain.TopicPointsCommands, fmt.Sprintf("settlement:%d", st.ID), map[string]any{
			"command": "credit", "settlement_id": st.ID, "order_id": orderID,
			"resident_id": o.ResidentID, "amount_cents": o.OnsiteCents, "points": points,
		})
	})
	return st, o, err
}

// RetryCashSettlement 现金支付失败后重试
func (s *Service) RetryCashSettlement(ctx context.Context, collectorID, orderID int64, simulateCashFail bool) (*domain.Settlement, *domain.Order, error) {
	var st *domain.Settlement
	var o *domain.Order
	err := s.Store.InTx(ctx, func(tx pgx.Tx) error {
		var err error
		o, err = s.loadCollectorOrder(ctx, tx, collectorID, orderID)
		if err != nil {
			return err
		}
		if o.Status != domain.StatusConfirmed {
			return conflict("order %s in status %s, expect CONFIRMED", o.OrderNo, o.Status)
		}
		st, err = store.LatestSettlement(ctx, tx, orderID)
		if err != nil {
			return err
		}
		if st.Type != domain.SettleCash || st.Status != domain.SettleFailed {
			return conflict("only failed cash settlement can be retried")
		}
		if simulateCashFail {
			if _, err := tx.Exec(ctx,
				`UPDATE settlements SET fail_reason='cash payment failed (simulated)', retry_count=retry_count+1, updated_at=now() WHERE id=$1`,
				st.ID); err != nil {
				return err
			}
			st.RetryCount++
			return nil
		}
		if _, err := tx.Exec(ctx,
			`UPDATE settlements SET status='SUCCESS', fail_reason=NULL, retry_count=retry_count+1, updated_at=now() WHERE id=$1`,
			st.ID); err != nil {
			return err
		}
		st.Status = domain.SettleSuccess
		st.RetryCount++
		if err := s.createEarning(ctx, tx, o); err != nil {
			return err
		}
		if err := s.ResolveSubmissionTaskOnCash(ctx, tx, o); err != nil {
			return err
		}
		return s.transit(ctx, tx, o, domain.StatusSettled, domain.RoleCollector, collectorID,
			map[string]any{"type": st.Type, "amount_cents": st.AmountCents, "retry": true},
			map[string]any{"settlement_type": st.Type})
	})
	return st, o, err
}

// createEarning 结算成功时生成回收员收益(基础+重量+楼层+距离)
func (s *Service) createEarning(ctx context.Context, tx pgx.Tx, o *domain.Order) error {
	if o.CollectorID == nil {
		return nil
	}
	items, err := store.ListOrderItems(ctx, tx, o.ID)
	if err != nil {
		return err
	}
	var totalWeight int64
	for _, it := range items {
		if it.OnsiteWeightG != nil {
			totalWeight += domain.EffectiveWeightG(*it.OnsiteWeightG, it.ContaminationPM)
		}
	}
	c, err := store.CollectorByID(ctx, tx, *o.CollectorID)
	if err != nil {
		return err
	}
	dist := domain.HaversineKm(c.Lat, c.Lng, o.Lat, o.Lng)
	base, weight, floorB, distB, total := domain.ComputeCollectorEarning(totalWeight, o.Floor, o.HasElevator, dist)
	return store.CreateEarning(ctx, tx, &domain.CollectorEarning{
		CollectorID: *o.CollectorID, OrderID: o.ID,
		BaseCents: base, WeightCents: weight, FloorCents: floorB, DistanceCents: distB,
		TotalCents: total,
	})
}

// Deliver 回收员将回收物送达站点
func (s *Service) Deliver(ctx context.Context, collectorID, orderID int64) (*domain.Order, error) {
	var o *domain.Order
	err := s.Store.InTx(ctx, func(tx pgx.Tx) error {
		var err error
		o, err = s.loadCollectorOrder(ctx, tx, collectorID, orderID)
		if err != nil {
			return err
		}
		return s.transit(ctx, tx, o, domain.StatusDelivered, domain.RoleCollector, collectorID, nil, nil)
	})
	return o, err
}

func photosJSON(photos []string) any {
	b, _ := json.Marshal(photos)
	return string(b)
}
