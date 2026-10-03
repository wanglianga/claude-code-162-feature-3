package svc

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"recycle/internal/domain"
	"recycle/internal/store"
)

// ReweighItem 站点复称行
type ReweighItem struct {
	Category string
	WeightKg float64
}

// StationReweigh 站点二次称重:
//   - 逐品类复称, 与现场称重比对, 超差标记 OVER_TOLERANCE
//   - 生成入库记录, 重算订单最终金额
//   - 超差时: 扣减回收员收益; 积分结算单生成积分差额调整(可追)
//   - 按品类开具环保凭证, 订单完结
func (s *Service) StationReweigh(ctx context.Context, stationUserID, orderID int64,
	weights []ReweighItem) (*domain.Order, []*domain.OrderItem, error) {

	station, err := store.StationByUserID(ctx, s.Store.Pool, stationUserID)
	if err != nil {
		return nil, nil, err
	}
	var o *domain.Order
	var items []*domain.OrderItem
	err = s.Store.InTx(ctx, func(tx pgx.Tx) error {
		var err error
		o, err = store.OrderByIDForUpdate(ctx, tx, orderID)
		if err != nil {
			return err
		}
		if o.Status != domain.StatusDelivered {
			return conflict("order %s in status %s, expect DELIVERED", o.OrderNo, o.Status)
		}
		if o.StationID == nil || *o.StationID != station.ID {
			return forbidden("order not delivered to your station")
		}
		items, err = store.ListOrderItems(ctx, tx, o.ID)
		if err != nil {
			return err
		}
		byCat := map[string]*domain.OrderItem{}
		for _, it := range items {
			byCat[it.Category] = it
		}
		seen := map[string]bool{}
		var finalTotal, totalVariance int64
		overTolerance := false
		varianceDetails := []map[string]any{}

		for _, w := range weights {
			it, ok := byCat[w.Category]
			if !ok {
				return badReq("category %s not in order", w.Category)
			}
			if w.WeightKg < 0 || w.WeightKg > 10000 {
				return badReq("invalid weight for %s", w.Category)
			}
			seen[w.Category] = true
			stationG := domain.KgToGrams(w.WeightKg)
			onsiteG := int64(0)
			if it.OnsiteWeightG != nil {
				onsiteG = *it.OnsiteWeightG
			}
			// 复称金额沿用现场扣杂率与锁定单价
			effG := domain.EffectiveWeightG(stationG, it.ContaminationPM)
			price := it.EstPriceCents
			if it.OnsitePriceCents != nil {
				price = *it.OnsitePriceCents
			}
			stationCents := domain.AmountForWeight(effG, price)
			variance := stationG - onsiteG
			vStatus := domain.VarianceOK
			if domain.IsVarianceOverTolerance(onsiteG, stationG) {
				vStatus = domain.VarianceOverTolerance
				overTolerance = true
			}
			if _, err := tx.Exec(ctx,
				`UPDATE order_items SET station_weight_g=$2, station_cents=$3, variance_g=$4, variance_status=$5 WHERE id=$1`,
				it.ID, stationG, stationCents, variance, vStatus); err != nil {
				return err
			}
			sg, sc, vg := stationG, stationCents, variance
			it.StationWeightG, it.StationCents, it.VarianceG, it.VarianceStatus = &sg, &sc, &vg, vStatus
			finalTotal += stationCents
			if variance < 0 {
				totalVariance += -variance
			} else {
				totalVariance += variance
			}
			varianceDetails = append(varianceDetails, map[string]any{
				"category": w.Category, "onsite_g": onsiteG, "station_g": stationG,
				"variance_g": variance, "variance_status": vStatus,
			})
		}
		// 未复称的品类按 0 计
		for _, it := range items {
			if !seen[it.Category] && it.StationWeightG == nil {
				onsiteG := int64(0)
				if it.OnsiteWeightG != nil {
					onsiteG = *it.OnsiteWeightG
				}
				vStatus := domain.VarianceOK
				if onsiteG > 0 {
					vStatus = domain.VarianceOverTolerance
					overTolerance = true
				}
				zero := int64(0)
				neg := -onsiteG
				if _, err := tx.Exec(ctx,
					`UPDATE order_items SET station_weight_g=0, station_cents=0, variance_g=$2, variance_status=$3 WHERE id=$1`,
					it.ID, neg, vStatus); err != nil {
					return err
				}
				it.StationWeightG, it.StationCents, it.VarianceG, it.VarianceStatus = &zero, &zero, &neg, vStatus
				totalVariance += onsiteG
			}
		}

		// 入库记录
		inStatus := "CONFIRMED"
		if overTolerance {
			inStatus = "VARIANCE"
		}
		inbound := &domain.InboundRecord{
			OrderID: o.ID, StationID: station.ID, OperatorID: stationUserID,
			Status: inStatus, VarianceG: totalVariance,
		}
		if err := store.CreateInboundRecord(ctx, tx, inbound); err != nil {
			return err
		}
		if err := store.AddConfirmation(ctx, tx, &domain.Confirmation{
			OrderID: o.ID, Kind: "STATION_REWEIGH", ActorRole: domain.RoleStation,
			ActorID: stationUserID, Method: domain.MethodSelf,
			Payload: map[string]any{"items": varianceDetails, "inbound_id": inbound.ID},
		}); err != nil {
			return err
		}

		// 超差影响后续结算: 扣减回收员收益 + 积分差额调整
		var pointsAdjust int64
		if overTolerance {
			penalty := o.OnsiteCents - finalTotal
			if penalty < 0 {
				penalty = -penalty
			}
			if err := s.applyEarningPenalty(ctx, tx, o, penalty); err != nil {
				return err
			}
			if o.SettlementType != nil && *o.SettlementType == domain.SettlePoints {
				// 按最终金额重算应得积分, 差额调整(多退少补)
				expected := domain.PointsForAmount(finalTotal, s.Cfg.PointsPerYuan, s.Cfg.PointsBonusPM)
				st, err := store.LatestSettlement(ctx, tx, o.ID)
				if err == nil && st.Status == domain.SettleSuccess {
					pointsAdjust = expected - st.Points
					if pointsAdjust != 0 {
						if _, _, err := store.CreditPoints(ctx, tx, o.ResidentID, &o.ID, &st.ID,
							pointsAdjust, "VARIANCE_ADJUST", fmt.Sprintf("variance:order:%d", o.ID)); err != nil {
							return err
						}
					}
				}
			}
		}

		// 环保凭证: 按品类(复称有效重量)
		certs := []map[string]any{}
		for _, it := range items {
			if it.StationWeightG == nil || *it.StationWeightG <= 0 {
				continue
			}
			_, co2PerKg, err := store.CategoryInfo(ctx, tx, it.Category)
			if err != nil {
				return err
			}
			effG := domain.EffectiveWeightG(*it.StationWeightG, it.ContaminationPM)
			cert := &domain.Certificate{
				CertNo:     fmt.Sprintf("EC%d-%s", o.ID, it.Category),
				OrderID:    o.ID,
				ResidentID: o.ResidentID,
				Category:   it.Category,
				WeightG:    effG,
				CO2SavedG:  domain.CO2SavedG(effG, int64(co2PerKg)),
			}
			if err := store.CreateCertificate(ctx, tx, cert); err != nil {
				return err
			}
			certs = append(certs, map[string]any{
				"cert_no": cert.CertNo, "category": it.Category,
				"weight_g": effG, "co2_saved_g": cert.CO2SavedG,
			})
		}

		// DELIVERED -> INBOUND_CONFIRMED -> COMPLETED
		if err := s.transit(ctx, tx, o, domain.StatusInboundConfirmed, domain.RoleStation, stationUserID,
			map[string]any{
				"inbound_id": inbound.ID, "final_cents": finalTotal,
				"over_tolerance": overTolerance, "variance_g": totalVariance,
				"points_adjust": pointsAdjust,
			},
			map[string]any{"final_cents": finalTotal}); err != nil {
			return err
		}
		if err := s.transit(ctx, tx, o, domain.StatusCompleted, domain.RoleSystem, 0,
			map[string]any{"certificates": certs}, nil); err != nil {
			return err
		}
		o.FinalCents = finalTotal
		// 通知居民(载荷带订单标识, 回调失败时可归入受影响居民名单)
		return store.AddOutbox(ctx, tx, domain.TopicNotifications, fmt.Sprintf("user:%d", o.ResidentID), map[string]any{
			"user_id": o.ResidentID, "kind": "ORDER_COMPLETED",
			"content": fmt.Sprintf("订单 %s 已完成, 最终金额 %.2f 元, 已开具 %d 张环保凭证",
				o.OrderNo, domain.CentsToYuan(finalTotal), len(certs)),
			"order_id": o.ID, "order_no": o.OrderNo, "resident_id": o.ResidentID,
			"at": time.Now().UTC(),
		})
	})
	return o, items, err
}

// applyEarningPenalty 复称超差扣减回收员收益
func (s *Service) applyEarningPenalty(ctx context.Context, tx pgx.Tx, o *domain.Order, penalty int64) error {
	e, err := store.EarningByOrder(ctx, tx, o.ID)
	if err != nil {
		if err == store.ErrNotFound {
			return nil // 无收益记录(异常场景), 跳过
		}
		return err
	}
	if penalty > e.TotalCents {
		penalty = e.TotalCents
	}
	e.PenaltyCents += penalty
	e.TotalCents -= penalty
	return store.CreateEarning(ctx, tx, e) // UPSERT by order_id
}
