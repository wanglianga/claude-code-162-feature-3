package svc

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"recycle/internal/domain"
	"recycle/internal/store"
)

// PointsClaimResult 积分申诉处理结果
type PointsClaimResult struct {
	ClaimID  int64  `json:"claim_id"`
	Status   string `json:"status"` // RESOLVED_CREDITED | ALREADY_CREDITED
	Credited int64  `json:"credited"`
	Balance  int64  `json:"balance"`
	Message  string `json:"message"`
}

// ClaimPoints 居民申诉"积分不到账": 同步对账
// 对账逻辑: 找到订单的积分结算单 -> 检查积分流水(幂等键 settlement:{id})
//   - 流水缺失 -> 立即补入账, 结算单置 SUCCESS, 订单推进 SETTLED
//   - 流水已存在 -> 返回当前余额, 申诉标记 ALREADY_CREDITED
func (s *Service) ClaimPoints(ctx context.Context, residentID, orderID int64, note string) (*PointsClaimResult, error) {
	result := &PointsClaimResult{}
	err := s.Store.InTx(ctx, func(tx pgx.Tx) error {
		o, err := store.OrderByIDForUpdate(ctx, tx, orderID)
		if err != nil {
			return err
		}
		if o.ResidentID != residentID {
			return forbidden("not your order")
		}
		st, err := store.LatestSettlement(ctx, tx, orderID)
		if err != nil {
			return badReq("order has no settlement")
		}
		if st.Type != domain.SettlePoints {
			return badReq("order settled by cash, no points involved")
		}
		claimID, err := store.CreatePointsClaim(ctx, tx, residentID, orderID, note)
		if err != nil {
			return err
		}
		result.ClaimID = claimID

		idemKey := fmt.Sprintf("settlement:%d", st.ID)
		// 与积分消费者/自动补发共用消费幂等标记: 占用后延迟消息恢复即为空操作,
		// 不会再次推进订单或重复发送到账通知。
		if _, err := store.TryMarkProcessed(ctx, tx, "points-consumer", idemKey); err != nil {
			return err
		}
		entry, credited, err := store.CreditPoints(ctx, tx, residentID, &orderID, &st.ID,
			st.Points, "RECYCLE_CREDIT", idemKey)
		if err != nil {
			return err
		}
		if credited {
			// 补账成功: 结算单置 SUCCESS, 订单推进 SETTLED
			if err := store.MarkSettlementSuccess(ctx, tx, st.ID); err != nil {
				return err
			}
			if o.Status == domain.StatusConfirmed {
				if err := s.createEarning(ctx, tx, o); err != nil {
					return err
				}
				if err := s.transit(ctx, tx, o, domain.StatusSettled, domain.RoleSystem, 0,
					map[string]any{"type": domain.SettlePoints, "points": st.Points, "via": "claim_reconcile"},
					map[string]any{"settlement_type": domain.SettlePoints}); err != nil {
					return err
				}
			}
			// 补发明细: 居民端可查补发来源、到账时间、原订单编号
			reissue := &domain.PointsReissue{
				ResidentID:      residentID,
				OrderID:         orderID,
				SettlementID:    st.ID,
				Points:          st.Points,
				Reason:          domain.ReissueReasonManualReconcile,
				Source:          domain.ReissueSourceManualReconcile,
				OriginalOrderNo: o.OrderNo,
				PointsEntryID:   &entry.ID,
				IdempotencyKey:  fmt.Sprintf("reissue:settlement:%d", st.ID),
			}
			if err := store.CreatePointsReissue(ctx, tx, reissue); err != nil {
				return err
			}
			result.Status = "RESOLVED_CREDITED"
			result.Credited = st.Points
			result.Balance = entry.BalanceAfter
			result.Message = fmt.Sprintf("积分已补入账: +%d, 当前余额 %d", st.Points, entry.BalanceAfter)
			if err := s.notify(ctx, tx, residentID, "POINTS_REISSUED",
				fmt.Sprintf("您订单 %s 的回收积分已补发: +%d(来源:%s, 到账时间:%s)",
					o.OrderNo, st.Points, domain.ReissueSourceManualReconcile,
					reissue.CreditedAt.Format("2006-01-02 15:04:05"))); err != nil {
				return err
			}
			return store.ResolvePointsClaim(ctx, tx, claimID, "RESOLVED_CREDITED", result.Message)
		}
		// 已入账
		bal, err := store.PointsBalance(ctx, tx, residentID)
		if err != nil {
			return err
		}
		result.Status = "ALREADY_CREDITED"
		result.Balance = bal
		result.Message = fmt.Sprintf("积分已于此前入账, 当前余额 %d", bal)
		return store.ResolvePointsClaim(ctx, tx, claimID, "ALREADY_CREDITED", result.Message)
	})
	return result, err
}

// PointsBalance 查询余额与流水
func (s *Service) PointsBalance(ctx context.Context, residentID int64) (int64, []*domain.PointsEntry, error) {
	bal, err := store.PointsBalance(ctx, s.Store.Pool, residentID)
	if err != nil {
		return 0, nil, err
	}
	entries, err := store.ListPointsEntries(ctx, s.Store.Pool, residentID, 50)
	return bal, entries, err
}

// ListReissues 居民查询积分补发记录(补发来源、到账时间、原订单编号)
func (s *Service) ListReissues(ctx context.Context, residentID int64) ([]*domain.PointsReissue, error) {
	return store.ListPointsReissuesByResident(ctx, s.Store.Pool, residentID, 100)
}

// CreditPointsFromCommand 积分消费者调用: 幂等入账 + 推进订单
// 返回 true 表示本次实际处理(供消费者提交 offset)
func (s *Service) CreditPointsFromCommand(ctx context.Context, settlementID int64) error {
	return s.Store.InTx(ctx, func(tx pgx.Tx) error {
		first, err := store.TryMarkProcessed(ctx, tx, "points-consumer", fmt.Sprintf("settlement:%d", settlementID))
		if err != nil {
			return err
		}
		if !first {
			return nil // 重放: 已处理
		}
		st, err := store.SettlementByID(ctx, tx, settlementID)
		if err != nil {
			return err
		}
		o, err := store.OrderByIDForUpdate(ctx, tx, st.OrderID)
		if err != nil {
			return err
		}
		if _, _, err := store.CreditPoints(ctx, tx, o.ResidentID, &o.ID, &st.ID,
			st.Points, "RECYCLE_CREDIT", fmt.Sprintf("settlement:%d", st.ID)); err != nil {
			return err
		}
		if err := store.MarkSettlementSuccess(ctx, tx, st.ID); err != nil {
			return err
		}
		if o.Status == domain.StatusConfirmed {
			if err := s.createEarning(ctx, tx, o); err != nil {
				return err
			}
			if err := s.transit(ctx, tx, o, domain.StatusSettled, domain.RoleSystem, 0,
				map[string]any{"type": domain.SettlePoints, "points": st.Points, "via": "points_consumer"},
				map[string]any{"settlement_type": domain.SettlePoints}); err != nil {
				return err
			}
		}
		bal, _ := store.PointsBalance(ctx, tx, o.ResidentID)
		if err := store.AddOutbox(ctx, tx, domain.TopicPointsEvents, fmt.Sprintf("settlement:%d", st.ID), map[string]any{
			"event": "points_credited", "settlement_id": st.ID, "order_id": o.ID,
			"resident_id": o.ResidentID, "points": st.Points, "balance": bal, "at": time.Now().UTC(),
		}); err != nil {
			return err
		}
		return s.notify(ctx, tx, o.ResidentID, "POINTS_CREDITED",
			fmt.Sprintf("订单 %s 积分已到账: +%d, 余额 %d", o.OrderNo, st.Points, bal))
	})
}
