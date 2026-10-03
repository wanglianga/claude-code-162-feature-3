package svc

import (
	"context"

	"github.com/jackc/pgx/v5"
	"recycle/internal/domain"
	"recycle/internal/store"
)

// ResolveDispute 街道/管理员裁决称重争议
// action: adjust(按裁决重量重新称重, 回到 WEIGHED 待居民再确认) | confirm(维持原称重直接确认) | reject(驳回, 回到 WEIGHED)
func (s *Service) ResolveDispute(ctx context.Context, adminID, disputeID int64,
	action string, adjustedWeights []WeighItem, note string) (*domain.Dispute, error) {

	var d *domain.Dispute
	err := s.Store.InTx(ctx, func(tx pgx.Tx) error {
		var err error
		d, err = store.DisputeByID(ctx, tx, disputeID)
		if err != nil {
			return err
		}
		if d.Status != "OPEN" && d.Status != "open" {
			return conflict("dispute already resolved")
		}
		o, err := store.OrderByIDForUpdate(ctx, tx, d.OrderID)
		if err != nil {
			return err
		}
		if o.Status != domain.StatusDisputed {
			return conflict("order %s in status %s, expect DISPUTED", o.OrderNo, o.Status)
		}
		resolution := map[string]any{"action": action, "note": note, "admin_id": adminID}

		switch action {
		case "adjust":
			if len(adjustedWeights) == 0 {
				return badReq("adjusted_weights required for adjust")
			}
			items, err := store.ListOrderItems(ctx, tx, o.ID)
			if err != nil {
				return err
			}
			byCat := map[string]*domain.OrderItem{}
			for _, it := range items {
				byCat[it.Category] = it
			}
			var total int64
			for _, w := range adjustedWeights {
				it, ok := byCat[w.Category]
				if !ok {
					return badReq("category %s not in order", w.Category)
				}
				weightG := domain.KgToGrams(w.WeightKg)
				effG := domain.EffectiveWeightG(weightG, w.ContaminationPM)
				amount := domain.AmountForWeight(effG, it.EstPriceCents)
				if _, err := tx.Exec(ctx,
					`UPDATE order_items SET onsite_weight_g=$2, contamination_pm=$3, onsite_cents=$4 WHERE id=$1`,
					it.ID, weightG, w.ContaminationPM, amount); err != nil {
					return err
				}
				total += amount
			}
			resolution["adjusted_cents"] = total
			if err := store.ResolveDispute(ctx, tx, disputeID, "RESOLVED", resolution); err != nil {
				return err
			}
			// 回到 WEIGHED 待居民重新确认
			return s.transit(ctx, tx, o, domain.StatusWeighed, domain.RoleStreet, adminID,
				map[string]any{"dispute_id": disputeID, "action": "adjust", "onsite_cents": total},
				map[string]any{"onsite_cents": total})
		case "confirm":
			if err := store.ResolveDispute(ctx, tx, disputeID, "RESOLVED", resolution); err != nil {
				return err
			}
			return s.transit(ctx, tx, o, domain.StatusConfirmed, domain.RoleStreet, adminID,
				map[string]any{"dispute_id": disputeID, "action": "confirm"}, nil)
		case "reject":
			if err := store.ResolveDispute(ctx, tx, disputeID, "REJECTED", resolution); err != nil {
				return err
			}
			return s.transit(ctx, tx, o, domain.StatusWeighed, domain.RoleStreet, adminID,
				map[string]any{"dispute_id": disputeID, "action": "reject"}, nil)
		default:
			return badReq("action must be adjust|confirm|reject")
		}
	})
	if err == nil {
		if fresh, gerr := store.DisputeByID(ctx, s.Store.Pool, disputeID); gerr == nil {
			d = fresh
		}
	}
	return d, err
}

// FileComplaint 投诉(居民/回收员/站点均可发起)
func (s *Service) FileComplaint(ctx context.Context, orderID int64, fromRole string, fromID int64, content string) (*domain.Complaint, error) {
	if content == "" {
		return nil, badReq("content required")
	}
	var c *domain.Complaint
	err := s.Store.InTx(ctx, func(tx pgx.Tx) error {
		o, err := store.OrderByID(ctx, tx, orderID)
		if err != nil {
			return err
		}
		// 仅订单相关方可投诉
		involved := o.ResidentID == fromID ||
			(o.CollectorID != nil && *o.CollectorID == fromID) ||
			fromRole == domain.RoleStation || fromRole == domain.RoleStreet
		if !involved {
			return forbidden("not involved in this order")
		}
		c = &domain.Complaint{
			OrderID: orderID, FromRole: fromRole, FromID: fromID,
			Content: content, Status: "OPEN",
		}
		if err := store.CreateComplaint(ctx, tx, c); err != nil {
			return err
		}
		// 投诉也记入订单事件流, 保证可追踪
		ev := &domain.OrderEvent{
			OrderID: orderID, FromStatus: o.Status, ToStatus: o.Status,
			ActorRole: fromRole, ActorID: fromID,
			Detail: map[string]any{"kind": "complaint", "complaint_id": c.ID},
		}
		return store.AddOrderEvent(ctx, tx, ev)
	})
	return c, err
}

// ResolveComplaint 街道处理投诉
func (s *Service) ResolveComplaint(ctx context.Context, complaintID int64, resolution string) error {
	if resolution == "" {
		return badReq("resolution required")
	}
	return store.ResolveComplaint(ctx, s.Store.Pool, complaintID, resolution)
}

// UpdateMarketPrice 更新市场价(版本化)并清缓存
func (s *Service) UpdateMarketPrice(ctx context.Context, category string, priceCents int64, by string) error {
	ok, err := store.CategoryExists(ctx, s.Store.Pool, category)
	if err != nil {
		return err
	}
	if !ok {
		return badReq("unknown category %s", category)
	}
	if priceCents <= 0 {
		return badReq("price must be positive")
	}
	_, err = s.Store.Pool.Exec(ctx,
		`INSERT INTO market_prices (category, price_cents_kg, created_by) VALUES ($1,$2,$3)`,
		category, priceCents, by)
	if err != nil {
		return err
	}
	s.Redis.Del(ctx, "cache:price:"+category)
	return nil
}

// OrderTrace 全链路追踪: 谁确认/何时确认/对后续结算的影响
type OrderTrace struct {
	Order         *domain.Order            `json:"order"`
	Items         []*domain.OrderItem      `json:"items"`
	Events        []*domain.OrderEvent     `json:"events"`
	Confirmations []*domain.Confirmation   `json:"confirmations"`
	Settlements   []*domain.Settlement     `json:"settlements"`
	PointsEntries []*domain.PointsEntry    `json:"points_entries"`
	Inbound       *domain.InboundRecord    `json:"inbound,omitempty"`
	Certificates  []*domain.Certificate    `json:"certificates"`
	Earning       *domain.CollectorEarning `json:"earning,omitempty"`
	Disputes      []*domain.Dispute        `json:"disputes"`
	Complaints    []*domain.Complaint      `json:"complaints"`
}

func (s *Service) Trace(ctx context.Context, orderID int64) (*OrderTrace, error) {
	q := s.Store.Pool
	o, err := store.OrderByID(ctx, q, orderID)
	if err != nil {
		return nil, err
	}
	t := &OrderTrace{Order: o}
	if t.Items, err = store.ListOrderItems(ctx, q, orderID); err != nil {
		return nil, err
	}
	if t.Events, err = store.ListOrderEvents(ctx, q, orderID); err != nil {
		return nil, err
	}
	if t.Confirmations, err = store.ListConfirmations(ctx, q, orderID); err != nil {
		return nil, err
	}
	if t.Settlements, err = store.ListSettlements(ctx, q, orderID); err != nil {
		return nil, err
	}
	// 积分流水(按订单)
	t.PointsEntries = []*domain.PointsEntry{}
	if o.ResidentID != 0 {
		entries, err := store.ListPointsEntries(ctx, q, o.ResidentID, 200)
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			if e.OrderID != nil && *e.OrderID == orderID {
				t.PointsEntries = append(t.PointsEntries, e)
			}
		}
	}
	if in, err := store.InboundByOrder(ctx, q, orderID); err == nil {
		t.Inbound = in
	}
	if t.Certificates, err = store.ListCertificates(ctx, q, orderID); err != nil {
		return nil, err
	}
	if e, err := store.EarningByOrder(ctx, q, orderID); err == nil {
		t.Earning = e
	}
	// 争议与投诉
	disputes, err := store.ListDisputes(ctx, q, "")
	if err != nil {
		return nil, err
	}
	for _, d := range disputes {
		if d.OrderID == orderID {
			t.Disputes = append(t.Disputes, d)
		}
	}
	complaints, err := store.ListComplaints(ctx, q, "")
	if err != nil {
		return nil, err
	}
	for _, c := range complaints {
		if c.OrderID == orderID {
			t.Complaints = append(t.Complaints, c)
		}
	}
	return t, nil
}
