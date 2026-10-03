package svc

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"recycle/internal/domain"
	"recycle/internal/store"
)

// 积分到账异常核对服务。
//
// 核对四要素(对一笔选择积分结算的回收订单):
//  1. 居民账号: points_accounts 是否存在、居民档案是否正常;
//  2. 回收员提交: settlements 是否存在、SETTLE_CONFIRM 确认记录是否存在;
//  3. 积分规则: 用 PointsForAmount 按现场金额与配置重算应得积分;
//  4. 消息回调: 积分指令经 Kafka 投递后消费者是否成功入账(points_entries 幂等键)。
//
// 处置:
//   - 已提交但回调延迟(结算 PENDING 超宽限) -> 幂等自动补发 + 通知居民, 记 points_reissues;
//   - 回收员未提交(CONFIRMED 超宽限无结算单) -> 生成回收员待处理任务, 结算挂起并影响收益;
//   - 回调频繁失败 -> RecordCallbackFailure 累计, 达阈值开技术排查记录; 恢复后自动关闭。

// ReissueOutcome 补发结果
type ReissueOutcome struct {
	SettlementID    int64     `json:"settlement_id"`
	OrderID         int64     `json:"order_id"`
	OriginalOrderNo string    `json:"original_order_no"`
	Points          int64     `json:"points"`
	Reason          string    `json:"reason"`
	Source          string    `json:"source"`
	CreditedAt      time.Time `json:"credited_at"`
	Created         bool      `json:"created"`
}

// ReconcileReport 一次核对的结果
type ReconcileReport struct {
	Reissued          []*ReissueOutcome `json:"reissued"`
	TasksCreated      int               `json:"tasks_created"`
	IncidentsResolved int               `json:"incidents_resolved"`
}

// reissuePoints 在事务内幂等补发一笔积分结算。
// 与积分消费者共用 processed_events("points-consumer","settlement:{id}") 幂等标记和
// points_entries 的 settlement:{id} 幂等键: 消费者与自动补发谁先执行都只入账一次,
// 消费者故障恢复后重放为空操作, 不会重复推进订单或重复通知。
func (s *Service) reissuePoints(ctx context.Context, tx pgx.Tx, st *domain.Settlement,
	entryKind, reason, source, via string) (*domain.PointsReissue, bool, error) {

	idemMark := fmt.Sprintf("settlement:%d", st.ID)
	// 已存在补发记录: 幂等返回
	if r, err := store.PointsReissueBySettlement(ctx, tx, st.ID); err == nil {
		return r, false, nil
	} else if err != store.ErrNotFound {
		return nil, false, err
	}
	first, err := store.TryMarkProcessed(ctx, tx, "points-consumer", idemMark)
	if err != nil {
		return nil, false, err
	}
	if !first {
		// 消费者已处理(标记存在则必然已入账并推进), 无需补发
		return nil, false, nil
	}

	o, err := store.OrderByIDForUpdate(ctx, tx, st.OrderID)
	if err != nil {
		return nil, false, err
	}

	// 核对要素 3: 按当前规则重算应得积分(结算单记录值通常一致, 不一致以规则重算为准)
	expected := domain.PointsForAmount(st.AmountCents, s.Cfg.PointsPerYuan, s.Cfg.PointsBonusPM)
	points := st.Points
	if expected > 0 {
		points = expected
	}

	// 核对要素 1/4: 幂等入账(键与消费者相同)
	entry, credited, err := store.CreditPoints(ctx, tx, o.ResidentID, &o.ID, &st.ID,
		points, entryKind, idemMark)
	if err != nil {
		return nil, false, err
	}
	if err := store.MarkSettlementSuccess(ctx, tx, st.ID); err != nil {
		return nil, false, err
	}
	if o.Status == domain.StatusConfirmed {
		if err := s.createEarning(ctx, tx, o); err != nil {
			return nil, false, err
		}
		if err := s.transit(ctx, tx, o, domain.StatusSettled, domain.RoleSystem, 0,
			map[string]any{"type": domain.SettlePoints, "points": points, "via": via, "reason": reason},
			map[string]any{"settlement_type": domain.SettlePoints}); err != nil {
			return nil, false, err
		}
	}

	r := &domain.PointsReissue{
		ResidentID:      o.ResidentID,
		OrderID:         o.ID,
		SettlementID:    st.ID,
		Points:          points,
		Reason:          reason,
		Source:          source,
		OriginalOrderNo: o.OrderNo,
		IdempotencyKey:  fmt.Sprintf("reissue:settlement:%d", st.ID),
	}
	if credited {
		r.PointsEntryID = &entry.ID
	}
	// 已被其他路径入账(重放)也留补发记录, points_entry_id 关联已有流水
	if !credited {
		if e, gerr := store.PointsEntryByKey(ctx, tx, idemMark); gerr == nil {
			r.PointsEntryID = &e.ID
		}
	}
	if err := store.CreatePointsReissue(ctx, tx, r); err != nil {
		return nil, false, err
	}

	// 入账结果事件 + 居民通知(补发来源、到账时间、原订单编号)
	if err := store.AddOutbox(ctx, tx, domain.TopicPointsEvents, idemMark, map[string]any{
		"event": "points_reissued", "settlement_id": st.ID, "order_id": o.ID,
		"order_no": o.OrderNo, "resident_id": o.ResidentID, "points": points,
		"reason": reason, "source": source, "credited_at": r.CreatedAt.UTC(),
		"balance": entry.BalanceAfter, "at": time.Now().UTC(),
	}); err != nil {
		return nil, false, err
	}
	if err := s.notify(ctx, tx, o.ResidentID, "POINTS_REISSUED",
		fmt.Sprintf("您订单 %s 的旧衣回收积分已补发: +%d(来源:%s, 到账时间:%s)",
			o.OrderNo, points, source, r.CreditedAt.Format("2006-01-02 15:04:05"))); err != nil {
		return nil, false, err
	}
	return r, credited || !first, nil
}

// reissueStaleSettlement 对单条 PENDING 结算执行延迟补发(事务内重新校验状态)
func (s *Service) reissueStaleSettlement(ctx context.Context, st *domain.Settlement,
	reason, source string) (*ReissueOutcome, error) {

	out := &ReissueOutcome{SettlementID: st.ID, Reason: reason, Source: source}
	didReissue := false
	err := s.Store.InTx(ctx, func(tx pgx.Tx) error {
		fresh, err := store.SettlementByID(ctx, tx, st.ID)
		if err != nil {
			return err
		}
		if fresh.Status != domain.SettlePending {
			return nil // 已被消费者处理
		}
		o, err := store.OrderByID(ctx, tx, fresh.OrderID)
		if err != nil {
			return err
		}
		if o.Status != domain.StatusConfirmed {
			return nil
		}
		r, created, err := s.reissuePoints(ctx, tx, fresh, domain.PointsKindAutoReissue,
			reason, source, "auto_reconcile")
		if err != nil {
			return err
		}
		if r == nil {
			return nil // 并发竞争: 消费者已抢先处理
		}
		didReissue = true
		out.OrderID = o.ID
		out.OriginalOrderNo = o.OrderNo
		out.Points = r.Points
		out.CreditedAt = r.CreditedAt
		out.Created = created
		return nil
	})
	if err != nil {
		return nil, err
	}
	if !didReissue {
		return nil, nil // 无需补发
	}
	return out, nil
}

// createUnsubmittedTask 回收员未提交结算: 生成待处理任务(幂等)并通知回收员与居民
func (s *Service) createUnsubmittedTask(ctx context.Context, o *domain.Order) (bool, error) {
	created := false
	err := s.Store.InTx(ctx, func(tx pgx.Tx) error {
		locked, err := store.OrderByIDForUpdate(ctx, tx, o.ID)
		if err != nil {
			return err
		}
		if locked.Status != domain.StatusConfirmed || locked.CollectorID == nil {
			return nil
		}
		if st, err := store.LatestSettlement(ctx, tx, locked.ID); err == nil && st != nil {
			return nil // 已提交
		}
		task := &domain.CollectorTask{
			CollectorID: *locked.CollectorID,
			OrderID:     locked.ID,
			Kind:        domain.TaskKindSettlementHeld,
			Reason:      fmt.Sprintf("订单 %s 居民已确认称重定价, 回收员超过宽限期未提交结算", locked.OrderNo),
			Impact:      "结算挂起: 回收员收益暂不生成, 居民积分暂不到账; 补提交后任务自动关闭",
			Detail: map[string]any{
				"order_no": locked.OrderNo, "onsite_cents": locked.OnsiteCents,
				"resident_id": locked.ResidentID,
			},
		}
		ok, err := store.CreateCollectorTaskIfAbsent(ctx, tx, task)
		if err != nil {
			return err
		}
		if !ok {
			return nil // 已存在 OPEN 任务
		}
		created = true
		if err := store.AddOrderEvent(ctx, tx, &domain.OrderEvent{
			OrderID: locked.ID, FromStatus: locked.Status, ToStatus: locked.Status,
			ActorRole: domain.RoleSystem, ActorID: 0,
			Detail: map[string]any{"kind": "SETTLEMENT_HELD", "task_id": task.ID,
				"reason": "collector_submission_overdue"},
		}); err != nil {
			return err
		}
		// 通知回收员: 有待处理任务, 结算挂起影响收益
		if err := s.notify(ctx, tx, *locked.CollectorID, "SETTLEMENT_HELD",
			fmt.Sprintf("订单 %s 您尚未提交结算, 已生成待处理任务, 请尽快提交以免影响结算与收益", locked.OrderNo)); err != nil {
			return err
		}
		// 通知居民: 便于解释积分暂未到账
		return s.notify(ctx, tx, locked.ResidentID, "POINTS_PENDING_SUBMISSION",
			fmt.Sprintf("您的订单 %s 回收员尚未提交结算单, 我们已督促处理, 积分将在提交核对后到账", locked.OrderNo))
	})
	return created, err
}

// ReconcilePoints 定时核对: 自动补发延迟积分 + 生成未提交任务 + 关闭已恢复的技术排查
func (s *Service) ReconcilePoints(ctx context.Context) (*ReconcileReport, error) {
	report := &ReconcileReport{Reissued: []*ReissueOutcome{}}

	// 场景 A: 回收员已提交积分结算, 但消息回调延迟 -> 自动补发
	grace := time.Duration(s.Cfg.PointsReissueGraceSec) * time.Second
	stale, err := store.ListStalePendingPointSettlements(ctx, s.Store.Pool, time.Now().Add(-grace), 100)
	if err != nil {
		return nil, err
	}
	for _, st := range stale {
		out, err := s.reissueStaleSettlement(ctx, st,
			domain.ReissueReasonSystemDelay, domain.ReissueSourceAutoReconcile)
		if err != nil {
			return nil, err
		}
		if out != nil {
			report.Reissued = append(report.Reissued, out)
		}
	}

	// 场景 B: 居民已确认但回收员未提交结算 -> 待处理任务, 结算挂起
	grace2 := time.Duration(s.Cfg.UnsubmittedGraceSec) * time.Second
	pending, err := store.ListConfirmedOrdersWithoutSettlement(ctx, s.Store.Pool, time.Now().Add(-grace2), 100)
	if err != nil {
		return nil, err
	}
	for _, o := range pending {
		ok, err := s.createUnsubmittedTask(ctx, o)
		if err != nil {
			return nil, err
		}
		if ok {
			report.TasksCreated++
		}
	}

	// 场景 C: 回调通道恢复(静默超过恢复时长) -> 自动关闭技术排查记录并通知运营
	recoverWithin := time.Duration(s.Cfg.CallbackRecoverSec) * time.Second
	open, err := store.ListTechIncidents(ctx, s.Store.Pool, domain.IncidentStatusOpen)
	if err != nil {
		return nil, err
	}
	for _, inc := range open {
		if time.Since(inc.LastFailAt) < recoverWithin {
			continue
		}
		summary := fmt.Sprintf("回调通道已恢复: %s/%s 连续 %ds 无失败, 排查记录自动关闭",
			inc.Topic, inc.Consumer, s.Cfg.CallbackRecoverSec)
		closed := false
		if err := s.Store.InTx(ctx, func(tx pgx.Tx) error {
			var err error
			closed, err = store.ResolveOpenTechIncident(ctx, tx, inc.Topic, inc.Consumer, summary)
			return err
		}); err != nil {
			return nil, err
		}
		if closed {
			report.IncidentsResolved++
			s.notifyRole(ctx, domain.RoleStreet, "TECH_INCIDENT_RESOLVED", summary)
		}
	}
	return report, nil
}

// RecordCallbackFailure 消息回调每次失败时由消费者调用:
// 落失败记录(同消息 5s 内去重) -> 窗口计数 -> 达阈值打开/更新技术排查记录并通知运营。
func (s *Service) RecordCallbackFailure(ctx context.Context, topic, consumer, msgKey string,
	handleErr error, residentID, orderID *int64) {

	if handleErr == nil {
		return
	}
	err := s.Store.InTx(ctx, func(tx pgx.Tx) error {
		// 同一消息持续重试时限速落库(消费者重试间隔>1s), 避免失败表无限膨胀
		if last, err := store.LatestCallbackFailure(ctx, tx, topic, consumer); err == nil {
			if last.MsgKey == msgKey && time.Since(last.CreatedAt) < time.Second {
				return nil
			}
		} else if err != store.ErrNotFound {
			return err
		}
		f := &domain.CallbackFailure{
			Topic: topic, Consumer: consumer, MsgKey: msgKey, Err: handleErr.Error(),
			ResidentID: residentID, OrderID: orderID,
		}
		if err := store.AddCallbackFailure(ctx, tx, f); err != nil {
			return err
		}
		window := time.Duration(s.Cfg.CallbackFailWindowSec) * time.Second
		since := time.Now().Add(-window)
		cnt, err := store.CountCallbackFailuresSince(ctx, tx, topic, consumer, since)
		if err != nil {
			return err
		}
		if cnt < s.Cfg.CallbackFailThreshold {
			return nil
		}
		summary := fmt.Sprintf("消息回调频繁失败: %s/%s 在 %ds 内失败 %d 次(阈值 %d), 需技术排查",
			topic, consumer, s.Cfg.CallbackFailWindowSec, cnt, s.Cfg.CallbackFailThreshold)
		inc, created, err := store.OpenTechIncident(ctx, tx, topic, consumer,
			cnt, int(s.Cfg.CallbackFailWindowSec), s.Cfg.CallbackFailThreshold, since, time.Now(), summary)
		if err != nil || inc == nil {
			return err
		}
		if !created {
			if err := store.UpdateTechIncidentCounts(ctx, tx, inc.ID, cnt,
				int(s.Cfg.CallbackFailWindowSec), time.Now()); err != nil {
				return err
			}
			return nil
		}
		// 首次打开: 通知运营(街道)
		return s.notifyRoleTx(ctx, tx, domain.RoleStreet, "TECH_INCIDENT_OPENED", summary)
	})
	if err != nil {
		// 排查链路本身失败不能影响消费主循环
		_ = err
	}
}

// ResidentOrderBySettlement 按结算单号查居民与订单(回调失败记录兜底解析)
func (s *Service) ResidentOrderBySettlement(ctx context.Context, settlementID int64) (residentID, orderID int64) {
	st, err := store.SettlementByID(ctx, s.Store.Pool, settlementID)
	if err != nil {
		return 0, 0
	}
	o, err := store.OrderByID(ctx, s.Store.Pool, st.OrderID)
	if err != nil {
		return 0, st.OrderID
	}
	return o.ResidentID, o.ID
}

// OrderReconcileCheck 单笔订单积分到账四要素核对(运营/排查用)
type OrderReconcileCheck struct {
	OrderID          int64  `json:"order_id"`
	OrderNo          string `json:"order_no"`
	OrderStatus      string `json:"order_status"`
	ResidentID       int64  `json:"resident_id"`
	AccountVerified  bool   `json:"account_verified"` // 要素1: 居民账号
	Submitted        bool   `json:"submitted"`        // 要素2: 回收员提交
	Settlement       any    `json:"settlement,omitempty"`
	ExpectedPoints   int64  `json:"expected_points"` // 要素3: 规则重算
	SettlementType   string `json:"settlement_type"`
	CallbackCredited bool   `json:"callback_credited"` // 要素4: 消息回调入账
	HasReissue       bool   `json:"has_reissue"`
	OpenTask         bool   `json:"open_task"`
	Diagnosis        string `json:"diagnosis"`
}

// CheckOrder 核对居民账号 / 回收员提交 / 积分规则 / 消息回调, 给出诊断
func (s *Service) CheckOrder(ctx context.Context, orderID int64) (*OrderReconcileCheck, error) {
	q := s.Store.Pool
	o, err := store.OrderByID(ctx, q, orderID)
	if err != nil {
		return nil, err
	}
	r := &OrderReconcileCheck{
		OrderID: o.ID, OrderNo: o.OrderNo, OrderStatus: o.Status, ResidentID: o.ResidentID,
	}
	// 要素 1: 居民账号存在且有效
	if _, err := store.ResidentByID(ctx, q, o.ResidentID); err == nil {
		r.AccountVerified = true
	}
	// 要素 2: 回收员提交结算
	stl, err := store.LatestSettlement(ctx, q, orderID)
	if err == nil {
		r.Submitted = true
		r.Settlement = stl
		r.SettlementType = stl.Type
	}
	// 要素 3: 积分规则重算
	r.ExpectedPoints = domain.PointsForAmount(o.OnsiteCents, s.Cfg.PointsPerYuan, s.Cfg.PointsBonusPM)
	// 要素 4: 消息回调入账结果
	if r.Submitted && stl.Type == domain.SettlePoints {
		if _, err := store.PointsEntryByKey(ctx, q, fmt.Sprintf("settlement:%d", stl.ID)); err == nil {
			r.CallbackCredited = true
		}
		if _, err := store.PointsReissueBySettlement(ctx, q, stl.ID); err == nil {
			r.HasReissue = true
		}
	}
	if o.CollectorID != nil {
		tasks, err := store.ListCollectorTasks(ctx, q, *o.CollectorID, domain.TaskStatusOpen)
		if err == nil {
			for _, t := range tasks {
				if t.OrderID == orderID {
					r.OpenTask = true
					break
				}
			}
		}
	}
	switch {
	case !r.Submitted && o.Status == domain.StatusConfirmed:
		r.Diagnosis = "COLLECTOR_NOT_SUBMITTED"
	case r.Submitted && stl.Type == domain.SettlePoints && !r.CallbackCredited && stl.Status == domain.SettlePending:
		r.Diagnosis = "SYSTEM_DELAY_PENDING_REISSUE"
	case r.CallbackCredited:
		r.Diagnosis = "OK_CREDITED"
	case r.HasReissue:
		r.Diagnosis = "REISSUED"
	default:
		r.Diagnosis = "NOT_POINTS_SETTLEMENT_OR_IN_PROGRESS"
	}
	return r, nil
}

// notifyRoleTx 事务内通知某角色全部账号(运营)
func (s *Service) notifyRoleTx(ctx context.Context, tx pgx.Tx, role, kind, content string) error {
	ids, err := store.ListUserIDsByRole(ctx, tx, role)
	if err != nil {
		return err
	}
	for _, id := range ids {
		if err := s.notify(ctx, tx, id, kind, content); err != nil {
			return err
		}
	}
	return nil
}

// notifyRole 事务外便捷封装
func (s *Service) notifyRole(ctx context.Context, role, kind, content string) {
	_ = s.Store.InTx(ctx, func(tx pgx.Tx) error {
		return s.notifyRoleTx(ctx, tx, role, kind, content)
	})
}
