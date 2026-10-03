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

// 积分到账异常处理:
//   对账四方: 居民账号(账户存在/余额) + 回收员提交(结算单) + 积分规则(应得积分) + 消息回调(失败记录)
//   - 系统延迟: 结算单 PENDING 但积分流水缺失 -> 自动补发(AUTO_REISSUE), 通知居民
//   - 回收员未提交: 订单 CONFIRMED 但无结算单 -> 生成待处理任务, 影响结算(收益挂起/冻结)
//   - 回调频繁失败: 失败计数超阈 -> 技术排查记录 + 受影响居民名单

// ReconcileChecks 四方核对结果
type ReconcileChecks struct {
	ResidentAccountOK  bool               `json:"resident_account_ok"`
	CollectorSubmitted bool               `json:"collector_submitted"`
	SettlementStatus   string             `json:"settlement_status"`
	Rule               *ReconcileRule     `json:"rule"`
	Callback           *ReconcileCallback `json:"callback"`
}

type ReconcileRule struct {
	AmountCents      int64 `json:"amount_cents"`
	PointsPerYuan    int64 `json:"points_per_yuan"`
	BonusPM          int64 `json:"bonus_pm"`
	ExpectedPoints   int64 `json:"expected_points"`
	SettlementPoints int64 `json:"settlement_points"`
	Match            bool  `json:"match"`
}

type ReconcileCallback struct {
	Failures   int   `json:"failures"`
	IncidentID int64 `json:"incident_id"`
}

// ReconcileResult 对账处置结果
type ReconcileResult struct {
	OrderID int64           `json:"order_id"`
	OrderNo string          `json:"order_no"`
	Outcome string          `json:"outcome"` // AUTO_REISSUED|TASK_OPENED|ALREADY_CREDITED|WAITING_COLLECTOR|NOT_POINTS_ORDER
	Message string          `json:"message"`
	Checks  ReconcileChecks `json:"checks"`
	TaskID  int64           `json:"task_id,omitempty"`
	Entry   *ReissuedEntry  `json:"entry,omitempty"`
}

// ReissuedEntry 居民端补发流水视图: 补发来源/到账时间/原订单编号
type ReissuedEntry struct {
	ID            int64     `json:"id"`
	Delta         int64     `json:"delta"`
	BalanceAfter  int64     `json:"balance_after"`
	Kind          string    `json:"kind"`
	Source        string    `json:"source"`          // AUTO_REISSUE | MANUAL_REISSUE | NORMAL
	ReissueReason string    `json:"reissue_reason"`  // SYSTEM_DELAY | RESIDENT_CLAIM
	OriginOrderNo string    `json:"origin_order_no"` // 原订单编号
	CreditedAt    time.Time `json:"credited_at"`     // 到账时间
}

func entryView(e *domain.PointsEntry) *ReissuedEntry {
	return &ReissuedEntry{
		ID: e.ID, Delta: e.Delta, BalanceAfter: e.BalanceAfter, Kind: e.Kind,
		Source: e.Source, ReissueReason: e.ReissueReason,
		OriginOrderNo: e.OriginOrderNo, CreditedAt: e.CreatedAt,
	}
}

// ReconcileResidentPoints 居民报告/系统定时触发的单订单对账
func (s *Service) ReconcileResidentPoints(ctx context.Context, residentID, orderID int64) (*ReconcileResult, error) {
	var result *ReconcileResult
	err := s.Store.InTx(ctx, func(tx pgx.Tx) error {
		o, err := store.OrderByIDForUpdate(ctx, tx, orderID)
		if err != nil {
			return err
		}
		if o.ResidentID != residentID {
			return forbidden("not your order")
		}
		res, err := store.ResidentByID(ctx, tx, residentID)
		r := &ReconcileResult{OrderID: o.ID, OrderNo: o.OrderNo, Checks: ReconcileChecks{
			ResidentAccountOK: err == nil && res != nil,
		}}

		// 现金单不涉及积分
		if o.SettlementType != nil && *o.SettlementType == domain.SettleCash {
			r.Outcome = "NOT_POINTS_ORDER"
			r.Message = "该订单以现金结算, 不涉及积分"
			result = r
			return nil
		}

		st, stErr := store.LatestSettlement(ctx, tx, orderID)
		if stErr == nil {
			r.Checks.CollectorSubmitted = true
			r.Checks.SettlementStatus = st.Status
			if st.Type == domain.SettlePoints {
				r.Checks.Rule = s.ruleCheck(o, st)
			}
		}

		// 消息回调核对
		r.Checks.Callback = s.callbackCheck(ctx, tx, orderID)

		if stErr != nil {
			// 尚未完成上门流程: 不是异常
			if o.Status != domain.StatusConfirmed || o.CollectorID == nil {
				r.Outcome = "WAITING_COLLECTOR"
				r.Message = fmt.Sprintf("订单当前状态 %s, 回收员尚未完成结算提交, 请稍后再查", o.Status)
				result = r
				return nil
			}
			// 回收员未提交: 生成待处理任务(居民报告即开单, 不等宽限)
			task := &domain.CollectorTask{
				TaskNo:      taskNo(o.ID),
				CollectorID: *o.CollectorID, OrderID: o.ID, ResidentID: residentID,
				Kind:   domain.TaskKindPointsSubmissionMissing,
				Status: domain.TaskStatusOpen, SettlementImpact: domain.TaskImpactPending,
				Detail: map[string]any{
					"source": "resident_report", "onsite_cents": o.OnsiteCents,
					"order_no": o.OrderNo,
				},
			}
			t, created, err := store.CreateCollectorTaskIfAbsent(ctx, tx, task)
			if err != nil {
				return err
			}
			r.TaskID = t.ID
			r.Outcome = "TASK_OPENED"
			if created {
				r.Message = "回收员尚未提交结算, 已生成待处理任务并通知回收员, 其本单结算将被挂起"
				if err := s.auditEvent(ctx, tx, o, "POINTS_SUBMISSION_MISSING", map[string]any{
					"task_id": t.ID, "source": "resident_report",
				}); err != nil {
					return err
				}
				if err := s.notify(ctx, tx, *o.CollectorID, "POINTS_TASK_OPENED",
					fmt.Sprintf("订单 %s 已完成但未提交积分结算, 请尽快补提交; 逾期本单收益将被冻结", o.OrderNo)); err != nil {
					return err
				}
				if err := s.notify(ctx, tx, residentID, "POINTS_TASK_OPENED",
					fmt.Sprintf("订单 %s 积分异常已受理: 回收员尚未提交结算, 平台已催办并挂起其结算", o.OrderNo)); err != nil {
					return err
				}
			} else {
				r.Message = "回收员尚未提交结算, 待处理任务已存在(#" + itoa64(t.ID) + "), 其本单结算已挂起"
			}
			result = r
			return nil
		}

		if st.Type != domain.SettlePoints {
			r.Outcome = "NOT_POINTS_ORDER"
			r.Message = "该订单以现金结算, 不涉及积分"
			result = r
			return nil
		}

		idemKey := fmt.Sprintf("settlement:%d", st.ID)
		entry, credited, err := store.CreditPointsWithSource(ctx, tx, residentID, &o.ID, &st.ID,
			st.Points, "RECYCLE_CREDIT", idemKey,
			domain.PointsSourceAutoReissue, domain.ReissueReasonSystemDelay, o.OrderNo)
		if err != nil {
			return err
		}
		if credited {
			// 居民报告时流水仍缺失: 判定为系统延迟, 立即补发
			if err := store.MarkSettlementSuccess(ctx, tx, st.ID); err != nil {
				return err
			}
			// 补发即视为积分指令已消费, 避免消费者恢复后重试产生重复通知
			_, _ = store.TryMarkProcessed(ctx, tx, "points-consumer", idemKey)
			if o.Status == domain.StatusConfirmed {
				if err := s.createEarning(ctx, tx, o); err != nil {
					return err
				}
				if err := s.handleSubmissionTask(ctx, tx, o, "resident_report"); err != nil {
					return err
				}
				if err := s.transit(ctx, tx, o, domain.StatusSettled, domain.RoleSystem, 0,
					map[string]any{"type": domain.SettlePoints, "points": st.Points, "via": "reconcile_auto_reissue"},
					map[string]any{"settlement_type": domain.SettlePoints}); err != nil {
					return err
				}
			}
			r.Outcome = "AUTO_REISSUED"
			r.Message = fmt.Sprintf("系统延迟导致积分未到账, 已自动补发 +%d (原订单 %s)", st.Points, o.OrderNo)
			r.Entry = entryView(entry)
			if err := s.notifyReissue(ctx, tx, residentID, o.OrderNo, st.Points, entry.BalanceAfter,
				"reconcile", domain.PointsSourceAutoReissue); err != nil {
				return err
			}
			result = r
			return nil
		}
		// 已入账
		existing, err := store.PointsEntryByKey(ctx, tx, idemKey)
		if err == nil {
			r.Entry = entryView(existing)
		}
		r.Outcome = "ALREADY_CREDITED"
		r.Message = fmt.Sprintf("积分已于 %s 到账, 当前余额 %d",
			entry.CreatedAt.Format("2006-01-02 15:04:05"), entry.BalanceAfter)
		result = r
		return nil
	})
	return result, err
}

// ruleCheck 积分规则核对: 按现场金额与规则重算应得积分
func (s *Service) ruleCheck(o *domain.Order, st *domain.Settlement) *ReconcileRule {
	expected := domain.PointsForAmount(o.OnsiteCents, s.Cfg.PointsPerYuan, s.Cfg.PointsBonusPM)
	return &ReconcileRule{
		AmountCents: o.OnsiteCents, PointsPerYuan: s.Cfg.PointsPerYuan, BonusPM: s.Cfg.PointsBonusPM,
		ExpectedPoints: expected, SettlementPoints: st.Points, Match: expected == st.Points,
	}
}

// callbackCheck 消息回调核对: 该单的失败次数与关联排查记录
func (s *Service) callbackCheck(ctx context.Context, q store.Querier, orderID int64) *ReconcileCallback {
	c := &ReconcileCallback{}
	_ = q.QueryRow(ctx,
		`SELECT count(*) FROM callback_failures WHERE order_id=$1`, orderID).Scan(&c.Failures)
	_ = q.QueryRow(ctx,
		`SELECT i.id FROM tech_incidents i
		 WHERE i.status='OPEN' AND EXISTS (
		   SELECT 1 FROM incident_affected_residents a WHERE a.incident_id=i.id AND a.order_id=$1)
		 ORDER BY i.id DESC LIMIT 1`, orderID).Scan(&c.IncidentID)
	return c
}

// notifyReissue 补发通知(居民端可见补发来源/到账时间/原订单编号)
func (s *Service) notifyReissue(ctx context.Context, tx pgx.Tx,
	residentID int64, orderNo string, points, balance int64, via, source string) error {
	at := time.Now().Format("2006-01-02 15:04:05")
	sourceLabel := "系统延迟自动补发"
	reason := domain.ReissueReasonSystemDelay
	if source == domain.PointsSourceManualReissue {
		sourceLabel = "申诉核对补发"
		reason = domain.ReissueReasonResidentClaim
	}
	content := fmt.Sprintf(
		"【积分补发】原订单 %s 的 %d 积分已于 %s 到账(补发来源: %s, 渠道: %s), 当前余额 %d",
		orderNo, points, at, sourceLabel, via, balance)
	// 载荷附带 order_no/来源, 并在消息回调失败时可按居民+订单聚合名单
	return store.AddOutbox(ctx, tx, domain.TopicNotifications, fmt.Sprintf("user:%d", residentID), map[string]any{
		"user_id": residentID, "kind": "POINTS_REISSUED", "content": content,
		"order_no": orderNo, "source": source,
		"reissue_reason": reason, "points": points,
		"at": time.Now().UTC(),
	})
}

// auditEvent 订单事件流留痕(不改变状态)
func (s *Service) auditEvent(ctx context.Context, tx pgx.Tx, o *domain.Order, kind string, detail map[string]any) error {
	if detail == nil {
		detail = map[string]any{}
	}
	detail["kind"] = kind
	ev := &domain.OrderEvent{
		OrderID: o.ID, FromStatus: o.Status, ToStatus: o.Status,
		ActorRole: domain.RoleSystem, ActorID: 0, Detail: detail,
	}
	if err := store.AddOrderEvent(ctx, tx, ev); err != nil {
		return err
	}
	return store.AddOutbox(ctx, tx, domain.TopicOrderEvents, "order:"+itoa64(o.ID), map[string]any{
		"event_id": ev.ID, "order_id": o.ID, "order_no": o.OrderNo,
		"from": o.Status, "to": o.Status, "actor_role": domain.RoleSystem,
		"resident_id": o.ResidentID, "collector_id": o.CollectorID, "street": o.Street,
		"detail": detail, "at": time.Now().UTC(),
	})
}

// handleSubmissionTask 回收员补提交后: 任务 OPEN -> SUBMITTED, 本单收益冻结待运营核对
func (s *Service) handleSubmissionTask(ctx context.Context, tx pgx.Tx, o *domain.Order, via string) error {
	task, err := store.OpenCollectorTaskByOrder(ctx, tx, o.ID)
	if err == store.ErrNotFound {
		return nil
	}
	if err != nil {
		return err
	}
	if err := store.MarkCollectorTaskSubmitted(ctx, tx, task.ID); err != nil {
		return err
	}
	if err := store.SetEarningHeld(ctx, tx, o.ID, true, "POINTS_SUBMISSION_MISSING"); err != nil {
		return err
	}
	if err := s.auditEvent(ctx, tx, o, "POINTS_TASK_SUBMITTED", map[string]any{
		"task_id": task.ID, "via": via,
	}); err != nil {
		return err
	}
	if o.CollectorID != nil {
		return s.notify(ctx, tx, *o.CollectorID, "POINTS_TASK_HELD",
			fmt.Sprintf("订单 %s 已补提交, 任务 #%d 待运营核对, 核对完成前本单收益暂不发放", o.OrderNo, task.ID))
	}
	return nil
}

// ResolveSubmissionTaskOnCash 现金结算成功后: 未提交任务直接结案(无积分挂账, 收益不冻结)
func (s *Service) ResolveSubmissionTaskOnCash(ctx context.Context, tx pgx.Tx, o *domain.Order) error {
	task, err := store.OpenCollectorTaskByOrder(ctx, tx, o.ID)
	if err == store.ErrNotFound {
		return nil
	}
	if err != nil {
		return err
	}
	if err := store.ResolveCollectorTask(ctx, tx, task.ID, "回收员已以现金补结算, 无积分挂账", domain.TaskImpactReleased); err != nil {
		return err
	}
	return s.auditEvent(ctx, tx, o, "POINTS_TASK_RESOLVED", map[string]any{
		"task_id": task.ID, "action": "cash_settled",
	})
}

// ---- 定时扫描 ----

// ScanPointsAnomalies 定时器: 自动补发系统延迟积分 + 为未提交回收员开任务
// 返回补发/开单数量
func (s *Service) ScanPointsAnomalies(ctx context.Context) (reissued, tasks int, err error) {
	delay := time.Duration(s.Cfg.PointsDelayGraceSeconds) * time.Second
	delayed, err := store.ListDelayedPointsSettlements(ctx, s.Store.Pool, delay)
	if err != nil {
		return 0, 0, err
	}
	for _, d := range delayed {
		if ok, _ := s.autoReissue(ctx, d.SettlementID); ok {
			reissued++
		}
	}
	grace := time.Duration(s.Cfg.MissingSettleGraceSeconds) * time.Second
	missing, err := store.ListMissingSettlementOrders(ctx, s.Store.Pool, grace)
	if err != nil {
		return reissued, 0, err
	}
	for _, m := range missing {
		if ok, _ := s.openMissingSubmissionTask(ctx, m, "scheduled_scan"); ok {
			tasks++
		}
	}
	return reissued, tasks, nil
}

// autoReissue 系统延迟自动补发(幂等: 与消费者共用 settlement:{id} 幂等键)
func (s *Service) autoReissue(ctx context.Context, settlementID int64) (bool, error) {
	done := false
	err := s.Store.InTx(ctx, func(tx pgx.Tx) error {
		st, err := store.SettlementByID(ctx, tx, settlementID)
		if err != nil {
			return err
		}
		if st.Type != domain.SettlePoints || st.Status != domain.SettlePending {
			return nil
		}
		o, err := store.OrderByIDForUpdate(ctx, tx, st.OrderID)
		if err != nil {
			return err
		}
		idemKey := fmt.Sprintf("settlement:%d", st.ID)
		entry, credited, err := store.CreditPointsWithSource(ctx, tx, o.ResidentID, &o.ID, &st.ID,
			st.Points, "RECYCLE_CREDIT", idemKey,
			domain.PointsSourceAutoReissue, domain.ReissueReasonSystemDelay, o.OrderNo)
		if err != nil {
			return err
		}
		if !credited {
			return nil // 消费者刚好已入账
		}
		if err := store.MarkSettlementSuccess(ctx, tx, st.ID); err != nil {
			return err
		}
		// 自动补发即视为积分指令已消费, 避免消费者恢复后重试产生重复通知
		_, _ = store.TryMarkProcessed(ctx, tx, "points-consumer", idemKey)
		if o.Status == domain.StatusConfirmed {
			if err := s.createEarning(ctx, tx, o); err != nil {
				return err
			}
			if err := s.handleSubmissionTask(ctx, tx, o, "auto_reissue"); err != nil {
				return err
			}
			if err := s.transit(ctx, tx, o, domain.StatusSettled, domain.RoleSystem, 0,
				map[string]any{"type": domain.SettlePoints, "points": st.Points, "via": "auto_reissue_delay"},
				map[string]any{"settlement_type": domain.SettlePoints}); err != nil {
				return err
			}
		}
		if err := store.AddOutbox(ctx, tx, domain.TopicPointsEvents, idemKey, map[string]any{
			"event": "points_reissued", "settlement_id": st.ID, "order_id": o.ID,
			"order_no": o.OrderNo, "resident_id": o.ResidentID, "points": st.Points,
			"balance": entry.BalanceAfter, "source": domain.PointsSourceAutoReissue,
			"reason": domain.ReissueReasonSystemDelay, "at": time.Now().UTC(),
		}); err != nil {
			return err
		}
		if err := s.notifyReissue(ctx, tx, o.ResidentID, o.OrderNo, st.Points, entry.BalanceAfter,
			"auto_scan", domain.PointsSourceAutoReissue); err != nil {
			return err
		}
		done = true
		return nil
	})
	return done, err
}

// openMissingSubmissionTask 回收员未提交 -> 待处理任务 + 影响结算
func (s *Service) openMissingSubmissionTask(ctx context.Context, m store.MissingSettlementOrder, source string) (bool, error) {
	created := false
	err := s.Store.InTx(ctx, func(tx pgx.Tx) error {
		o, err := store.OrderByIDForUpdate(ctx, tx, m.OrderID)
		if err != nil {
			return err
		}
		if o.Status != domain.StatusConfirmed {
			return nil
		}
		if existing, gerr := store.LatestSettlement(ctx, tx, o.ID); gerr == nil && existing != nil {
			return nil // 已提交
		}
		task := &domain.CollectorTask{
			TaskNo:      taskNo(o.ID),
			CollectorID: m.CollectorID, OrderID: o.ID, ResidentID: o.ResidentID,
			Kind:   domain.TaskKindPointsSubmissionMissing,
			Status: domain.TaskStatusOpen, SettlementImpact: domain.TaskImpactPending,
			Detail: map[string]any{
				"source": source, "onsite_cents": o.OnsiteCents, "order_no": o.OrderNo,
			},
		}
		t, isNew, err := store.CreateCollectorTaskIfAbsent(ctx, tx, task)
		if err != nil {
			return err
		}
		if !isNew {
			return nil
		}
		created = true
		if err := s.auditEvent(ctx, tx, o, "POINTS_SUBMISSION_MISSING", map[string]any{
			"task_id": t.ID, "source": source,
		}); err != nil {
			return err
		}
		return s.notify(ctx, tx, m.CollectorID, "POINTS_TASK_OPENED",
			fmt.Sprintf("订单 %s 已完成但未提交结算, 请立即补提交; 该单结算已挂起, 逾期收益将冻结", o.OrderNo))
	})
	return created, err
}

func taskNo(orderID int64) string {
	return fmt.Sprintf("TASK%06d-%d", orderID, time.Now().UnixNano()%1000)
}

// ---- 运营: 待处理任务处置 ----

// ResolveCollectorTask 运营核对回收员补提交: 解冻收益(RELEASED) 或 作废(CANCELLED)
func (s *Service) ResolveCollectorTask(ctx context.Context, taskID int64, action, resolution string) (*domain.CollectorTask, error) {
	if resolution == "" {
		return nil, badReq("resolution required")
	}
	if action != "release" && action != "cancel" {
		return nil, badReq("action must be release|cancel")
	}
	err := s.Store.InTx(ctx, func(tx pgx.Tx) error {
		t, err := store.CollectorTaskByID(ctx, tx, taskID)
		if err != nil {
			return err
		}
		o, err := store.OrderByIDForUpdate(ctx, tx, t.OrderID)
		if err != nil {
			return err
		}
		if action == "release" {
			if err := store.ResolveCollectorTask(ctx, tx, taskID, resolution, domain.TaskImpactReleased); err != nil {
				return err
			}
			if err := store.SetEarningHeld(ctx, tx, t.OrderID, false, ""); err != nil {
				return err
			}
			if err := s.auditEvent(ctx, tx, o, "POINTS_TASK_RESOLVED", map[string]any{
				"task_id": taskID, "action": "release",
			}); err != nil {
				return err
			}
			if err := s.notify(ctx, tx, t.CollectorID, "POINTS_TASK_RESOLVED",
				fmt.Sprintf("订单 %s 的结算任务 #%d 已核对无误, 收益冻结解除", o.OrderNo, taskID)); err != nil {
				return err
			}
		} else {
			if err := store.ResolveCollectorTask(ctx, tx, taskID, resolution, domain.TaskImpactCancelled); err != nil {
				return err
			}
			if err := s.auditEvent(ctx, tx, o, "POINTS_TASK_CANCELLED", map[string]any{
				"task_id": taskID,
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return store.CollectorTaskByID(ctx, s.Store.Pool, taskID)
}

// ---- 回调失败记录与技术排查 ----

// RecordCallbackFailure 记录一次消息回调失败; 时间窗内超阈自动生成技术排查记录
func (s *Service) RecordCallbackFailure(ctx context.Context, topic, consumer, key string, payload []byte, attempt int, reason string) error {
	var body map[string]any
	_ = json.Unmarshal(payload, &body)
	residentID := int64FromMap(body, "resident_id")
	if residentID == 0 {
		residentID = int64FromMap(body, "user_id") // 通知消息以 user_id 标识接收居民
	}
	orderID := int64FromMap(body, "order_id")
	if orderID == 0 {
		if no, _ := body["order_no"].(string); no != "" {
			orderID = store.OrderIDByOrderNo(ctx, s.Store.Pool, no)
		}
	}
	return s.Store.InTx(ctx, func(tx pgx.Tx) error {
		if _, err := store.RecordCallbackFailure(ctx, tx, topic, consumer, key, attempt,
			residentID, orderID, reason, body); err != nil {
			return err
		}
		window := time.Duration(s.Cfg.CallbackFailWindowSeconds) * time.Second
		n, err := store.CountCallbackFailures(ctx, tx, consumer, window)
		if err != nil {
			return err
		}
		// 已有未关闭排查记录: 持续累加失败并把新的受影响居民并入名单
		if inc, gerr := store.OpenTechIncidentByConsumer(ctx, tx, consumer); gerr == nil {
			if err := s.appendIncidentResident(ctx, tx, inc.ID, residentID, orderID); err != nil {
				return err
			}
			return store.BumpIncidentFailures(ctx, tx, inc.ID, n)
		} else if gerr != store.ErrNotFound {
			return gerr
		}
		if n < s.Cfg.CallbackFailThreshold {
			return nil
		}
		// 频繁失败 -> 技术排查记录 + 受影响居民名单
		residents, err := store.ListFailureResidents(ctx, tx, consumer, window)
		if err != nil {
			return err
		}
		incident := &domain.TechIncident{
			IncidentNo: fmt.Sprintf("INC%s-%d", time.Now().Format("20060102"), time.Now().UnixNano()%100000),
			Kind:       domain.IncidentKindCallbackFailBurst,
			Topic:      topic, Consumer: consumer,
			FailCount: n, WindowSeconds: s.Cfg.CallbackFailWindowSeconds,
			Threshold:             s.Cfg.CallbackFailThreshold,
			AffectedResidentCount: len(residents),
			Detail: map[string]any{
				"last_msg_key": key, "last_reason": reason,
				"window_seconds": s.Cfg.CallbackFailWindowSeconds,
			},
		}
		if err := store.CreateTechIncident(ctx, tx, incident); err != nil {
			return err
		}
		for _, r := range residents {
			if err := store.UpsertIncidentResident(ctx, tx, incident.ID, r.ResidentID, r.OrderID, r.LastFailAt); err != nil {
				return err
			}
		}
		// 按去重居民数回填统计(同一居民多单只算一人)
		if err := store.BumpIncidentFailures(ctx, tx, incident.ID, n); err != nil {
			return err
		}
		// 通知运营(街道): 已有排查记录与名单可用于集中解释
		ops, err := store.UsersByRole(ctx, tx, domain.RoleStreet)
		if err != nil {
			return err
		}
		for _, op := range ops {
			if err := s.notify(ctx, tx, op.ID, "TECH_INCIDENT_OPENED",
				fmt.Sprintf("消费者 %s 在 %d 秒内回调失败 %d 次(阈值 %d), 已生成技术排查记录 #%d, 受影响居民 %d 人",
					consumer, s.Cfg.CallbackFailWindowSeconds, n, s.Cfg.CallbackFailThreshold,
					incident.ID, len(residents))); err != nil {
				return err
			}
		}
		return nil
	})
}

// appendIncidentResident 把一次失败归属的居民并入排查名单(仅真实居民; 运营/回收员通知不计入)
func (s *Service) appendIncidentResident(ctx context.Context, tx pgx.Tx, incidentID, residentID, orderID int64) error {
	if residentID <= 0 {
		return nil
	}
	ok, err := store.ResidentExists(ctx, tx, residentID)
	if err != nil || !ok {
		return err
	}
	return store.UpsertIncidentResident(ctx, tx, incidentID, residentID, orderID, time.Now())
}

// ResolveTechIncident 运营关闭技术排查记录
func (s *Service) ResolveTechIncident(ctx context.Context, incidentID int64, resolution string) error {
	if resolution == "" {
		return badReq("resolution required")
	}
	return store.ResolveTechIncident(ctx, s.Store.Pool, incidentID, resolution)
}

func int64FromMap(m map[string]any, key string) int64 {
	if m == nil {
		return 0
	}
	switch v := m[key].(type) {
	case float64:
		return int64(v)
	case json.Number:
		n, _ := v.Int64()
		return n
	case int64:
		return v
	}
	return 0
}
