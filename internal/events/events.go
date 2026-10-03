// Package events 实现 Kafka 生产/消费与事务性 outbox relay
package events

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/segmentio/kafka-go"
	"recycle/internal/domain"
	"recycle/internal/store"
	"recycle/internal/svc"
)

// Producer 向 Kafka 发消息
type Producer struct {
	writer *kafka.Writer
}

func NewProducer(brokers []string) *Producer {
	return &Producer{writer: &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		BatchTimeout: 100 * time.Millisecond,
		RequiredAcks: kafka.RequireOne,
	}}
}

func (p *Producer) Send(ctx context.Context, topic, key string, payload []byte) error {
	return p.writer.WriteMessages(ctx, kafka.Message{
		Topic: topic, Key: []byte(key), Value: payload,
		Time: time.Now(),
	})
}

func (p *Producer) Close() error { return p.writer.Close() }

// Relay 事务性 outbox -> Kafka (at-least-once; 消费者幂等兜底)
func Relay(ctx context.Context, st *store.Store, p *Producer, interval time.Duration) {
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			msgs, err := store.FetchUnpublished(ctx, st.Pool, 100)
			if err != nil {
				log.Printf("[relay] fetch outbox: %v", err)
				continue
			}
			for _, m := range msgs {
				if err := p.Send(ctx, m.Topic, m.Key, m.Payload); err != nil {
					log.Printf("[relay] publish outbox#%d to %s: %v", m.ID, m.Topic, err)
					break // 保序: 失败则下轮重试
				}
				if err := store.MarkPublished(ctx, st.Pool, m.ID); err != nil {
					log.Printf("[relay] mark published #%d: %v", m.ID, err)
				}
			}
		}
	}
}

// Consumer 单 topic 消费循环
type Consumer struct {
	reader  *kafka.Reader
	handler func(ctx context.Context, key string, payload []byte) error
	name    string
}

func NewConsumer(brokers []string, topic, group, name string,
	handler func(ctx context.Context, key string, payload []byte) error) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:        brokers,
			Topic:          topic,
			GroupID:        group,
			MinBytes:       1,
			MaxBytes:       10 << 20,
			CommitInterval: time.Second,
			StartOffset:    kafka.FirstOffset,
		}),
		handler: handler,
		name:    name,
	}
}

func (c *Consumer) Run(ctx context.Context) {
	for {
		m, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("[%s] fetch: %v", c.name, err)
			time.Sleep(time.Second)
			continue
		}
		if err := c.handler(ctx, string(m.Key), m.Value); err != nil {
			log.Printf("[%s] handle key=%s: %v (will retry)", c.name, string(m.Key), err)
			time.Sleep(500 * time.Millisecond)
			continue // 不提交 offset, 下轮重试
		}
		if err := c.reader.CommitMessages(ctx, m); err != nil && ctx.Err() == nil {
			log.Printf("[%s] commit: %v", c.name, err)
		}
	}
}

func (c *Consumer) Close() error { return c.reader.Close() }

// ---- 消费者处理器 ----

// PointsCommandHandler 积分入账指令。
// 处理失败(含暂停注入)时记录"消息回调失败", 频繁失败由 svc 侧升级为技术排查记录。
func PointsCommandHandler(s *svc.Service, paused func() bool) func(ctx context.Context, key string, payload []byte) error {
	return func(ctx context.Context, key string, payload []byte) error {
		if paused != nil && paused() {
			// 测试注入: 暂停消费以模拟"积分不到账/回调持续失败"
			time.Sleep(2 * time.Second)
			err := errPaused
			recordPointsCallbackFailure(ctx, s, key, payload, err)
			return err
		}
		var cmd struct {
			Command      string `json:"command"`
			SettlementID int64  `json:"settlement_id"`
			ResidentID   int64  `json:"resident_id"`
			OrderID      int64  `json:"order_id"`
		}
		if err := json.Unmarshal(payload, &cmd); err != nil {
			return nil // 毒消息: 跳过
		}
		if cmd.Command != "credit" || cmd.SettlementID == 0 {
			return nil
		}
		if err := s.CreditPointsFromCommand(ctx, cmd.SettlementID); err != nil {
			var rid, oid *int64
			if cmd.ResidentID != 0 {
				v := cmd.ResidentID
				rid = &v
			}
			if cmd.OrderID != 0 {
				v := cmd.OrderID
				oid = &v
			}
			s.RecordCallbackFailure(ctx, domain.TopicPointsCommands, "points-consumer", key, err, rid, oid)
			return err
		}
		return nil
	}
}

// recordPointsCallbackFailure paused 注入路径下从指令解析受影响居民/订单后记录失败
func recordPointsCallbackFailure(ctx context.Context, s *svc.Service, key string, payload []byte, handleErr error) {
	var cmd struct {
		SettlementID int64 `json:"settlement_id"`
		ResidentID   int64 `json:"resident_id"`
		OrderID      int64 `json:"order_id"`
	}
	_ = json.Unmarshal(payload, &cmd)
	var rid, oid *int64
	if cmd.ResidentID != 0 {
		v := cmd.ResidentID
		rid = &v
	}
	if cmd.OrderID != 0 {
		v := cmd.OrderID
		oid = &v
	}
	// 暂停注入的指令消息未带居民/订单时, 用结算单号兜底解析
	if rid == nil && cmd.SettlementID != 0 {
		r, o := s.ResidentOrderBySettlement(ctx, cmd.SettlementID)
		if r != 0 {
			v := r
			rid = &v
		}
		if o != 0 {
			v := o
			oid = &v
		}
	}
	s.RecordCallbackFailure(ctx, domain.TopicPointsCommands, "points-consumer", key, handleErr, rid, oid)
}

var errPaused = &pausedError{}

type pausedError struct{}

func (e *pausedError) Error() string { return "points consumer paused (test injection)" }

// NotificationHandler 通知落库
func NotificationHandler(st *store.Store) func(ctx context.Context, key string, payload []byte) error {
	return func(ctx context.Context, key string, payload []byte) error {
		var n struct {
			UserID  int64  `json:"user_id"`
			Kind    string `json:"kind"`
			Content string `json:"content"`
		}
		if err := json.Unmarshal(payload, &n); err != nil || n.UserID == 0 {
			return nil
		}
		return st.InTx(ctx, func(tx pgx.Tx) error {
			first, err := store.TryMarkProcessed(ctx, tx, "notification", key+":"+n.Kind+":"+n.Content)
			if err != nil || !first {
				return err
			}
			return store.AddNotification(ctx, tx, n.UserID, n.Kind, n.Content)
		})
	}
}

// StatsHandler 街道统计聚合: 订单完成时按品类累计重量/金额/积分
func StatsHandler(st *store.Store) func(ctx context.Context, key string, payload []byte) error {
	return func(ctx context.Context, key string, payload []byte) error {
		var ev struct {
			EventID int64  `json:"event_id"`
			OrderID int64  `json:"order_id"`
			To      string `json:"to"`
			Street  string `json:"street"`
		}
		if err := json.Unmarshal(payload, &ev); err != nil {
			return nil
		}
		if ev.To != domain.StatusCompleted {
			return nil
		}
		return st.InTx(ctx, func(tx pgx.Tx) error {
			first, err := store.TryMarkProcessed(ctx, tx, "stats", key+":completed")
			if err != nil || !first {
				return err
			}
			o, err := store.OrderByID(ctx, tx, ev.OrderID)
			if err != nil {
				return err
			}
			items, err := store.ListOrderItems(ctx, tx, ev.OrderID)
			if err != nil {
				return err
			}
			var points int64
			if stl, err := store.LatestSettlement(ctx, tx, ev.OrderID); err == nil && stl.Type == domain.SettlePoints {
				points = stl.Points
			}
			date := o.UpdatedAt.Format("2006-01-02")
			for _, it := range items {
				var w, cents int64
				if it.StationWeightG != nil {
					w = *it.StationWeightG
				}
				if it.StationCents != nil {
					cents = *it.StationCents
				}
				if err := store.UpsertStreetStat(ctx, tx, o.Street, date, it.Category, 1, w, cents, points); err != nil {
					return err
				}
				points = 0 // 积分只计一次(挂在首个品类行)
			}
			return nil
		})
	}
}
