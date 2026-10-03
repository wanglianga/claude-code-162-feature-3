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

type ctxKey string

const attemptKey ctxKey = "attempt"

// AttemptFromContext 取当前消息的处理尝试次数(首次=1)
func AttemptFromContext(ctx context.Context) int {
	if v, ok := ctx.Value(attemptKey).(int); ok && v > 0 {
		return v
	}
	return 1
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
		// 同一条消息重试时 attempt 累加, 提交后随下一条重置
		for attempt := 1; ; attempt++ {
			hctx := context.WithValue(ctx, attemptKey, attempt)
			herr := c.handler(hctx, string(m.Key), m.Value)
			if herr == nil {
				break
			}
			log.Printf("[%s] handle key=%s attempt=%d: %v (will retry)", c.name, string(m.Key), attempt, herr)
			time.Sleep(500 * time.Millisecond)
			if ctx.Err() != nil {
				return
			}
		}
		if err := c.reader.CommitMessages(ctx, m); err != nil && ctx.Err() == nil {
			log.Printf("[%s] commit: %v", c.name, err)
		}
	}
}

func (c *Consumer) Close() error { return c.reader.Close() }

// ---- 消费者处理器 ----

// PointsCommandHandler 积分入账指令
func PointsCommandHandler(s *svc.Service, paused func() bool) func(ctx context.Context, key string, payload []byte) error {
	return func(ctx context.Context, key string, payload []byte) error {
		if paused != nil && paused() {
			// 测试注入: 暂停消费以模拟"积分不到账"(回调失败, 计入技术排查)
			err := errPaused
			_ = s.RecordCallbackFailure(ctx, domain.TopicPointsCommands, "points-consumer", key, payload,
				AttemptFromContext(ctx), err.Error())
			time.Sleep(500 * time.Millisecond)
			return err
		}
		var cmd struct {
			Command      string `json:"command"`
			SettlementID int64  `json:"settlement_id"`
		}
		if err := json.Unmarshal(payload, &cmd); err != nil {
			return nil // 毒消息: 跳过
		}
		if cmd.Command != "credit" || cmd.SettlementID == 0 {
			return nil
		}
		if err := s.CreditPointsFromCommand(ctx, cmd.SettlementID); err != nil {
			_ = s.RecordCallbackFailure(ctx, domain.TopicPointsCommands, "points-consumer", key, payload,
				AttemptFromContext(ctx), err.Error())
			return err
		}
		return nil
	}
}

var errPaused = &pausedError{}

type pausedError struct{}

func (e *pausedError) Error() string { return "points consumer paused (test injection)" }

// NotificationHandler 通知落库
// failAttempts: 测试注入, 返回 >0 时每条消息前 N 次处理强制失败(模拟回调频繁失败, 之后恢复成功)
func NotificationHandler(s *svc.Service, failAttempts func() int) func(ctx context.Context, key string, payload []byte) error {
	return func(ctx context.Context, key string, payload []byte) error {
		var n struct {
			UserID  int64  `json:"user_id"`
			Kind    string `json:"kind"`
			Content string `json:"content"`
		}
		if err := json.Unmarshal(payload, &n); err != nil || n.UserID == 0 {
			return nil
		}
		attempt := AttemptFromContext(ctx)
		if max := func() int {
			if failAttempts == nil {
				return 0
			}
			return failAttempts()
		}(); max > 0 && attempt <= max {
			err := errNotifyPaused
			_ = s.RecordCallbackFailure(ctx, domain.TopicNotifications, "notification-consumer", key, payload,
				attempt, err.Error())
			time.Sleep(200 * time.Millisecond)
			return err
		}
		err := s.Store.InTx(ctx, func(tx pgx.Tx) error {
			first, err := store.TryMarkProcessed(ctx, tx, "notification", key+":"+n.Kind+":"+n.Content)
			if err != nil || !first {
				return err
			}
			return store.AddNotification(ctx, tx, n.UserID, n.Kind, n.Content)
		})
		if err != nil {
			_ = s.RecordCallbackFailure(ctx, domain.TopicNotifications, "notification-consumer", key, payload,
				attempt, err.Error())
		}
		return err
	}
}

var errNotifyPaused = &notifyPausedError{}

type notifyPausedError struct{}

func (e *notifyPausedError) Error() string { return "notification consumer paused (test injection)" }

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
