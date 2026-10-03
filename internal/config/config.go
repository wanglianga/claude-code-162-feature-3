// Package config 从环境变量加载服务配置
package config

import (
	"os"
	"strconv"
	"strings"
)

type Config struct {
	HTTPAddr     string   // HTTP 监听地址
	PGDSN        string   // PostgreSQL DSN
	RedisAddr    string   // Redis 地址
	KafkaBrokers []string // Kafka brokers
	// 派单半径(公里)
	DispatchRadiusKm float64
	// 迟到宽限(分钟): 超过预约窗口结束 + 宽限仍未到达记为迟到
	LateGraceMinutes int
	// 积分结算激励千分比: 1100 = +10%
	PointsBonusPM int64
	// 每元基础积分
	PointsPerYuan int64
	// 测试注入: 暂停积分消费者以模拟"积分不到账"
	PointsConsumerPaused bool
	// 系统延迟判定宽限(秒): 积分结算单 PENDING 超过该时间且无流水 -> 自动补发
	PointsDelayGraceSeconds int
	// 回收员未提交判定宽限(秒): CONFIRMED 后超过该时间无结算单 -> 生成待处理任务
	MissingSettleGraceSeconds int
	// 消息回调失败排查: 时间窗(秒)与阈值, 窗内失败达阈即生成技术排查记录
	CallbackFailWindowSeconds int
	CallbackFailThreshold     int
	// 测试注入: 通知消息每条前 N 次回调失败(模拟回调频繁失败, 随后恢复); 0=关闭
	NotifyFailAttempts int
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getfloat(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

func getint(key string, def int64) int64 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return def
}

func Load() Config {
	return Config{
		HTTPAddr:                  getenv("HTTP_ADDR", ":8080"),
		PGDSN:                     getenv("PG_DSN", "postgres://postgres@127.0.0.1:5433/recycle?sslmode=disable"),
		RedisAddr:                 getenv("REDIS_ADDR", "127.0.0.1:6379"),
		KafkaBrokers:              strings.Split(getenv("KAFKA_BROKERS", "127.0.0.1:9092"), ","),
		DispatchRadiusKm:          getfloat("DISPATCH_RADIUS_KM", 5),
		LateGraceMinutes:          int(getint("LATE_GRACE_MINUTES", 10)),
		PointsBonusPM:             getint("POINTS_BONUS_PM", 1100),
		PointsPerYuan:             getint("POINTS_PER_YUAN", 10),
		PointsConsumerPaused:      getenv("POINTS_CONSUMER_PAUSED", "") == "1",
		PointsDelayGraceSeconds:   int(getint("POINTS_DELAY_GRACE_SECONDS", 60)),
		MissingSettleGraceSeconds: int(getint("MISSING_SETTLE_GRACE_SECONDS", 300)),
		CallbackFailWindowSeconds: int(getint("CALLBACK_FAIL_WINDOW_SECONDS", 120)),
		CallbackFailThreshold:     int(getint("CALLBACK_FAIL_THRESHOLD", 5)),
		NotifyFailAttempts:        int(getint("NOTIFY_FAIL_ATTEMPTS", 0)),
	}
}
