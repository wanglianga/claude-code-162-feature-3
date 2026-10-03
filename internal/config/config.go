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
	// 积分到账延迟宽限(秒): 结算提交超过该时长仍未入账, 定时对账自动补发
	PointsReissueGraceSec int64
	// 回收员未提交结算宽限(秒): 居民确认后超过该时长未提交结算, 生成待处理任务
	UnsubmittedGraceSec int64
	// 消息回调失败统计窗口(秒)与阈值: 窗口内失败达阈值生成技术排查记录
	CallbackFailWindowSec int64
	CallbackFailThreshold int
	// 回调失败静默多久(秒)视为恢复, 自动关闭排查记录
	CallbackRecoverSec int64
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
		HTTPAddr:              getenv("HTTP_ADDR", ":8080"),
		PGDSN:                 getenv("PG_DSN", "postgres://postgres@127.0.0.1:5433/recycle?sslmode=disable"),
		RedisAddr:             getenv("REDIS_ADDR", "127.0.0.1:6379"),
		KafkaBrokers:          strings.Split(getenv("KAFKA_BROKERS", "127.0.0.1:9092"), ","),
		DispatchRadiusKm:      getfloat("DISPATCH_RADIUS_KM", 5),
		LateGraceMinutes:      int(getint("LATE_GRACE_MINUTES", 10)),
		PointsBonusPM:         getint("POINTS_BONUS_PM", 1100),
		PointsPerYuan:         getint("POINTS_PER_YUAN", 10),
		PointsConsumerPaused:  getenv("POINTS_CONSUMER_PAUSED", "") == "1",
		PointsReissueGraceSec: getint("POINTS_REISSUE_GRACE_SEC", 30),
		UnsubmittedGraceSec:   getint("UNSUBMITTED_GRACE_SEC", 60),
		CallbackFailWindowSec: getint("CALLBACK_FAIL_WINDOW_SEC", 300),
		CallbackFailThreshold: int(getint("CALLBACK_FAIL_THRESHOLD", 5)),
		CallbackRecoverSec:    getint("CALLBACK_RECOVER_SEC", 60),
	}
}
