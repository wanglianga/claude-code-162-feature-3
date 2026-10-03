// 回收服务入口: server(HTTP+派单) / worker(Kafka relay+消费者) / migrate / seed / all
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"recycle/internal/config"
	"recycle/internal/events"
	"recycle/internal/httpapi"
	"recycle/internal/seed"
	"recycle/internal/store"
	"recycle/internal/svc"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	mode := "all"
	if len(os.Args) > 1 {
		mode = os.Args[1]
	}
	cfg := config.Load()
	ctx := context.Background()

	st, err := store.Connect(ctx, cfg.PGDSN)
	if err != nil {
		log.Fatalf("connect postgres: %v", err)
	}
	if err := st.Migrate(ctx, "migrations"); err != nil {
		log.Fatalf("migrate: %v", err)
	}
	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("connect redis: %v", err)
	}

	switch mode {
	case "migrate":
		log.Println("migrated")
		return
	case "seed":
		if err := seed.Run(ctx, st, rdb); err != nil {
			log.Fatalf("seed: %v", err)
		}
		if err := seed.SyncGeo(ctx, st, rdb); err != nil {
			log.Fatalf("sync geo: %v", err)
		}
		log.Println("seeded")
		return
	}

	s := svc.New(st, rdb, cfg)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	runServer := mode == "server" || mode == "all"
	runWorker := mode == "worker" || mode == "all"

	if runWorker {
		producer := events.NewProducer(cfg.KafkaBrokers)
		defer producer.Close()
		go events.Relay(ctx, st, producer, 300*time.Millisecond)

		// 积分消费者(可用 POINTS_CONSUMER_PAUSED=1 暂停以模拟积分不到账)
		pointsConsumer := events.NewConsumer(cfg.KafkaBrokers, "recycle.points.commands", "points-consumer", "points",
			events.PointsCommandHandler(s, func() bool { return cfg.PointsConsumerPaused }))
		defer pointsConsumer.Close()
		go pointsConsumer.Run(ctx)

		notifConsumer := events.NewConsumer(cfg.KafkaBrokers, "recycle.notifications", "notification-consumer", "notify",
			events.NotificationHandler(st))
		defer notifConsumer.Close()
		go notifConsumer.Run(ctx)

		statsConsumer := events.NewConsumer(cfg.KafkaBrokers, "recycle.order.events", "stats-consumer", "stats",
			events.StatsHandler(st))
		defer statsConsumer.Close()
		go statsConsumer.Run(ctx)
		log.Println("worker started: outbox relay + points/notification/stats consumers")
	}

	if runServer {
		// 派单与迟到检测定时器
		go func() {
			t := time.NewTicker(10 * time.Second)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					if n, err := s.DispatchPending(ctx); err == nil && n > 0 {
						log.Printf("dispatch tick: %d pending processed", n)
					}
					if n, err := s.DetectLate(ctx); err == nil && n > 0 {
						log.Printf("late tick: %d orders marked late", n)
					}
				}
			}
		}()

		// 积分到账异常核对定时器: 延迟自动补发 / 回收员未提交生成任务 / 回调排查自动关闭
		go func() {
			t := time.NewTicker(5 * time.Second)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					rep, err := s.ReconcilePoints(ctx)
					if err != nil {
						log.Printf("points reconcile tick: %v", err)
						continue
					}
					if rep != nil && (len(rep.Reissued) > 0 || rep.TasksCreated > 0 || rep.IncidentsResolved > 0) {
						log.Printf("points reconcile tick: %d reissued, %d tasks, %d incidents resolved",
							len(rep.Reissued), rep.TasksCreated, rep.IncidentsResolved)
					}
				}
			}
		}()

		srv := &http.Server{
			Addr:    cfg.HTTPAddr,
			Handler: httpapi.NewServer(s, st).Router(),
		}
		go func() {
			<-ctx.Done()
			shutCtx, c := context.WithTimeout(context.Background(), 5*time.Second)
			defer c()
			_ = srv.Shutdown(shutCtx)
		}()
		log.Printf("http listening on %s", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("http: %v", err)
		}
	} else {
		<-ctx.Done()
	}
	log.Println("bye")
}
