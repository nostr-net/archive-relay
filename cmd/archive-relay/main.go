// Command archive-relay runs the Nostr archive relay: a khatru relay backed by
// ClickHouse (events/stats) and embedded SQLite (control plane), with a live
// crawler ingesting from upstream relays and a stats refresh service.
package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	_ "net/http/pprof" // registers /debug/pprof handlers on http.DefaultServeMux
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/fiatjaf/khatru"
	"github.com/nbd-wtf/go-nostr"

	"github.com/nostr-net/archive-relay/internal/api"
	"github.com/nostr-net/archive-relay/internal/config"
	"github.com/nostr-net/archive-relay/internal/control"
	"github.com/nostr-net/archive-relay/internal/crawler"
	"github.com/nostr-net/archive-relay/internal/policy"
	"github.com/nostr-net/archive-relay/internal/relay"
	"github.com/nostr-net/archive-relay/internal/scheduler"
	"github.com/nostr-net/archive-relay/internal/stats"
	"github.com/nostr-net/archive-relay/internal/store"
)

func main() {
	configPath := flag.String("config", "", "path to config.yaml (optional)")
	sources := flag.String("sources",
		"wss://relay.damus.io,wss://nos.lol,wss://relay.primal.net,wss://relay.nostr.net",
		"comma-separated upstream relay URLs to crawl")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Error("config load failed", "err", err)
		os.Exit(1)
	}

	// Control plane (embedded SQLite) — crawler/scheduler/auth state.
	cdb, err := control.Open(cfg.SQLite.Path, log.With("pkg", "control"))
	if err != nil {
		log.Error("control db init failed", "err", err)
		os.Exit(1)
	}
	defer cdb.Close()

	// Event store (ClickHouse) — all events, tombstones, snapshots.
	s := store.New(cfg, log.With("pkg", "store"))
	if err := s.Init(); err != nil {
		log.Error("store init failed", "err", err)
		os.Exit(1)
	}
	defer s.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Crawler: ingest from upstream relays (idempotent via SQLite seen_events).
	cr := crawler.New(splitSources(*sources), s, cdb, log.With("pkg", "crawler"))
	go cr.Run(ctx)

	// Stats: periodic refresh of snapshot tables.
	svc := stats.New(s.CH(), log.With("pkg", "stats"))
	go svc.Run(ctx)

	// Forward-declare the relay so the scheduler's publish closure can capture it.
	// The closure is only invoked from sched.Run (goroutine), by which point rl is set.
	var rl *khatru.Relay
	wot := &policy.WoT{Lookup: svc.Followers, Threshold: 0} // Threshold 0 = WoT disabled; set >0 to gate reads
	limiter := policy.NewLimiter(600)                       // per-IP events/reads/REST per minute; tune for your threat model
	breadth := policy.RejectFilterBreadth{
		MaxIDs: cfg.Policy.MaxIDs, MaxAuthors: cfg.Policy.MaxAuthors,
		MaxKinds: cfg.Policy.MaxKinds, MaxTags: cfg.Policy.MaxTags,
	}
	sched := scheduler.New(cdb.Conn(),
		func(ctx context.Context, evt *nostr.Event) error {
			_, err := rl.AddEvent(ctx, evt)
			return err
		},
		60*time.Second, log.With("pkg", "scheduler"))
	rl = relay.New(relay.Deps{Store: s, Sched: sched, WoT: wot, Limiter: limiter, Breadth: breadth})
	go sched.Run(ctx)

	api.NewHandler(svc, s, limiter, log.With("pkg", "api")).Register(rl.Router())

	// Ops profiling (stdlib pprof) on loopback only — never on the public port.
	// net/http/pprof registers its handlers on http.DefaultServeMux at import.
	pprofSrv := &http.Server{Addr: "127.0.0.1:6060", Handler: http.DefaultServeMux}
	go func() {
		<-ctx.Done()
		_ = pprofSrv.Shutdown(context.Background())
	}()
	go func() {
		log.Info("pprof listening", "addr", pprofSrv.Addr)
		if err := pprofSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Warn("pprof server stopped", "err", err)
		}
	}()

	srv := &http.Server{Addr: cfg.Relay.Addr, Handler: rl}
	go func() {
		<-ctx.Done()
		log.Info("shutting down")
		_ = srv.Shutdown(context.Background())
	}()

	log.Info("archive relay listening",
		"addr", cfg.Relay.Addr, "ch", cfg.ClickHouse.Addr, "sources", *sources)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Error("server error", "err", err)
		os.Exit(1)
	}
}

func splitSources(s string) []string {
	var out []string
	cur := ""
	for _, r := range s {
		if r == ',' {
			if cur != "" {
				out = append(out, cur)
			}
			cur = ""
		} else {
			cur += string(r)
		}
	}
	if cur != "" {
		out = append(out, cur)
	}
	return out
}
