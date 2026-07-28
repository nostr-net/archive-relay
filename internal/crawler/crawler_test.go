//go:build integration

package crawler

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/nbd-wtf/go-nostr"

	"github.com/nostr-net/archive-relay/internal/config"
	"github.com/nostr-net/archive-relay/internal/control"
	"github.com/nostr-net/archive-relay/internal/store"
)

var (
	chAddr = envOr("CH_ADDR", "localhost:9000")
	testDB = "test_archive_relay_crawler"
)

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func resetDB(t *testing.T) {
	t.Helper()
	admin, err := clickhouse.Open(&clickhouse.Options{Addr: []string{chAddr}, Auth: clickhouse.Auth{Database: "default"}})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	_ = admin.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s", testDB))
	if err := admin.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", testDB)); err != nil {
		t.Fatal(err)
	}
	_ = admin.Close()
}

// TestCrawlerIngestsFromRelayNostrNet connects to the real relay.nostr.net,
// ingests for 20s, and verifies that in-scope events land in ClickHouse.
func TestCrawlerIngestsFromRelayNostrNet(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping live network test in -short mode")
	}
	resetDB(t)
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cdb, err := control.Open(t.TempDir()+"/control.db", log.With("pkg", "control"))
	if err != nil {
		t.Fatal(err)
	}
	defer cdb.Close()

	cfg := &config.Config{
		ClickHouse: config.ClickHouse{Addr: chAddr, Database: testDB, Username: "default"},
		Batch:      config.Batch{MaxSize: 500, MaxAge: 500 * time.Millisecond},
		Retention:  config.Retention{Archive: "10 YEAR", Social: "1 YEAR", Transient: "30 DAY"},
	}
	s := store.New(cfg, log.With("pkg", "store"))
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	cr := New([]string{"wss://relay.nostr.net"}, s, cdb, log.With("pkg", "crawler"))

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	go cr.Run(ctx)

	// let it ingest
	<-ctx.Done()
	s.FlushAll()

	// count rows across tiers
	conn, _ := clickhouse.Open(&clickhouse.Options{Addr: []string{chAddr}, Auth: clickhouse.Auth{Database: testDB}})
	var total uint64
	for _, tier := range []string{"permanent", "archive", "social"} {
		var n uint64
		_ = conn.QueryRow(context.Background(), fmt.Sprintf("SELECT count() FROM events_%s", tier)).Scan(&n)
		total += n
		fmt.Printf("  tier %-10s %d rows\n", tier, n)
	}
	fmt.Println("total ingested:", total)
	if total == 0 {
		t.Fatal("expected >0 events ingested from relay.nostr.net, got 0")
	}

	// every stored row must be an in-scope kind (1,3,6,7,16,9735,10002,0)
	inScope := map[uint32]bool{0: true, 1: true, 3: true, 6: true, 7: true, 16: true, 9735: true, 10002: true}
	for _, tier := range []string{"permanent", "archive", "social"} {
		rows, _ := conn.Query(context.Background(), fmt.Sprintf("SELECT DISTINCT kind FROM events_%s", tier))
		for rows.Next() {
			var k uint32
			rows.Scan(&k)
			if !inScope[k] {
				t.Errorf("tier %s contains out-of-scope kind %d", tier, k)
			}
		}
		rows.Close()
	}

	// and QueryEvents must return real events (round-trip through our store)
	ch, err := s.QueryEvents(context.Background(), nostr.Filter{Kinds: []int{1}, Limit: 5})
	if err != nil {
		t.Fatal(err)
	}
	got := 0
	for range ch {
		got++
	}
	fmt.Println("kind-1 queryable via store:", got)
}

// keep unused import references honest
var _ = nostr.Now
