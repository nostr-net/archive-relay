//go:build integration

package stats

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
	testDB = "test_archive_relay_stats"
)

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func resetDB(t *testing.T) {
	t.Helper()
	admin, _ := clickhouse.Open(&clickhouse.Options{Addr: []string{chAddr}, Auth: clickhouse.Auth{Database: "default"}})
	ctx := context.Background()
	_ = admin.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s", testDB))
	if err := admin.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", testDB)); err != nil {
		t.Fatal(err)
	}
	_ = admin.Close()
}

func sign(t *testing.T, sk string, kind int, content string, tags nostr.Tags) *nostr.Event {
	t.Helper()
	pk, _ := nostr.GetPublicKey(sk)
	e := &nostr.Event{PubKey: pk, CreatedAt: nostr.Now(), Kind: kind, Tags: tags, Content: content}
	e.ID = e.GetID()
	if err := e.Sign(sk); err != nil {
		t.Fatal(err)
	}
	return e
}

func setup(t *testing.T) (*store.Store, *Service, func()) {
	t.Helper()
	resetDB(t)
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cdb, err := control.Open(t.TempDir()+"/c.db", log)
	if err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{
		ClickHouse: config.ClickHouse{Addr: chAddr, Database: testDB, Username: "default"},
		Batch:      config.Batch{MaxSize: 50, MaxAge: 200 * time.Millisecond},
	}
	s := store.New(cfg, log)
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	svc := New(s.CH(), log)
	return s, svc, func() { s.Close(); cdb.Close() }
}

func TestStatsEngagementAndFollowers(t *testing.T) {
	s, svc, teardown := setup(t)
	defer teardown()
	ctx := context.Background()

	author := nostr.GeneratePrivateKey()
	reacter := nostr.GeneratePrivateKey()
	reposter := nostr.GeneratePrivateKey()
	zapper := nostr.GeneratePrivateKey()
	follower := nostr.GeneratePrivateKey()

	note := sign(t, author, 1, "root note", nil)
	s.SaveEvent(ctx, note)
	s.SaveEvent(ctx, sign(t, reacter, 7, "+", nostr.Tags{{"e", note.ID}}))
	s.SaveEvent(ctx, sign(t, reposter, 6, "", nostr.Tags{{"e", note.ID}}))
	s.SaveEvent(ctx, sign(t, zapper, 9735, "", nostr.Tags{{"e", note.ID}, {"amount", "21000"}}))
	// follower follows the author via kind-3
	pk, _ := nostr.GetPublicKey(author)
	s.SaveEvent(ctx, sign(t, follower, 3, "", nostr.Tags{{"p", pk}}))
	s.FlushAll()

	if err := svc.RefreshNoteMonthly(ctx); err != nil {
		t.Fatalf("note monthly: %v", err)
	}
	if err := svc.RefreshFollowers(ctx); err != nil {
		t.Fatalf("followers: %v", err)
	}
	if err := svc.RefreshDaily(ctx); err != nil {
		t.Fatalf("daily: %v", err)
	}
	if err := svc.RefreshDailyActive(ctx); err != nil {
		t.Fatalf("daily active: %v", err)
	}

	// engagement for the note
	eng, err := svc.Engagement(ctx, note.ID)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("engagement:", eng)
	if eng["reaction"] != 1 {
		t.Errorf("reaction = %d, want 1", eng["reaction"])
	}
	if eng["repost"] != 1 {
		t.Errorf("repost = %d, want 1", eng["repost"])
	}
	if eng["zap"] != 1 {
		t.Errorf("zap = %d, want 1", eng["zap"])
	}
	if eng["zap_sats"] != 21000 {
		t.Errorf("zap_sats = %d, want 21000", eng["zap_sats"])
	}

	// follower count for author
	fl, err := svc.Followers(ctx, pk)
	if err != nil {
		t.Fatal(err)
	}
	if fl != 1 {
		t.Errorf("followers = %d, want 1", fl)
	}

	// daily rollups include today's posts/reactions/reposts/zaps
	daily, err := svc.Daily(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	m := map[string]uint64{}
	for _, r := range daily {
		m[r.Metric] = r.Value
	}
	fmt.Println("daily:", m)
	if m["posts"] < 1 {
		t.Errorf("posts = %d, want >=1", m["posts"])
	}
	if m["reactions"] < 1 {
		t.Errorf("reactions = %d, want >=1", m["reactions"])
	}

	// DAU: at least the 5 distinct authors
	dau, err := svc.DAU(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("dau:", dau)
	var todayActive uint64
	for _, d := range dau {
		if d.Active > todayActive {
			todayActive = d.Active
		}
	}
	if todayActive < 5 {
		t.Errorf("today's DAU = %d, want >=5", todayActive)
	}
}
