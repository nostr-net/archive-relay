package stats

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type fakeConn struct {
	driver.Conn
	mu         sync.Mutex
	statements []string
	engine     string
	hook       func(string) error
}

func (f *fakeConn) record(q string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.statements = append(f.statements, strings.TrimSpace(q))
}
func (f *fakeConn) recorded() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.statements...)
}
func (f *fakeConn) Exec(_ context.Context, q string, _ ...any) error {
	f.record(q)
	if f.hook != nil {
		return f.hook(strings.TrimSpace(q))
	}
	return nil
}
func (f *fakeConn) QueryRow(_ context.Context, q string, _ ...any) driver.Row {
	f.record(q)
	return fakeRow{engine: f.engine}
}

type fakeRow struct {
	driver.Row
	engine string
}

func (r fakeRow) Scan(dest ...any) error { *dest[0].(*string) = r.engine; return nil }

func assertFollowerSequence(t *testing.T, statements []string, runs int) {
	t.Helper()
	if len(statements) != 1+3*runs {
		t.Fatalf("statements = %v", statements)
	}
	if statements[0] != "SELECT engine FROM system.databases WHERE name = currentDatabase()" {
		t.Fatal(statements[0])
	}
	for i := 0; i < runs; i++ {
		q := statements[1+3*i : 4+3*i]
		if q[0] != "TRUNCATE TABLE author_follower_counts_staging" || !strings.HasPrefix(q[1], "INSERT INTO author_follower_counts_staging") || q[2] != "EXCHANGE TABLES author_follower_counts AND author_follower_counts_staging" {
			t.Fatalf("sequence: %v", q)
		}
		if !strings.Contains(q[1], "ORDER BY created_at DESC, id ASC LIMIT 1 BY pubkey") || !strings.Contains(q[1], "NOT dictHas('tombstone_dict', id)") {
			t.Fatal(q[1])
		}
	}
}
func TestFollowersSequence(t *testing.T) {
	f := &fakeConn{engine: "Atomic"}
	s := New(f, slog.Default())
	for i := 0; i < 2; i++ {
		if err := s.RefreshFollowers(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	assertFollowerSequence(t, f.recorded(), 2)
}
func TestFollowersSerialized(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	f := &fakeConn{engine: "Atomic"}
	f.hook = func(q string) error {
		if strings.HasPrefix(q, "INSERT") {
			once.Do(func() { close(entered); <-release })
		}
		return nil
	}
	s := New(f, slog.Default())
	results := make(chan error, 2)
	go func() { results <- s.RefreshFollowers(context.Background()) }()
	<-entered
	started := make(chan struct{})
	go func() { close(started); results <- s.RefreshFollowers(context.Background()) }()
	<-started
	// The first build is held open; a second refresh must not truncate staging.
	time.Sleep(30 * time.Millisecond)
	before := f.recorded()
	close(release)
	for i := 0; i < 2; i++ {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if len(before) != 3 {
		t.Fatalf("refresh interleaved while build blocked: %v", before)
	}
	assertFollowerSequence(t, f.recorded(), 2)
}
func TestFollowersNonAtomic(t *testing.T) {
	f := &fakeConn{engine: "Ordinary"}
	s := New(f, slog.Default())
	for i := 0; i < 2; i++ {
		if err := s.RefreshFollowers(context.Background()); err == nil || !strings.Contains(err.Error(), "requires Atomic") {
			t.Fatalf("error = %v", err)
		}
	}
	if len(f.recorded()) != 1 {
		t.Fatal(f.recorded())
	}
}
func TestFollowersFailureDoesNotSwap(t *testing.T) {
	for _, prefix := range []string{"TRUNCATE", "INSERT", "EXCHANGE"} {
		t.Run(prefix, func(t *testing.T) {
			want := errors.New("failed")
			f := &fakeConn{engine: "Atomic", hook: func(q string) error {
				if strings.HasPrefix(q, prefix) {
					return want
				}
				return nil
			}}
			if err := New(f, slog.Default()).RefreshFollowers(context.Background()); !errors.Is(err, want) {
				t.Fatalf("error = %v", err)
			}
			qs := f.recorded()
			if !strings.HasPrefix(qs[len(qs)-1], prefix) {
				t.Fatal(qs)
			}
		})
	}
}

// Both test types share this owned file. CH_ADDR enables the live integration
// test; run with CH_ADDR=localhost:9000 go test -tags=integration ./internal/stats/.
func TestStatsIntegration(t *testing.T) {
	addr := os.Getenv("CH_ADDR")
	if addr == "" {
		t.Skip("set CH_ADDR to run ClickHouse integration")
	}
	ctx := context.Background()
	admin, err := clickhouse.Open(&clickhouse.Options{Addr: []string{addr}, Auth: clickhouse.Auth{Database: "default"}})
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	db := fmt.Sprintf("stats_it_%d", time.Now().UnixNano())
	if err := admin.Exec(ctx, "CREATE DATABASE "+db+" ENGINE = Atomic"); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := admin.Exec(ctx, "DROP DATABASE "+db+" SYNC"); err != nil {
			t.Error(err)
		}
	}()
	ch, err := clickhouse.Open(&clickhouse.Options{Addr: []string{addr}, Auth: clickhouse.Auth{Database: db}})
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()
	exec := func(q string, args ...any) {
		t.Helper()
		if err := ch.Exec(ctx, q, args...); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	exec(`CREATE TABLE events_permanent (id String, pubkey String, created_at UInt32, kind UInt32, tags_raw String, tag_e Array(String), tag_p Array(String), reply_to String) ENGINE = ReplacingMergeTree ORDER BY (kind,pubkey,created_at,id)`)
	exec(`CREATE TABLE events_archive AS events_permanent`)
	exec(`CREATE TABLE events_social AS events_permanent`)
	exec(`CREATE VIEW events_all AS SELECT * FROM events_permanent UNION ALL SELECT * FROM events_archive UNION ALL SELECT * FROM events_social`)
	exec(`CREATE TABLE tombstones (id String) ENGINE = MergeTree ORDER BY id`)
	exec(fmt.Sprintf(`CREATE DICTIONARY tombstone_dict (id String) PRIMARY KEY id SOURCE(CLICKHOUSE(DB '%s' TABLE 'tombstones')) LAYOUT(COMPLEX_KEY_HASHED()) LIFETIME(0)`, db))
	exec(`CREATE TABLE author_follower_counts (pubkey String, followers UInt64) ENGINE = MergeTree ORDER BY pubkey`)
	exec(`CREATE TABLE author_follower_counts_staging AS author_follower_counts`)
	exec(`CREATE TABLE stats_note_monthly (note_id String, month Date, metric LowCardinality(String), count UInt64, sats UInt64) ENGINE = SummingMergeTree PARTITION BY toYYYYMM(month) ORDER BY (note_id,month,metric)`)
	exec(`CREATE TABLE stats_daily (day Date, metric LowCardinality(String), value UInt64) ENGINE = SummingMergeTree PARTITION BY toYYYYMM(day) ORDER BY (day,metric)`)
	exec(`CREATE TABLE stats_daily_active (day Date, authors AggregateFunction(uniq,String)) ENGINE = AggregatingMergeTree ORDER BY day`)
	now := uint32(time.Now().Unix())
	insert := func(table, id, author string, at uint32, kind uint32, tags string, e, p []string, reply string) {
		t.Helper()
		exec("INSERT INTO "+table+" VALUES (?, ?, ?, ?, ?, ?, ?, ?)", id, author, at, kind, tags, e, p, reply)
	}
	insert("events_permanent", "old", "follower", now-20, 3, "[]", nil, []string{"old-target"}, "")
	insert("events_permanent", "b", "follower", now-10, 3, "[]", nil, []string{"losing-target"}, "")
	insert("events_permanent", "a", "follower", now-10, 3, "[]", nil, []string{"target"}, "")
	insert("events_permanent", "dead-contact", "dead-follower", now, 3, "[]", nil, []string{"target"}, "")
	// Across tiers, background merges cannot hide duplicate ingestion from the test.
	for _, table := range []string{"events_archive", "events_social"} {
		insert(table, "zap", "zapper", now, 9735, `[["amount","21000"]]`, []string{"note"}, nil, "")
	}
	insert("events_social", "reaction", "reactor", now, 7, "[]", []string{"note"}, nil, "")
	insert("events_social", "repost", "reposter", now, 6, "[]", []string{"note"}, nil, "")
	insert("events_social", "reply", "reply-author", now, 1, "[]", nil, nil, "note")
	insert("events_social", "dead-post", "dead-author", now, 1, "[]", nil, nil, "note")
	insert("events_social", "dead-zap", "dead-zapper", now, 9735, `[["amount","90000"]]`, []string{"note"}, nil, "")
	exec(`INSERT INTO tombstones VALUES ('dead-contact'), ('dead-post'), ('dead-zap')`)
	exec(`SYSTEM RELOAD DICTIONARY tombstone_dict`)
	s := New(ch, slog.Default())
	for _, refresh := range []func(context.Context) error{s.RefreshFollowers, s.RefreshFollowers, s.RefreshNoteMonthly, s.RefreshDaily, s.RefreshDailyActive} {
		if err := refresh(ctx); err != nil {
			t.Fatal(err)
		}
	}
	for target, want := range map[string]int64{"target": 1, "old-target": 0, "losing-target": 0} {
		got, err := s.Followers(ctx, target)
		if err != nil || got != want {
			t.Fatalf("followers %s = %d, %v; want %d", target, got, err, want)
		}
	}
	eng, err := s.Engagement(ctx, "note")
	if err != nil {
		t.Fatal(err)
	}
	for metric, want := range map[string]int64{"zap": 1, "zap_sats": 21000, "reaction": 1, "repost": 1, "reply": 1} {
		if eng[metric] != want {
			t.Errorf("%s = %d, want %d", metric, eng[metric], want)
		}
	}
	daily, err := s.Daily(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	totals := map[string]uint64{}
	for _, r := range daily {
		totals[r.Metric] += r.Value
	}
	for _, metric := range []string{"posts", "reactions", "reposts", "zaps"} {
		if totals[metric] != 1 {
			t.Errorf("daily %s = %d, want 1", metric, totals[metric])
		}
	}
	dau, err := s.DAU(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	var active uint64
	for _, r := range dau {
		active += r.Active
	}
	if active != 5 {
		t.Errorf("DAU = %d, want 5", active)
	}
}
