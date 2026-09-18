//go:build integration

package store

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/nbd-wtf/go-nostr"
)

type tombCountingConn struct {
	driver.Conn
	inserts atomic.Int32
	reloads atomic.Int32
}

func (c *tombCountingConn) PrepareBatch(ctx context.Context, q string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
	c.inserts.Add(1)
	return c.Conn.PrepareBatch(ctx, q, opts...)
}
func (c *tombCountingConn) Exec(ctx context.Context, q string, args ...any) error {
	if strings.HasPrefix(q, "SYSTEM RELOAD") {
		c.reloads.Add(1)
	}
	return c.Conn.Exec(ctx, q, args...)
}
func TestTombstoneIntegrationCoalescesAndHides(t *testing.T) {
	s, _, teardown := setupStore(t)
	defer teardown()
	ctx := context.Background()
	evt := signEvent(t, nostr.GeneratePrivateKey(), 1, "tombstone writer integration", nostr.Tags{}, 0)
	if err := s.SaveEvent(ctx, evt); err != nil {
		t.Fatal(err)
	}
	if err := s.FlushAll(); err != nil {
		t.Fatal(err)
	}
	ch, err := s.QueryEvents(ctx, nostr.Filter{IDs: []string{evt.ID}})
	if err != nil {
		t.Fatal(err)
	}
	if len(drain(ch)) != 1 {
		t.Fatal("event not visible before retirement")
	}
	conn := &tombCountingConn{Conn: s.ch}
	w := newTombstoneWriter(conn, testLogger())
	w.start()
	defer w.stop()
	ids := tombTestIDs(100)
	ids[0] = evt.ID
	for _, id := range ids {
		if err := w.retire([]string{id}, "nip09", evt.PubKey); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(2 * time.Second)
	var found uint8
	for time.Now().Before(deadline) {
		if err := s.ch.QueryRow(ctx, "SELECT dictHas('tombstone_dict', ?)", evt.ID).Scan(&found); err != nil {
			t.Fatal(err)
		}
		if found == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if found != 1 {
		t.Fatal("dictionary did not hide event within two seconds")
	}
	var count, parts uint64
	if err := s.ch.QueryRow(ctx, "SELECT count() FROM tombstones").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if err := s.ch.QueryRow(ctx, "SELECT count() FROM system.parts WHERE database=currentDatabase() AND table='tombstones' AND active").Scan(&parts); err != nil {
		t.Fatal(err)
	}
	if count != 100 || parts > 2 || conn.inserts.Load() > 2 || conn.reloads.Load() > 2 {
		t.Fatalf("rows=%d parts=%d inserts=%d reloads=%d", count, parts, conn.inserts.Load(), conn.reloads.Load())
	}
	ch, err = s.QueryEvents(ctx, nostr.Filter{IDs: []string{evt.ID}})
	if err != nil {
		t.Fatal(err)
	}
	if len(drain(ch)) != 0 {
		t.Fatal("tombstoned event returned by reloaded read")
	}
	t.Logf("100 singleton retire calls: rows=%d parts=%d inserts=%d reloads=%d", count, parts, conn.inserts.Load(), conn.reloads.Load())
}
func TestSchemaIntegrationMigrationAndSanity(t *testing.T) {
	s, _, teardown := setupStore(t)
	defer teardown()
	ctx := context.Background()
	for _, tier := range activeTiers {
		for _, q := range []string{
			"ALTER TABLE events_" + tier + " DROP INDEX idx_pubkey",
			"ALTER TABLE events_" + tier + " ADD INDEX idx_content content TYPE ngrambf_v1(3, 256, 2, 0) GRANULARITY 4",
		} {
			if err := s.ch.Exec(ctx, q); err != nil {
				t.Fatal(err)
			}
		}
	}
	for i := 0; i < 2; i++ {
		if err := s.initSchema(ctx); err != nil {
			t.Fatal(err)
		}
	}
	for _, tier := range activeTiers {
		var pubkey, content uint64
		if err := s.ch.QueryRow(ctx, "SELECT countIf(name='idx_pubkey'), countIf(name='idx_content') FROM system.data_skipping_indices WHERE database=currentDatabase() AND table=?", "events_"+tier).Scan(&pubkey, &content); err != nil {
			t.Fatal(err)
		}
		if pubkey != 1 || content != 0 {
			t.Fatalf("%s: pubkey=%d content=%d", tier, pubkey, content)
		}
	}
	if err := s.ch.Exec(ctx, "DROP DICTIONARY tombstone_dict"); err != nil {
		t.Fatal(err)
	}
	broken := strings.Replace(tombstonesDDL, "TABLE 'tombstones'", "TABLE 'missing_tombstones'", 1)
	for _, q := range strings.Split(broken, ";") {
		if strings.TrimSpace(q) != "" {
			if err := s.ch.Exec(ctx, q); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := s.initSchema(ctx); err == nil || !strings.Contains(err.Error(), "dictionary startup sanity probe") {
		t.Fatalf("broken dictionary startup error: %v", err)
	} else {
		t.Log(fmt.Sprintf("broken dictionary correctly rejected: %v", err))
	}
}
