// Package stats maintains the snapshot/aggregate tables (ANALYSIS.md §3e) and
// exposes query helpers for the stats API. Refresh jobs recompute from FINAL
// reads with count(DISTINCT id), so duplicate ingests never inflate them.
package stats

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// Service runs the periodic refresh jobs and answers stats queries.
type Service struct {
	ch  driver.Conn
	log *slog.Logger
}

// New constructs a stats Service over the given ClickHouse connection.
func New(ch driver.Conn, log *slog.Logger) *Service {
	return &Service{ch: ch, log: log}
}

// Run starts the periodic refresh jobs and blocks until ctx is canceled.
// - followers: every 5 min (read on every profile render, so keep fresh)
// - monthly/daily rollups: hourly (reprocess current+previous bucket)
func (s *Service) Run(ctx context.Context) {
	// initial refresh so stats exist immediately after start
	s.refreshAll(ctx)

	followerTick := time.NewTicker(5 * time.Minute)
	hourly := time.NewTicker(time.Hour)
	defer followerTick.Stop()
	defer hourly.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-followerTick.C:
			if err := s.RefreshFollowers(ctx); err != nil {
				s.log.Warn("follower refresh failed", "err", err)
			}
		case <-hourly.C:
			s.refreshAll(ctx)
		}
	}
}

func (s *Service) refreshAll(ctx context.Context) {
	if err := s.RefreshFollowers(ctx); err != nil {
		s.log.Warn("follower refresh failed", "err", err)
	}
	if err := s.RefreshNoteMonthly(ctx); err != nil {
		s.log.Warn("note-monthly refresh failed", "err", err)
	}
	if err := s.RefreshDaily(ctx); err != nil {
		s.log.Warn("daily refresh failed", "err", err)
	}
	if err := s.RefreshDailyActive(ctx); err != nil {
		s.log.Warn("daily-active refresh failed", "err", err)
	}
}

// RefreshFollowers recomputes per-author follower counts from the latest kind-3
// contact list of each author. author_follower_counts is ReplacingMergeTree so
// we REPLACE INTO (delete old + insert).
func (s *Service) RefreshFollowers(ctx context.Context) error {
	// TRUNCATE then repopulate; cheap relative to the scan.
	if err := s.ch.Exec(ctx, "TRUNCATE TABLE author_follower_counts"); err != nil {
		return fmt.Errorf("truncate: %w", err)
	}
	q := `
INSERT INTO author_follower_counts (pubkey, followers)
SELECT p_tag AS pubkey, count(DISTINCT author) AS followers
FROM (
  SELECT pubkey AS author, arrayJoin(tag_p) AS p_tag
  FROM (SELECT * FROM events_permanent FINAL WHERE kind = 3 ORDER BY created_at DESC LIMIT 1 BY pubkey)
)
GROUP BY p_tag`
	return s.ch.Exec(ctx, q)
}

// RefreshNoteMonthly recomputes per-note-per-month engagement for the current
// and previous month (stragglers); older months are frozen. §3e.
func (s *Service) RefreshNoteMonthly(ctx context.Context) error {
	if err := s.ch.Exec(ctx,
		"DELETE FROM stats_note_monthly WHERE month >= toStartOfMonth(addMonths(today(), -1))"); err != nil {
		return fmt.Errorf("delete: %w", err)
	}
	q := `
INSERT INTO stats_note_monthly (note_id, month, metric, count, sats)
SELECT note_id, month, metric,
       count(DISTINCT id) AS count,
       sum(if(metric = 'zap', toUInt64OrZero(extract(tags_raw, '"amount","([0-9]+)"')), 0)) AS sats
FROM (
  SELECT
    multiIf(kind = 1, reply_to, tag_e[1]) AS note_id,
    toStartOfMonth(toDateTime(created_at)) AS month,
    multiIf(kind = 7, 'reaction', kind IN (6, 16), 'repost', kind = 1, 'reply', kind = 9735, 'zap', '') AS metric,
    id, tags_raw
  FROM events_all
  WHERE kind IN (1, 6, 7, 16, 9735)
    AND ((kind = 1 AND reply_to != '') OR (kind IN (6, 7, 16, 9735) AND length(tag_e) >= 1))
    AND created_at >= toUnixTimestamp(toStartOfMonth(addMonths(today(), -1)))
)
WHERE metric != '' AND note_id != ''
GROUP BY note_id, month, metric`
	return s.ch.Exec(ctx, q)
}

// RefreshDaily recomputes the daily network rollups for the last 3 days.
func (s *Service) RefreshDaily(ctx context.Context) error {
	if err := s.ch.Exec(ctx, "DELETE FROM stats_daily WHERE day >= today() - 3"); err != nil {
		return fmt.Errorf("delete: %w", err)
	}
	q := `
INSERT INTO stats_daily (day, metric, value)
SELECT day, metric, count(DISTINCT id) AS value
FROM (
  SELECT toDate(toDateTime(created_at)) AS day,
         multiIf(kind = 1, 'posts', kind = 7, 'reactions', kind IN (6, 16), 'reposts', kind = 9735, 'zaps', '') AS metric,
         id
  FROM events_all
  WHERE kind IN (1, 6, 7, 16, 9735) AND created_at >= toUnixTimestamp(today() - 3)
)
WHERE metric != ''
GROUP BY day, metric`
	return s.ch.Exec(ctx, q)
}

// RefreshDailyActive recomputes DAU (distinct active authors) for last 3 days.
func (s *Service) RefreshDailyActive(ctx context.Context) error {
	if err := s.ch.Exec(ctx, "DELETE FROM stats_daily_active WHERE day >= today() - 3"); err != nil {
		return fmt.Errorf("delete: %w", err)
	}
	q := `
INSERT INTO stats_daily_active (day, authors)
SELECT toDate(toDateTime(created_at)) AS day, uniqState(pubkey) AS authors
FROM events_all
WHERE created_at >= toUnixTimestamp(today() - 3)
GROUP BY day`
	return s.ch.Exec(ctx, q)
}

// --- query helpers (used by the HTTP API) ---

// Engagement returns the all-time per-metric engagement for a note.
func (s *Service) Engagement(ctx context.Context, noteID string) (map[string]int64, error) {
	rows, err := s.ch.Query(ctx,
		`SELECT metric, sum(count), sum(sats) FROM stats_note_monthly WHERE note_id = ? GROUP BY metric`,
		noteID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var metric string
		var c, ss uint64
		if err := rows.Scan(&metric, &c, &ss); err != nil {
			return nil, err
		}
		out[metric] = int64(c)
		out[metric+"_sats"] = int64(ss)
	}
	return out, nil
}

// Daily returns per-day metric totals for the last `days` days.
func (s *Service) Daily(ctx context.Context, days int) ([]DailyRow, error) {
	rows, err := s.ch.Query(ctx,
		`SELECT day, metric, sum(value) FROM stats_daily
		 WHERE day >= today() - ? GROUP BY day, metric ORDER BY day DESC, metric`,
		int32(days))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DailyRow
	for rows.Next() {
		var r DailyRow
		if err := rows.Scan(&r.Day, &r.Metric, &r.Value); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, nil
}

// DAU returns daily active authors for the last `days` days.
func (s *Service) DAU(ctx context.Context, days int) ([]DAURow, error) {
	rows, err := s.ch.Query(ctx,
		`SELECT day, uniqMerge(authors) FROM stats_daily_active
		 WHERE day >= today() - ? GROUP BY day ORDER BY day DESC`,
		int32(days))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DAURow
	for rows.Next() {
		var r DAURow
		if err := rows.Scan(&r.Day, &r.Active); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, nil
}

// Followers returns the follower count for a pubkey (0 if unknown/not yet
// snapshotted). A missing row is a known "0", not an error.
func (s *Service) Followers(ctx context.Context, pubkey string) (int64, error) {
	var n uint64
	err := s.ch.QueryRow(ctx,
		`SELECT followers FROM author_follower_counts FINAL WHERE pubkey = ?`, pubkey).Scan(&n)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, err
	}
	return int64(n), nil
}

type DailyRow struct {
	Day    time.Time
	Metric string
	Value  uint64
}

type DAURow struct {
	Day    time.Time
	Active uint64
}
