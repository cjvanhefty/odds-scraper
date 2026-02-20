-- Sports props app: PrizePicks + Underdog, NBA
-- SQLite schema

-- Books (sources)
CREATE TABLE IF NOT EXISTS books (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  slug TEXT NOT NULL UNIQUE,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- NBA events (games)
CREATE TABLE IF NOT EXISTS events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  external_id TEXT,
  name TEXT NOT NULL,
  game_date TEXT NOT NULL,
  start_time TEXT,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  UNIQUE(external_id, name, game_date)
);

-- Prop lines: one row per player/stat/book per scrape
CREATE TABLE IF NOT EXISTS prop_lines (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id INTEGER NOT NULL,
  book_id INTEGER NOT NULL,
  player_name TEXT NOT NULL,
  stat_type TEXT NOT NULL,
  line_value REAL NOT NULL,
  multiplier REAL,
  scraped_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (event_id) REFERENCES events(id),
  FOREIGN KEY (book_id) REFERENCES books(id)
);

CREATE INDEX IF NOT EXISTS idx_prop_lines_event_book_player_stat
  ON prop_lines(event_id, book_id, player_name, stat_type);
CREATE INDEX IF NOT EXISTS idx_prop_lines_scraped_at
  ON prop_lines(scraped_at);
CREATE INDEX IF NOT EXISTS idx_events_game_date
  ON events(game_date);

-- Seed books
INSERT OR IGNORE INTO books (id, name, slug) VALUES (1, 'PrizePicks', 'prizepicks');
INSERT OR IGNORE INTO books (id, name, slug) VALUES (2, 'Underdog', 'underdog');
