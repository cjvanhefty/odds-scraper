# Prize Picks Odds Scraper

Scrapes player projections and prop lines from [Prize Picks](https://app.prizepicks.com) via their public API.

## Setup

```bash
pip install -r requirements.txt
```

## Usage

```bash
# Scrape NBA props (default)
python prizepicks_scraper.py

# Scrape a specific league
python prizepicks_scraper.py -l nfl
python prizepicks_scraper.py -l nhl
python prizepicks_scraper.py -l mlb

# Use raw league ID
python prizepicks_scraper.py --league-id 20

# Save to CSV or JSON
python prizepicks_scraper.py -o props.csv
python prizepicks_scraper.py -o props.json

# Upsert to SQL Server (Props database)
# Projections: prizepicks_projection_stage → prizepicks_projection (history on line change)
# Players: prizepicks_player_stage → prizepicks_player (no history)
python prizepicks_scraper.py -l nba --db

# DB options: --db-server, --db-user, --db-password (or PROPS_DB_* env vars)
```

### Supported leagues

| League | Flag  | ID  |
|--------|-------|-----|
| NBA    | `nba` | 7   |
| NFL    | `nfl` | 9   |
| NHL    | `nhl` | 8   |
| MLB    | `mlb` | 2   |
| CFB    | `cfb` | 15  |
| CBB    | `cbb` | 20  |
| WNBA   | `wnba`| 3   |
| PGA    | `pga` | 1   |
| Tennis | `tennis` | 5 |
| MMA    | `mma` | 12  |
| EPL    | `epl` | 14  |

## Output

Each projection includes:

- **player_name** – Player display name
- **stat_type** – Prop type (e.g. Points, Rebounds, Assists)
- **line** – Projected line (e.g. 24.5)
- **description** – Game matchup (e.g. BOS@DET)
- **start_time** – ISO timestamp
- **team** – Team abbreviation (e.g. GSW)

## 403 Forbidden

The Prize Picks API often blocks direct HTTP requests (403). Try these steps:

### 1. Browser mode (auto-retry or `--browser`)

```bash
pip install playwright
playwright install chromium
python prizepicks_scraper.py --browser -o props.csv
```

The scraper auto-retries with Playwright when it gets 403 (if Playwright is installed).

### 2. Use your existing Chrome (recommended if you're already logged in)

If you have Chrome open with Prize Picks and you're logged in:

1. **Close all Chrome windows**, then start Chrome with remote debugging:
   ```powershell
   & "C:\Program Files\Google\Chrome\Application\chrome.exe" --remote-debugging-port=9222
   ```
2. Open [app.prizepicks.com](https://app.prizepicks.com) and log in
3. In a new terminal, run:
   ```bash
   python prizepicks_scraper.py --connect http://localhost:9222 -o props.csv
   ```

The script connects to your Chrome, opens a new tab, and captures the projections (using your logged-in session).

### 3. Visible browser + manual login (`--headed`)

If the app requires login, run with a visible browser and log in when it opens:

```bash
python prizepicks_scraper.py --browser --headed -o props.csv
```

A Chrome window will open. Log in to Prize Picks if prompted. The script waits ~15 seconds for the projections to load.

### 4. Persistent profile (login once, reuse)

```bash
python prizepicks_scraper.py --browser --persistent --headed -o props.csv
```

First run: browser opens, log in. Later runs (with or without `--headed`): uses saved login.

### 5. Use cookies from a logged-in session

Export your browser cookies and pass them:

1. Log in at [app.prizepicks.com](https://app.prizepicks.com) in Chrome
2. Use an extension like "Get cookies.txt" or "EditThisCookie" to export cookies
3. Save as JSON in Playwright format: `[{"name":"...","value":"...","domain":".prizepicks.com","path":"/"}]`
4. Run: `python prizepicks_scraper.py --browser --cookies cookies.json -o props.csv`

### 6. Try a different network

Sometimes 403 is IP-based. Try a different Wi‑Fi or disable VPN.

## API Notes

- Uses `api.prizepicks.com/projections` (no browser required when not blocked)
- Cloudflare may block some networks; use `--browser` to bypass
