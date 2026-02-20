# Sports props app (PrizePicks + Underdog, NBA)

Local app to scrape NBA prop lines from PrizePicks and Underdog and view them in a dashboard.

## Setup

1. **Python 3.10+** and a virtualenv (recommended):

   ```bash
   cd sports-props-app
   python -m venv .venv
   .venv\Scripts\activate   # Windows
   ```

2. **Install dependencies:**

   ```bash
   pip install -r requirements.txt
   playwright install chromium
   ```

3. **Initialize the database** (if you haven’t already):

   ```bash
   cd backend
   python db.py
   cd ..
   ```

## Run PrizePicks scraper

From the **project root** (`sports-props-app`):

```bash
python -m scraper.adapters.prizepicks_nba
```

- Use `--headed` to see the browser window:  
  `python -m scraper.adapters.prizepicks_nba --headed`

- Data is written to `backend/data/props.db`.

If the site structure changes or you get no data, open https://www.prizepicks.com/category/nba in DevTools, check Network for API calls that return props, and adjust parsing in `scraper/adapters/prizepicks_nba.py` (or add DOM selectors).

## Next steps

- Underdog NBA adapter
- FastAPI backend + scheduler
- React dashboard
