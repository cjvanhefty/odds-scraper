"""
Cross-provider stat normalization for PrizePicks, Underdog, and Parlay Play.

- normalize_stat_basic: map API codes / short tokens to PrizePicks-style labels (for scraper ingest).
- normalize_for_join: same labels plus internal join buckets (e.g. Blocks + Blocked Shots) for matching.
- parlay_match_league_id_for_prizepicks: PrizePicks `league_id` -> Parlay `match.league_id` for joins.
- parlay_match_league_id_to_prizepicks: inverse (Parlay match id -> PrizePicks id) for unified storage.
  Parlay and PP use different numbers (e.g. Parlay NBA=2 vs PP NBA=7; Parlay MLB=7 vs PP MLB=2).
"""

from __future__ import annotations

import os
import re


def _alnum_key(s: str | None) -> str:
    return re.sub(r"[^a-z0-9]", "", (s or "").strip().lower())


def apply_join_aliases(s: str) -> str:
    """Merge equivalent *string* labels used by different books into stable join tokens."""
    t = (s or "").strip()
    if not t:
        return ""
    if t in ("Blocks", "Blocked Shots"):
        return "Blocks__Blocked_Shots"
    if t in ("Double Doubles", "Double-Doubles", "Double-Double", "Double Double"):
        return "Double_Doubles"
    if t in ("Triple Doubles", "Triple-Doubles", "Triple-Double"):
        return "Triple_Doubles"
    if t.startswith("Blks+Stls") or t in ("Blocks + Steals", "Blocks+Steals"):
        return "Blks_Stls"
    if t in ("3-PT Attempted", "3 Pointers Attempted", "3s Attempted"):
        return "FG3A"
    if t in ("3 Pointers", "3 Pointers Made", "3-PT Made", "3-Pointers Made"):
        return "FG3M"
    if t in ("Hits+Runs+RBIs", "Hits + Runs + RBIs"):
        return "Hits+Runs+RBIs"
    if t in ("Shots On Target", "Shots on Target"):
        return "Shots On Target"
    if t in ("Goal + Assist", "Goals + Assists"):
        return "Goal + Assist"
    # Underdog soccer often labels this as "Passes"; PrizePicks uses "Passes Attempted".
    if t in ("Passes Attempted", "Passes"):
        return "Passes Attempted"
    return t


# Alphanumeric keys -> PrizePicks-like display / join *base* label (before apply_join_aliases).
CANONICAL_STAT_BY_ALNUM: dict[str, str] = {
    # --- Core basketball ---
    "pts": "Points",
    "points": "Points",
    "point": "Points",
    "reb": "Rebounds",
    "rebounds": "Rebounds",
    "ast": "Assists",
    "assists": "Assists",
    "stl": "Steals",
    "steals": "Steals",
    "blk": "Blocks",
    "blocks": "Blocks",
    "blockedshots": "Blocked Shots",
    "tov": "Turnovers",
    "to": "Turnovers",
    "turnovers": "Turnovers",
    "turnover": "Turnovers",
    "blksstls": "Blks+Stls",
    "blockssteals": "Blks+Stls",
    "stocks": "Blks+Stls",
    "fantasypoints": "Fantasy Score",
    "fantasyscore": "Fantasy Score",
    "ptsreb": "Pts+Rebs",
    "ptsrebs": "Pts+Rebs",
    "ptsast": "Pts+Asts",
    "ptsasts": "Pts+Asts",
    "rebast": "Rebs+Asts",
    "rebsasts": "Rebs+Asts",
    "ptsrebast": "Pts+Rebs+Asts",
    "ptsrebsasts": "Pts+Rebs+Asts",
    "ptsrebsast": "Pts+Rebs+Asts",
    "pointsreboundsassists": "Pts+Rebs+Asts",
    "pointsrebounds": "Pts+Rebs",
    "pointsassists": "Pts+Asts",
    "reboundsassists": "Rebs+Asts",
    "doubledoubles": "Double-Double",
    "doubledouble": "Double-Double",
    "tripledoubles": "Triple-Double",
    "oreb": "Offensive Rebounds",
    "dreb": "Defensive Rebounds",
    "3pm": "3-PT Made",
    "3ptm": "3-PT Made",
    "3ptmade": "3-PT Made",
    "3pmade": "3-PT Made",
    "3pointersmade": "3 Pointers Made",
    "threepointsmade": "3 Pointers Made",
    "threes": "3 Pointers Made",
    "3pt": "3 Pointers",
    "3sattempted": "3-PT Attempted",
    "threepointsatt": "3-PT Attempted",
    "threepointersattempted": "3 Pointers Attempted",
    "attemptedthrees": "3-PT Attempted",
    "fgattempted": "FG Attempted",
    "fieldgoalsatt": "FG Attempted",
    "fgmade": "FG Made",
    "ftmade": "Free Throws Made",
    "freethrowsmade": "Free Throws Made",
    "freethrowsattempted": "Free Throws Attempted",
    "twopointersmade": "Two Pointers Made",
    "twopointersattempted": "Two Pointers Attempted",
    # Parlay bb_* challenge codes
    "bbpoints": "Points",
    "bbrebounds": "Rebounds",
    "bbassists": "Assists",
    "bbsteals": "Steals",
    "bbblocks": "Blocks",
    "bbturnovers": "Turnovers",
    "bbpersonal": "Personal Fouls",
    "bbdreb": "Defensive Rebounds",
    "bboreb": "Offensive Rebounds",
    "bbfgmade": "FG Made",
    "bbfgattempted": "FG Attempted",
    "bbtwopointersmade": "Two Pointers Made",
    "bbtwopointersattempted": "Two Pointers Attempted",
    "bbfreethrowsmade": "Free Throws Made",
    "bbfreethrowsattempted": "Free Throws Attempted",
    "bbptsreb": "Pts+Rebs",
    "bbptsast": "Pts+Asts",
    "bbptsrebast": "Pts+Rebs+Asts",
    "bbrebast": "Rebs+Asts",
    "bbdd": "Double-Double",
    "bbtd": "Triple-Double",
    "bbparlaypoints": "Fantasy Score",
    "bbfirstbasket": "First Point Scorer",
    "bbthreepointersmade": "3 Pointers Made",
    "bbthreepointersattempted": "3-PT Attempted",
    "bbthreepointfieldgoalsattempted": "3-PT Attempted",
    "bbfg3a": "3-PT Attempted",
    "bb3ptattempted": "3-PT Attempted",
    # Underdog period markets (keep distinct from full-game PP markets)
    "ptsrebsasts1h": "1H Pts + Rebs + Asts",
    "period1points": "1Q Points",
    "period12points": "1H Points",
    "period1rebounds": "1Q Rebounds",
    "period12rebounds": "1H Rebounds",
    "period1assists": "1Q Assists",
    "period12assists": "1H Assists",
    "period1threepointsmade": "1Q 3-Pointers Made",
    "period12threepointsmade": "1H 3-Pointers Made",
    "period1ptsrebsasts": "1Q Pts + Rebs + Asts",
    "period12ptsrebsasts": "1H Pts + Rebs + Asts",
    # --- MLB (PrizePicks labels) ---
    "babhrr": "Hits+Runs+RBIs",
    "hitsrunsrbis": "Hits+Runs+RBIs",
    "hrr": "Hits+Runs+RBIs",
    "babtotalbases": "Total Bases",
    "babhits": "Hits",
    "babrbi": "RBIs",
    "babruns": "Runs",
    "babsingles": "Singles",
    "babdoubles": "Doubles",
    "babtriples": "Triples",
    "babhomeruns": "Home Runs",
    "babstolenbases": "Stolen Bases",
    "babwalks": "Walks",
    "babpitchingstrikeouts": "Pitcher Strikeouts",
    "babstrikeouts": "Hitter Strikeouts",
    "babpitchingouts": "Pitching Outs",
    "babpitchesthrown": "Pitches Thrown",
    "babhitsallowed": "Hits Allowed",
    "babwalksallowed": "Walks Allowed",
    "babparlaypoints": "Hitter Fantasy Score",
    "earnedrunsallowed": "Earned Runs Allowed",
    "firstinningrunsallowed": "1st Inning Runs Allowed",
    # --- Soccer (PrizePicks labels; Parlay uses soc_* codes) ---
    "socshots": "Shots",
    "socshotsontarget": "Shots On Target",
    "socshotsongoal": "Shots On Target",
    "soctackles": "Tackles",
    "socgoals": "Goals",
    "socassists": "Assists",
    "socfouls": "Fouls",
    "socfouled": "Fouled",
    "soccards": "Cards",
    "socgoalassist": "Goal + Assist",
    "socdribblesattempted": "Attempted Dribbles",
    "soccrosses": "Crosses",
    "socgksaves": "Goalie Saves",
    "socoffsides": "Offsides",
    # --- Football-ish (PrizePicks CFB sample) ---
    "passyards": "Pass Yards",
    "passingyards": "Pass Yards",
    "passtds": "Pass TDs",
    "passingtds": "Pass TDs",
    "rushyards": "Rush Yards",
    "rushingyards": "Rush Yards",
    "rushtds": "Rush TDs",
    "rushingtds": "Rush TDs",
    "recyards": "Receiving Yards",
    "receivingyards": "Receiving Yards",
    "rectds": "Rec TDs",
    "receivingtds": "Rec TDs",
    "rushtrectds": "Rush + Rec TDs",
    "rushrectds": "Rush + Rec TDs",
    # --- UFC ---
    "significantstrikes": "Significant Strikes",
    "takedowns": "Takedowns",
    "tackledowns": "Takedowns",
    "totalrounds": "Total Rounds",
    "fighttimemins": "Fight Time (Mins)",
    # --- Tennis (Parlay ten_*) ---
    "tentotalgames": "Total Games",
    "tengameswon": "Total Games Won",
    "tensetswon": "Total Sets",
    "tenaces": "Aces",
    # --- Esports (Parlay es_*) ---
    "escsgokills": "MAPS 1-2 Kills",
    "escsgoheadshots": "MAPS 1-2 Headshots",
    "eslolkills": "MAPS 1-2 Kills",
    "esvalkills": "MAPS 1-2 Kills",
    "esdota2kills": "MAPS 1-3 Kills",
    # Cricket
    "crickruns": "Runs",
    "crickfours": "Fours",
    "cricksixes": "Sixes",
}


def normalize_stat_basic(stat_type_name: str | None) -> str:
    """Normalize a provider stat string/code to a PrizePicks-like label for storage/display."""
    raw = (stat_type_name or "").strip()
    if not raw:
        return ""
    mapped = CANONICAL_STAT_BY_ALNUM.get(_alnum_key(raw))
    return mapped if mapped is not None else raw


def normalize_for_join(stat_type_name: str | None) -> str:
    """Normalize for cross-book joins (includes Blocks/3PT buckets)."""
    s = (stat_type_name or "").strip()
    if not s:
        return ""
    pre = apply_join_aliases(s)
    if pre != s:
        return pre
    mapped = CANONICAL_STAT_BY_ALNUM.get(_alnum_key(s))
    if mapped is not None:
        return apply_join_aliases(mapped)
    return s


# PrizePicks league_id -> Parlay Play `parlay_play_match.league_id` (NOT the same numbering).
# None: umbrella leagues (soccer) or books not present / ambiguous.
PRIZEPICKS_TO_PARLAY_MATCH_LEAGUE_ID: dict[int, int | None] = {
    7: 2,  # NBA
    2: 7,  # MLB
    8: 3,  # NHL
    20: 6,  # CBB
    14: 41,  # EPL (PrizePicks)
    1: 535,  # PGA
    12: 4,  # UFC
    82: None,  # Soccer umbrella -> many Parlay leagues
    # NFL / CFB: Parlay `parlay_play_league.id` is not the same as PrizePicks. If your DB has no row yet,
    # run Parlay ETL then: SELECT id, league_name_short, slug FROM dbo.parlay_play_league ORDER BY id;
    # or set PROPS_PARLAY_MATCH_LEAGUE_ID_NFL / PROPS_PARLAY_MATCH_LEAGUE_ID_CFB in .env (see .env.example).
    9: None,  # NFL (PrizePicks)
    15: None,  # CFB (PrizePicks)
}


def _apply_env_parlay_league_overrides(m: dict[int, int | None]) -> None:
    """Allow configuring Parlay match.league_id for football without a code change."""
    overrides = (
        ("PROPS_PARLAY_MATCH_LEAGUE_ID_NFL", 9),
        ("PROPS_PARLAY_MATCH_LEAGUE_ID_CFB", 15),
    )
    for env_name, pp_league_id in overrides:
        raw = os.environ.get(env_name, "").strip()
        if not raw:
            continue
        try:
            m[pp_league_id] = int(raw)
        except ValueError:
            pass


_apply_env_parlay_league_overrides(PRIZEPICKS_TO_PARLAY_MATCH_LEAGUE_ID)

# Labels for PrizePicks `league_id` (same numeric space as `sportsbook_projection.league_id`
# after Parlay→PP mapping). Used when the DB has no `prizepicks_player.league` text (e.g. unified store).
PRIZEPICKS_LEAGUE_DISPLAY_NAME: dict[int, str] = {
    1: "PGA",
    2: "MLB",
    # PrizePicks uses extra league rows for MLB partial-game / innings markets (not full 9-inning slate).
    231: "MLB (partial / innings)",
    3: "WNBA",
    5: "Tennis",
    7: "NBA",
    8: "NHL",
    9: "NFL",
    12: "UFC",
    14: "EPL",
    15: "CFB",
    20: "CBB",
    82: "Soccer",
}


def prizepicks_league_display_name(league_id: int | None) -> str | None:
    if league_id is None:
        return None
    try:
        k = int(league_id)
    except (TypeError, ValueError):
        return None
    return PRIZEPICKS_LEAGUE_DISPLAY_NAME.get(k)


def _invert_parlay_match_to_prizepicks_leagues() -> dict[int, int]:
    """Parlay `parlay_play_match.league_id` -> PrizePicks `league_id` (same space as PP/UD in sportsbook_projection)."""
    inv: dict[int, int] = {}
    for pp_id, parlay_id in PRIZEPICKS_TO_PARLAY_MATCH_LEAGUE_ID.items():
        if parlay_id is not None:
            inv[parlay_id] = pp_id
    return inv


# Built after env overrides so NFL/CFB Parlay ids map back correctly.
PARLAY_MATCH_LEAGUE_ID_TO_PRIZEPICKS: dict[int, int] = _invert_parlay_match_to_prizepicks_leagues()


def parlay_match_league_id_to_prizepicks(parlay_match_league_id: int | None) -> int | None:
    """Map Parlay Play match.league_id into PrizePicks league_id (None if unknown / unmapped)."""
    if parlay_match_league_id is None:
        return None
    try:
        k = int(parlay_match_league_id)
    except (TypeError, ValueError):
        return None
    return PARLAY_MATCH_LEAGUE_ID_TO_PRIZEPICKS.get(k)


def parlay_match_league_id_for_prizepicks(pp_league_id: int | None) -> int | None:
    if pp_league_id is None:
        return None
    try:
        k = int(pp_league_id)
    except (TypeError, ValueError):
        return None
    if k not in PRIZEPICKS_TO_PARLAY_MATCH_LEAGUE_ID:
        return None
    return PRIZEPICKS_TO_PARLAY_MATCH_LEAGUE_ID[k]
