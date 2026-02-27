"""
scraper.py — VLR.gg match-by-match scraper
Discovers every match from an event page, scrapes per-match player stats,
stores raw match data, then aggregates into the player leaderboard.

CSS selectors mirror the Power Query logic:
  Player:   .mod-active .text-of
  Team:     .mod-active .ge-text-light
  Rating:   .mod-active .mod-stat .mod-both          (first)
  ACS:      .mod-active .mod-stat + .mod-stat .mod-both
  Kills:    .mod-active .mod-vlr-kills .mod-both
  Deaths:   .mod-active .mod-vlr-deaths .mod-both
  Assists:  .mod-active .mod-vlr-assists .mod-both
  KD Diff:  .mod-active .mod-kd-diff .mod-both
  KAST:     .mod-active .mod-kd-diff + .mod-stat .mod-both
  ADR:      .mod-active .mod-combat .mod-both
  FK:       .mod-active .mod-fb .mod-both
  FD:       .mod-active .mod-fd .mod-both
  FK Diff:  .mod-active .mod-fk-diff .mod-both
  Rows:     .mod-active tbody tr
"""

import re
import time
import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.vlr.gg"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://www.vlr.gg/",
}

TEAM_REGION_MAP = {
    # EMEA
    "fnatic": "EMEA", "natus vincere": "EMEA", "navi": "EMEA",
    "team liquid": "EMEA", "liquid": "EMEA", "bbl": "EMEA",
    "karmine corp": "EMEA", "kc": "EMEA", "gentle mates": "EMEA",
    "gm": "EMEA", "team heretics": "EMEA", "heretics": "EMEA",
    "fut esports": "EMEA", "fut": "EMEA", "apeks": "EMEA",
    "nip": "EMEA", "ninjas in pyjamas": "EMEA",
    "vitality": "EMEA", "m8": "EMEA", "loud emea": "EMEA",
    # AMER
    "sentinels": "AMER", "sen": "AMER", "cloud9": "AMER", "c9": "AMER",
    "100 thieves": "AMER", "100t": "AMER", "nrg": "AMER",
    "evil geniuses": "AMER", "eg": "AMER", "furia": "AMER",
    "mibr": "AMER", "leviatán": "AMER", "leviatan": "AMER",
    "loud": "AMER", "kru esports": "AMER", "kru": "AMER",
    "2game esports": "AMER",
    # APAC
    "paper rex": "APAC", "prx": "APAC", "rex regum qeon": "APAC",
    "rrq": "APAC", "drx": "APAC", "global esports": "APAC",
    "ge": "APAC", "team secret": "APAC", "nongshim redforce": "APAC",
    "ns": "APAC", "t1": "APAC", "gen.g": "APAC", "geng": "APAC",
    "talon esports": "APAC", "talon": "APAC", "zeta division": "APAC",
    "bleed esports": "APAC",
    # CN
    "edg": "CN", "edward gaming": "CN", "blg": "CN", "bilibili gaming": "CN",
    "te": "CN", "thunderbird esports": "CN", "wolves": "CN",
    "nova esports": "CN", "fpx": "CN", "funplus phoenix": "CN",
    "tes": "CN", "top esports": "CN",
}


def infer_region(team_name: str, source_region: str = "") -> str:
    if source_region:
        return source_region
    low = team_name.lower()
    for k, v in TEAM_REGION_MAP.items():
        if k in low:
            return v
    return ""


def _get(url: str, retries: int = 3):
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            if r.status_code == 200:
                return BeautifulSoup(r.text, "lxml")
            print(f"[scraper] HTTP {r.status_code} for {url}")
        except Exception as e:
            print(f"[scraper] Request error (attempt {attempt+1}): {e}")
        time.sleep(2 ** attempt)
    return None


def _safe_float(text: str) -> float:
    if not text:
        return 0.0
    text = (text.strip()
            .replace("%", "")
            .replace("+", "")
            .replace("−", "-")
            .replace("\u2212", "-"))
    try:
        return float(text)
    except ValueError:
        return 0.0


def _safe_int(text: str) -> int:
    return int(_safe_float(text))


def _event_id_and_slug(url: str):
    """Return (event_id, slug) from any VLR event URL."""
    m = re.search(r"/event(?:/(?:matches|stats|results))?/(\d+)/?([^/?#]*)", url)
    if m:
        return m.group(1), m.group(2)
    return None, ""


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 1 — DISCOVER MATCHES FROM AN EVENT PAGE
# ═══════════════════════════════════════════════════════════════════════════════

def get_event_match_urls(event_url: str) -> list:
    """
    Given any VLR event URL, return a sorted list of all completed match page URLs.
    """
    event_id, slug = _event_id_and_slug(event_url)
    if not event_id:
        print(f"[scraper] Could not extract event ID from: {event_url}")
        return []

    # Try the dedicated matches listing page (with ?series_id=all to get everything)
    candidates = []
    if slug:
        candidates.append(f"{BASE_URL}/event/matches/{event_id}/{slug}?series_id=all")
    candidates.append(f"{BASE_URL}/event/matches/{event_id}?series_id=all")

    match_urls = set()

    for url in candidates:
        print(f"[scraper] Fetching match list: {url}")
        soup = _get(url)
        if soup is None:
            continue

        # Match links: /123456/team-a-vs-team-b-event-name
        for a in soup.find_all("a", href=re.compile(r"^/\d+/[a-z0-9\-]+")):
            href = a["href"].split("?")[0].split("#")[0].strip()
            if re.match(r"^/\d+/[a-z0-9\-]+$", href):
                match_urls.add(BASE_URL + href)

        if match_urls:
            print(f"[scraper] Found {len(match_urls)} match URLs.")
            return sorted(match_urls)

    print(f"[scraper] Warning: found 0 match URLs for event {event_id}.")
    return []


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 2 — SCRAPE A SINGLE MATCH PAGE
# ═══════════════════════════════════════════════════════════════════════════════

def scrape_match_page(match_url: str, tournament_id: int, source_region: str = ""):
    """
    Scrape a single VLR match page.
    Returns dict with keys: match_url, match_id, team_a, team_b, players (list).
    """
    print(f"[scraper] Scraping match: {match_url}")
    soup = _get(match_url)
    if soup is None:
        return None

    m = re.search(r"/(\d+)/", match_url)
    match_id = m.group(1) if m else re.sub(r"[^a-z0-9]", "_", match_url)

    # Team names from match header
    team_names = []
    for el in soup.select(".match-header-link-name .wf-title-med"):
        name = el.get_text(strip=True)
        if name:
            team_names.append(name)
    team_a = team_names[0] if len(team_names) > 0 else ""
    team_b = team_names[1] if len(team_names) > 1 else ""

    # ── Find the "All Maps" stats section ────────────────────────────────────
    # VLR marks this with data-game-id="all"
    active_section = soup.select_one(".vm-stats-game[data-game-id='all']")

    players = []

    if active_section:
        tbodies = active_section.select("tbody")
        # VLR always has exactly 2 tbody elements: [0]=team_a, [1]=team_b
        # This is the authoritative team assignment — don't rely on text matching later.
        team_by_tbody = {0: team_a, 1: team_b}
        for i, tbody in enumerate(tbodies):
            assigned_team = team_by_tbody.get(i, "")
            for row in tbody.select("tr"):
                p = _parse_row_by_class(row, match_id, match_url, tournament_id, source_region)
                if p:
                    # Overwrite with the correct full team name from the match header
                    p["match_team"] = assigned_team
                    players.append(p)
    
    # Fallback: Power Query selector approach on full page
    if not players:
        players = _parse_power_query_style(soup, match_id, match_url, tournament_id, source_region)
        # In fallback mode, assign teams by position (first 5 = team_a, next 5 = team_b)
        if players:
            mid = len(players) // 2
            for i, p in enumerate(players):
                p["match_team"] = team_a if i < mid else team_b

    # Extract score + format
    score_a, score_b, map_count, match_format, status = _extract_match_score(soup)

    return {
        "match_url":    match_url,
        "match_id":     match_id,
        "team_a":       team_a,
        "team_b":       team_b,
        "score_a":      score_a,
        "score_b":      score_b,
        "map_count":    map_count,
        "match_format": match_format,
        "status":       status,
        "players":      players,
        "_soup":        soup,   # kept for stub extraction; not persisted
    }


def _build_player_id(row, tournament_id: int, ign: str) -> tuple:
    """Return (player_id, vlr_url)."""
    a_tag = row.find("a", href=re.compile(r"/player/"))
    vlr_url = (BASE_URL + a_tag["href"]) if a_tag else ""
    if vlr_url and "/player/" in vlr_url:
        parts = vlr_url.rstrip("/").split("/")
        raw_id = f"{parts[-2]}_{parts[-1]}" if len(parts) >= 2 else parts[-1]
    else:
        raw_id = re.sub(r"[^a-z0-9]", "_", ign.lower())
    return f"t{tournament_id}_{raw_id}", vlr_url


def _parse_row_by_class(row, match_id, match_url, tournament_id, source_region):
    """
    Parse a <tr> from the stats table using Power Query CSS class names.
    This is the primary approach.
    """
    ign_el   = row.select_one(".text-of")
    team_el  = row.select_one(".ge-text-light")
    ign      = ign_el.get_text(strip=True) if ign_el else ""
    if not ign:
        return None
    team_abbr = team_el.get_text(strip=True) if team_el else ""

    player_id, vlr_url = _build_player_id(row, tournament_id, ign)

    # Agent images
    imgs  = row.select("img")
    agent = ", ".join(
        (img.get("alt") or img.get("title") or "").strip()
        for img in imgs if (img.get("alt") or img.get("title"))
    )

    def sel(css, as_float=False):
        el = row.select_one(css)
        txt = el.get_text(strip=True) if el else ""
        return _safe_float(txt) if as_float else _safe_int(txt)

    # Rating & ACS — first two .mod-stat .mod-both elements
    stat_els = row.select(".mod-stat .mod-both")
    rating = _safe_float(stat_els[0].get_text(strip=True)) if len(stat_els) > 0 else 0.0
    acs    = _safe_float(stat_els[1].get_text(strip=True)) if len(stat_els) > 1 else 0.0

    kills        = sel(".mod-vlr-kills .mod-both")
    deaths_el    = row.select_one(".mod-vlr-deaths .mod-both")
    deaths       = _safe_int(deaths_el.get_text(strip=True).lstrip("/")) if deaths_el else 1
    assists      = sel(".mod-vlr-assists .mod-both")
    kd_diff      = sel(".mod-kd-diff .mod-both")

    kast_el = row.select_one(".mod-kd-diff + .mod-stat .mod-both")
    kast_raw = _safe_float(kast_el.get_text(strip=True)) if kast_el else 0.0
    # VLR sometimes returns KAST as a decimal (0.73) instead of percent (73).
    # Normalise: if value is clearly a 0-1 decimal, convert to 0-100.
    kast = kast_raw * 100 if 0 < kast_raw <= 1.5 else kast_raw
    kast_missing = (kast_el is None or kast_el.get_text(strip=True).strip() in ('', '-', '—'))

    adr          = sel(".mod-combat .mod-both", as_float=True)
    first_kills  = sel(".mod-fb .mod-both")
    first_deaths = sel(".mod-fd .mod-both")
    fk_diff      = sel(".mod-fk-diff .mod-both")

    # HS% — .mod-hs .mod-both if present
    hs_el        = row.select_one(".mod-hs .mod-both")
    headshot_pct = _safe_float(hs_el.get_text(strip=True)) if hs_el else 0.0

    # Skip rows that are clearly empty headers
    if kills == 0 and deaths == 0 and acs == 0 and rating == 0:
        return None

    # Detect missing/zero critical stats and flag for manual review
    missing = []
    if kast_missing or kast == 0:
        missing.append("kast")
    if adr == 0:
        missing.append("adr")
    stats_incomplete = 1 if missing else 0

    return {
        "player_id":       player_id,
        "tournament_id":   tournament_id,
        "match_id":        match_id,
        "match_url":       match_url,
        "ign":             ign,
        "team":            team_abbr,
        "team_abbr":       team_abbr,
        "region":          infer_region(team_abbr, source_region),
        "role":            "flex",
        "agent":           agent,
        "rating":          rating,
        "acs":             acs,
        "kills":           kills,
        "deaths":          max(deaths, 1),
        "assists":         assists,
        "kd_diff":         kd_diff,
        "kast":            kast,
        "adr":             adr,
        "headshot_pct":    headshot_pct,
        "first_kills":     first_kills,
        "first_deaths":    first_deaths,
        "fk_diff":         fk_diff,
        "vlr_url":         vlr_url,
        "match_team":      "",  # set by caller from tbody position
        "stats_incomplete": stats_incomplete,
        "missing_fields":  ",".join(missing),
    }


def _parse_power_query_style(soup, match_id, match_url, tournament_id, source_region):
    """
    Fallback: use .mod-active tbody tr rows (Power Query RowSelector).
    """
    players = []
    rows = soup.select(".mod-active tbody tr")
    print(f"[scraper] Fallback selector: {len(rows)} rows in .mod-active tbody")
    for row in rows:
        p = _parse_row_by_class(row, match_id, match_url, tournament_id, source_region)
        if p:
            players.append(p)
    return players


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 3 — AGGREGATE MATCH STATS → PLAYER LEADERBOARD
# ═══════════════════════════════════════════════════════════════════════════════

def aggregate_player_stats(match_player_rows: list) -> list:
    """
    Aggregate per-match player rows into one summary row per player.

    KEY DESIGN: Fantasy points are calculated PER MATCH then SUMMED.
    This is correct because the scoring formula has non-linear interactions
    (ACS*KAST, KAD ratios) that give different results if calculated on
    averaged/summed aggregates vs per-match. This matches the Power Query
    behaviour where each match row had points calculated individually.

    Display stats (kills, deaths, ACS, etc.) are summed/averaged as usual.
    """
    from collections import defaultdict, Counter
    from points import calculate_player_points

    buckets = defaultdict(list)
    for row in match_player_rows:
        buckets[row["player_id"]].append(row)

    players = []
    for pid, rows in buckets.items():
        n    = len(rows)
        base = rows[0]

        # ── Display stats ─────────────────────────────────────────────────
        kills        = sum(r["kills"]        for r in rows)
        deaths       = sum(r["deaths"]       for r in rows)
        assists      = sum(r["assists"]      for r in rows)
        first_kills  = sum(r["first_kills"]  for r in rows)
        first_deaths = sum(r["first_deaths"] for r in rows)
        kd_diff      = sum(r["kd_diff"]      for r in rows)
        fk_diff      = sum(r["fk_diff"]      for r in rows)

        rating       = sum(r["rating"]       for r in rows) / n
        acs          = sum(r["acs"]          for r in rows) / n
        kast         = sum(r["kast"]         for r in rows) / n
        adr          = sum(r["adr"]          for r in rows) / n
        headshot_pct = sum(r["headshot_pct"] for r in rows) / n

        kd_ratio    = round(kills / max(deaths, 1), 3)
        fk_fd_ratio = round(first_kills / max(first_deaths, 1), 3)

        agent_counter = Counter(r["agent"] for r in rows if r["agent"])
        agent = agent_counter.most_common(1)[0][0] if agent_counter else ""

        # ── Points: calculate per match, then SUM ─────────────────────────
        # Each match row uses its own ACS/KAST/ADR/Rating (per-match averages)
        # so the non-linear formula terms are correct per match.
        role = base.get("role", "flex")
        total_base  = 0.0
        total_role  = 0.0
        total_pts   = 0.0
        has_incomplete = any(r.get("stats_incomplete", 0) for r in rows)
        for r in rows:
            # Skip rows flagged as incomplete — they would corrupt the points calc
            if r.get("stats_incomplete", 0):
                print(f"[scraper] Skipping incomplete stats for {r['ign']} in match {r['match_id']} (missing: {r.get('missing_fields','')})")
                continue
            pdata = {
                "role":         role,
                "kills":        r["kills"],
                "deaths":       r["deaths"],
                "assists":      r["assists"],
                "acs":          r["acs"],
                "kast":         r["kast"],
                "adr":          r["adr"],
                "first_kills":  r["first_kills"],
                "first_deaths": r["first_deaths"],
                "rating":       r["rating"],
            }
            bp, rp, tp = calculate_player_points(pdata)
            total_base += bp
            total_role += rp
            total_pts  += tp

        players.append({
            "player_id":      pid,
            "tournament_id":  base["tournament_id"],
            "ign":            base["ign"],
            "real_name":      "",
            "team":           base["team"],
            "team_abbr":      base["team_abbr"],
            "country":        "",
            "region":         base["region"],
            "role":           role,
            "agent":          agent,
            "rounds_played":  n,
            "matches_played": n,
            "has_incomplete_matches": has_incomplete,
            "rating":         round(rating, 3),
            "acs":            round(acs, 2),
            "kills":          kills,
            "deaths":         deaths,
            "assists":        assists,
            "kd_ratio":       kd_ratio,
            "kast":           round(kast, 2),
            "adr":            round(adr, 2),
            "headshot_pct":   round(headshot_pct, 2),
            "first_kills":    first_kills,
            "first_deaths":   first_deaths,
            "fk_fd_ratio":    fk_fd_ratio,
            "base_points":    round(total_base, 4),
            "role_points":    round(total_role, 4),
            "fantasy_points": round(total_pts, 2),
            "vlr_url":        base.get("vlr_url", ""),
        })

    return players


def _extract_match_score(soup) -> tuple:
    """
    Extract maps-won score + map count from a VLR match page.
    Returns (score_a, score_b, map_count, match_format, status).
    match_format is 'bo3' or 'bo5' based on number of maps played.
    """
    # Primary: header score badges
    score_els = soup.select(".match-header-vs-score .js-spoiler")
    if not score_els:
        score_els = soup.select(".match-header-vs-score span")
    
    # Try winner/loser specific containers first (most reliable)
    winner_el = soup.select_one(".match-header-vs-score-winner .js-spoiler, .match-header-vs-score-winner")
    loser_el  = soup.select_one(".match-header-vs-score-loser .js-spoiler, .match-header-vs-score-loser")
    
    score_a, score_b = 0, 0
    if winner_el and loser_el:
        w = _safe_int(winner_el.get_text(strip=True))
        l = _safe_int(loser_el.get_text(strip=True))
        # Determine which team (a or b) is the winner by checking order in the page
        # The left team (.match-header-link:first-child) corresponds to score position
        vs_block = soup.select_one(".match-header-vs")
        if vs_block:
            # Check if winner score comes before loser score in the DOM
            all_spans = vs_block.select(".js-spoiler")
            vals = []
            for s in all_spans:
                try:
                    vals.append(int(s.get_text(strip=True)))
                except ValueError:
                    pass
            if len(vals) >= 2:
                score_a, score_b = vals[0], vals[1]
            else:
                score_a, score_b = w, l
        else:
            score_a, score_b = w, l
    else:
        # Fallback: all .js-spoiler inside the score block
        scores = []
        for el in score_els:
            txt = el.get_text(strip=True)
            if txt in (":", "—", "–", "", "TBD"):
                continue
            try:
                scores.append(int(txt))
            except ValueError:
                pass
        score_a = scores[0] if len(scores) >= 1 else 0
        score_b = scores[1] if len(scores) >= 2 else 0

    # Count actual map tabs (each data-game-id that is a number = one map played)
    map_sections = soup.select(".vm-stats-game[data-game-id]")
    map_count = len([s for s in map_sections
                     if s.get("data-game-id","all").isdigit()])
    # Fallback: derive from score if no sections found
    if map_count == 0 and (score_a + score_b) > 0:
        map_count = score_a + score_b

    match_format = "bo5" if map_count > 3 else "bo3"
    
    # Detect upcoming
    has_scores = score_a > 0 or score_b > 0
    tbd_el = soup.select_one(".match-header-vs-note")
    if not has_scores and tbd_el and "tbd" in tbd_el.get_text(strip=True).lower():
        return 0, 0, 0, "", "upcoming"

    status = "completed" if has_scores else "upcoming"
    return score_a, score_b, map_count, match_format, status


def scrape_upcoming_matches(event_url: str, tournament_id: int) -> list:
    """
    Scrape VLR event matches page to find upcoming (not yet played) matches.
    Returns list of dicts with match metadata.
    """
    from database import upsert_match
    event_id, slug = _event_id_and_slug(event_url)
    if not event_id:
        return []

    candidates = []
    if slug:
        candidates.append(f"{BASE_URL}/event/matches/{event_id}/{slug}?series_id=all")
    candidates.append(f"{BASE_URL}/event/matches/{event_id}?series_id=all")

    upcoming = []
    for url in candidates:
        soup = _get(url)
        if not soup:
            continue

        # VLR upcoming matches have .match-item that don't have scores
        for a in soup.find_all("a", href=re.compile(r"^/\d+/[a-z0-9\-]+")):
            href = a["href"].split("?")[0].strip()
            if not re.match(r"^/\d+/[a-z0-9\-]+$", href):
                continue

            # Check if this looks like an upcoming match (no score visible)
            score_els = a.select(".match-item-event-series")
            time_el   = a.select_one(".match-item-time")
            eta_el    = a.select_one(".ml-eta")

            # Try to get team names from the link card
            team_els = a.select(".match-item-vs-team-name")
            team_a = team_els[0].get_text(strip=True) if len(team_els) > 0 else ""
            team_b = team_els[1].get_text(strip=True) if len(team_els) > 1 else ""

            # Score spans
            score_spans = a.select(".match-item-vs-team-score")
            s_a = score_spans[0].get_text(strip=True) if len(score_spans) > 0 else ""
            s_b = score_spans[1].get_text(strip=True) if len(score_spans) > 1 else ""

            is_upcoming = (s_a in ("", "-", "–") or s_b in ("", "-", "–"))

            if is_upcoming and team_a and team_b:
                scheduled = time_el.get_text(strip=True) if time_el else ""
                m = re.search(r"/(\d+)/", href)
                match_id = m.group(1) if m else href
                match_url = BASE_URL + href

                # Store as upcoming in DB
                try:
                    upsert_match({
                        "match_id":      match_id,
                        "tournament_id": tournament_id,
                        "source_id":     None,
                        "match_url":     match_url,
                        "team_a":        team_a,
                        "team_b":        team_b,
                    })
                    from database import upsert_match_score
                    upsert_match_score(match_id, tournament_id, 0, 0, "upcoming")
                    # Store schedule time if available
                    if scheduled:
                        import sqlite3, os
                        from database import DB_PATH, get_connection
                        conn = get_connection()
                        conn.execute(
                            "UPDATE matches SET scheduled_at=? WHERE match_id=? AND tournament_id=?",
                            (scheduled, match_id, tournament_id)
                        )
                        conn.commit()
                        conn.close()
                except Exception as e:
                    print(f"[scraper] Upcoming match store error: {e}")

                upcoming.append({
                    "match_id":    match_id,
                    "match_url":   match_url,
                    "team_a":      team_a,
                    "team_b":      team_b,
                    "scheduled":   scheduled,
                })

        if upcoming:
            break

    print(f"[scraper] Found {len(upcoming)} upcoming matches.")
    return upcoming



def _make_player_stub(player_id, tournament_id, ign, team_full, team_abbr, vlr_url, region):
    """Return a zero-stat player dict ready for upsert_player."""
    return {
        "player_id":      player_id,
        "tournament_id":  tournament_id,
        "ign":            ign,
        "real_name":      "",
        "team":           team_full,
        "team_abbr":      team_abbr,
        "country":        "",
        "region":         region,
        "role":           "flex",
        "agent":          "",
        "rounds_played":  0,
        "matches_played": 0,
        "rating":         0.0,
        "acs":            0.0,
        "kills":          0,
        "deaths":         0,
        "assists":        0,
        "kd_ratio":       0.0,
        "kast":           0.0,
        "adr":            0.0,
        "headshot_pct":   0.0,
        "first_kills":    0,
        "first_deaths":   0,
        "fk_fd_ratio":    0.0,
        "base_points":    0.0,
        "role_points":    0.0,
        "fantasy_points": 0.0,
        "vlr_url":        vlr_url,
        "manual_pts":     0.0,
    }


def _player_id_from_link(a_tag, ign, tournament_id):
    """Build a player_id + vlr_url from a /player/ anchor tag."""
    if not a_tag:
        return f"t{tournament_id}_{re.sub(r'[^a-z0-9]', '_', ign.lower())}", ""
    href    = a_tag.get("href", "")
    vlr_url = (BASE_URL + href) if href.startswith("/") else href
    if "/player/" in vlr_url:
        parts  = vlr_url.rstrip("/").split("/")
        raw_id = f"{parts[-2]}_{parts[-1]}" if len(parts) >= 2 else parts[-1]
    else:
        raw_id = re.sub(r"[^a-z0-9]", "_", ign.lower())
    return f"t{tournament_id}_{raw_id}", vlr_url


def scrape_event_rosters(event_url: str, tournament_id: int) -> dict:
    """
    Scrape player rosters for a tournament BEFORE matches are played.
    Three-tier approach (stops at first one that yields results):
      1. Event stats page  — same table format, works once any match is played
      2. All match pages   — extract every /player/ link even from upcoming pages
      3. Event teams page  — team roster cards
    Players are upserted with 0 stats so they appear on the leaderboard immediately.
    Returns {team_name: [player_dicts]}.
    """
    from database import upsert_player, get_connection

    event_id, slug = _event_id_and_slug(event_url)
    if not event_id:
        print("[roster] Could not parse event ID from URL")
        return {}

    seen_ids = set()   # deduplicate across approaches
    rosters  = {}      # team_full -> [player_dicts]

    def _register(player_dict):
        pid = player_dict["player_id"]
        if pid in seen_ids:
            return
        seen_ids.add(pid)
        team = player_dict["team"] or player_dict["team_abbr"] or "Unknown"
        rosters.setdefault(team, []).append(player_dict)
        # Only insert if not already in DB with real stats (fantasy_points > 0)
        try:
            conn = get_connection()
            existing = conn.execute(
                "SELECT fantasy_points FROM players WHERE player_id=? AND tournament_id=?",
                (pid, tournament_id)
            ).fetchone()
            conn.close()
            if existing is None:
                upsert_player(player_dict)
            # If existing has real stats, don't overwrite
        except Exception as e:
            print(f"[roster] upsert error for {player_dict['ign']}: {e}")

    # ── APPROACH 1: Event stats leaderboard ─────────────────────────────────
    # /event/stats/{id}/{slug} has the same tbody row structure as match pages
    stat_urls = []
    if slug:
        stat_urls.append(f"{BASE_URL}/event/stats/{event_id}/{slug}")
    stat_urls.append(f"{BASE_URL}/event/stats/{event_id}")

    for url in stat_urls:
        print(f"[roster] Trying stats page: {url}")
        soup = _get(url)
        if not soup:
            continue
        for row in soup.select("tbody tr"):
            ign_el    = row.select_one(".text-of")
            team_el   = row.select_one(".ge-text-light")
            a_tag     = row.find("a", href=re.compile(r"/player/"))
            ign       = ign_el.get_text(strip=True) if ign_el else ""
            team_abbr = team_el.get_text(strip=True) if team_el else ""
            if not ign or len(ign) < 2:
                continue
            pid, vlr_url = _player_id_from_link(a_tag, ign, tournament_id)
            _register(_make_player_stub(
                pid, tournament_id, ign,
                team_abbr, team_abbr, vlr_url,
                infer_region(team_abbr, "")
            ))
        if rosters:
            print(f"[roster] Stats page: {len(seen_ids)} players from {len(rosters)} teams")
            break

    if rosters:
        print(f"[roster] Done via stats page: {len(seen_ids)} players")
        return rosters

    # ── APPROACH 2: Scan all match URLs, extract every /player/ link ─────────
    # Works for both upcoming and completed matches — VLR always links player profiles
    print("[roster] Trying match-page player extraction…")
    match_urls = get_event_match_urls(event_url)

    for match_url in match_urls[:30]:   # cap at 30 to avoid hammering the server
        soup = _get(match_url)
        if not soup:
            continue

        # Get the two team names from the match header (authoritative)
        header_teams = []
        for el in soup.select(".match-header-link-name .wf-title-med"):
            t = el.get_text(strip=True)
            if t:
                header_teams.append(t)
        team_a = header_teams[0] if len(header_teams) > 0 else ""
        team_b = header_teams[1] if len(header_teams) > 1 else ""

        # Collect every /player/ link on the whole page
        all_player_links = soup.find_all("a", href=re.compile(r"/player/\d+/"))

        for a in all_player_links:
            href = a.get("href", "")
            # Build player_id from URL
            pid, vlr_url = _player_id_from_link(a, "", tournament_id)
            if pid in seen_ids:
                continue

            # IGN: look for .text-of inside the <a>, or take the link text itself
            ign_el = a.select_one(".text-of")
            ign = ign_el.get_text(strip=True) if ign_el else a.get_text(strip=True).strip()
            if not ign or len(ign) < 2:
                continue

            # Team: look for .ge-text-light in parent row, or use match header teams
            row_el    = a.find_parent("tr")
            team_el   = row_el.select_one(".ge-text-light") if row_el else None
            team_abbr = team_el.get_text(strip=True) if team_el else ""

            # Determine which team this player belongs to via tbody position
            team_full = ""
            tbody = a.find_parent("tbody")
            if tbody and team_a and team_b:
                # Count which tbody index this is in the stats section
                parent_section = soup.select_one(".vm-stats-game[data-game-id='all']")
                if parent_section:
                    tbodies = parent_section.select("tbody")
                    if len(tbodies) >= 2:
                        team_full = team_a if tbody == tbodies[0] else team_b

            if not team_full:
                team_full = team_abbr or team_a or "Unknown"

            _register(_make_player_stub(
                pid, tournament_id, ign,
                team_full, team_abbr or team_full[:5].upper(),
                vlr_url, infer_region(team_abbr or team_full, "")
            ))

        time.sleep(0.8)

    if rosters:
        print(f"[roster] Done via match pages: {len(seen_ids)} players from {len(rosters)} teams")
        return rosters

    # ── APPROACH 3: Event teams page ─────────────────────────────────────────
    print("[roster] Trying event teams page…")
    team_page_urls = []
    if slug:
        team_page_urls.append(f"{BASE_URL}/event/teams/{event_id}/{slug}")
    team_page_urls.append(f"{BASE_URL}/event/{event_id}")

    for url in team_page_urls:
        soup = _get(url)
        if not soup:
            continue

        # Find all /player/ links in the page — pair with nearest team heading
        current_team = "Unknown"
        for el in soup.find_all(["h2", "h3", "div", "a"]):
            # Update current team when we hit a team name element
            if el.name in ("h2", "h3"):
                t = el.get_text(strip=True)
                if t and len(t) > 1:
                    current_team = t
            elif el.name == "a" and re.search(r"/player/\d+/", el.get("href", "")):
                pid, vlr_url = _player_id_from_link(el, "", tournament_id)
                if pid in seen_ids:
                    continue
                ign = el.get_text(strip=True).strip()
                if not ign or len(ign) < 2:
                    continue
                _register(_make_player_stub(
                    pid, tournament_id, ign,
                    current_team, current_team[:5].upper(),
                    vlr_url, infer_region(current_team, "")
                ))

        if rosters:
            break

    print(f"[roster] Total: {len(seen_ids)} players from {len(rosters)} teams")
    return rosters




# ═══════════════════════════════════════════════════════════════════════════════
# ORCHESTRATION
# ═══════════════════════════════════════════════════════════════════════════════

def _extract_player_stubs_from_match(soup, result: dict, tournament_id: int, source_region: str) -> list:
    """
    Extract minimal player stubs from a match page even when no stats are available.
    Uses every /player/ link found on the page, assigns team from tbody position
    or from match header teams.
    Returns list of player dicts ready for upsert_player (zero stats).
    """
    if soup is None:
        return []

    team_a = result.get("team_a", "")
    team_b = result.get("team_b", "")
    match_id = result.get("match_id", "")
    stubs = []
    seen = set()

    # Try structured stats section first (might have player rows even pre-match)
    active_section = soup.select_one(".vm-stats-game[data-game-id='all']")
    tbodies = active_section.select("tbody") if active_section else []

    for i, tbody in enumerate(tbodies):
        assigned_team = team_a if i == 0 else team_b
        for a in tbody.find_all("a", href=re.compile(r"/player/\d+/")):
            pid, vlr_url = _player_id_from_link(a, "", tournament_id)
            if pid in seen:
                continue
            seen.add(pid)
            ign_el = a.select_one(".text-of") or a
            ign    = ign_el.get_text(strip=True)
            abbr_el = a.find_parent("tr").select_one(".ge-text-light") if a.find_parent("tr") else None
            abbr   = abbr_el.get_text(strip=True) if abbr_el else assigned_team[:5].upper()
            if not ign or len(ign) < 2:
                continue
            stubs.append(_make_player_stub(
                pid, tournament_id, ign, assigned_team, abbr, vlr_url,
                infer_region(abbr or assigned_team, source_region)
            ))

    # Fallback: all /player/ links on the page
    if not stubs:
        all_links = soup.find_all("a", href=re.compile(r"/player/\d+/"))
        # Team assignment by page position — first half = team_a
        mid = max(len(all_links) // 2, 1)
        for idx, a in enumerate(all_links):
            pid, vlr_url = _player_id_from_link(a, "", tournament_id)
            if pid in seen:
                continue
            seen.add(pid)
            ign = a.get_text(strip=True).strip()
            if not ign or len(ign) < 2:
                continue
            assigned = team_a if idx < mid else team_b
            stubs.append(_make_player_stub(
                pid, tournament_id, ign,
                assigned, assigned[:5].upper(), vlr_url,
                infer_region(assigned, source_region)
            ))

    return stubs


def scrape_source(source: dict) -> tuple:
    """
    Scrape a single event_source.
    1. Discover all match URLs from the event page.
    2. Scrape each match for per-player stats.
    3. Persist raw stats in match_player_stats table.
    4. Aggregate and upsert into players table.
    Returns (player_count, status_message).
    """
    from database import (
        upsert_player, upsert_match, upsert_match_player_stats,
        log_scrape, update_source_scraped,
    )

    tournament_id = source["tournament_id"]
    source_id     = source["id"]
    event_url     = source["vlr_url"]
    region        = source.get("region", "")

    try:
        match_urls = get_event_match_urls(event_url)
        if not match_urls:
            msg = "No match URLs found — check the event URL."
            log_scrape(tournament_id, source_id, 0, "warning", msg)
            return 0, msg

        all_player_rows = []
        matches_scraped = 0

        stub_count = 0  # players registered from match headers (upcoming matches)

        for i, match_url in enumerate(match_urls):
            result = scrape_match_page(match_url, tournament_id, region)
            if result is None:
                print(f"[scraper] Skipping (None): {match_url}")
                continue

            # Always store the match record — even for upcoming matches
            upsert_match({
                "match_id":     result["match_id"],
                "tournament_id": tournament_id,
                "source_id":    source_id,
                "match_url":    result["match_url"],
                "team_a":       result["team_a"],
                "team_b":       result["team_b"],
                "score_a":      result.get("score_a", 0),
                "score_b":      result.get("score_b", 0),
                "map_count":    result.get("map_count", 0),
                "match_format": result.get("match_format", ""),
                "status":       result.get("status", "completed"),
            })

            if result["players"]:
                # Completed match — store per-match stats and aggregate later
                for prow in result["players"]:
                    upsert_match_player_stats(prow)
                all_player_rows.extend(result["players"])
                matches_scraped += 1
            else:
                # Upcoming or stats-free match — register player stubs from page links
                stubs = _extract_player_stubs_from_match(
                    result.get("_soup"), result, tournament_id, region
                )
                for stub in stubs:
                    try:
                        from database import get_connection as _gc
                        _conn = _gc()
                        existing = _conn.execute(
                            "SELECT fantasy_points FROM players WHERE player_id=? AND tournament_id=?",
                            (stub["player_id"], tournament_id)
                        ).fetchone()
                        _conn.close()
                        if existing is None:
                            upsert_player(stub)
                            stub_count += 1
                    except Exception as _e:
                        print(f"[scraper] stub upsert error: {_e}")

            if i < len(match_urls) - 1:
                time.sleep(1.2)

        if not all_player_rows:
            msg = f"Processed {len(match_urls)} URLs, no completed match data. {stub_count} player stubs registered."
            log_scrape(tournament_id, source_id, stub_count, "warning", msg)
            return stub_count, msg

        aggregated = aggregate_player_stats(all_player_rows)
        for p in aggregated:
            upsert_player(p)

        update_source_scraped(source_id, len(aggregated))
        msg = f"Scraped {matches_scraped} matches → {len(aggregated)} players. {stub_count} stubs."
        log_scrape(tournament_id, source_id, len(aggregated), "success",
                   f"{matches_scraped} matches scraped.")
        return len(aggregated), msg

    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        log_scrape(tournament_id, source_id, 0, "error", str(e))
        print(f"[scraper] EXCEPTION:\n{tb}")
        return 0, f"Scrape failed: {e}"


def scrape_all_sources(tournament_id: int) -> tuple:
    from database import get_event_sources
    sources = get_event_sources(tournament_id)
    if not sources:
        return 0, ["No event sources configured for this tournament."]

    total    = 0
    messages = []
    for source in sources:
        count, msg = scrape_source(source)
        total += count
        messages.append(f"[{source.get('region', '?')}] {msg}")
        time.sleep(2)

    return total, messages
