"""
database.py — Full SQLite schema and helper functions
VCT Fantasy League Manager v2
"""
import sqlite3
import json
import os

DB_PATH = os.environ.get("DB_PATH", "/data/vct_fantasy.db")


def _run_migrations():
    """
    Comprehensive, safe schema migrations.
    Runs on every import — handles any version of the old DB.
    """
    if not os.path.exists(DB_PATH):
        return  # Fresh install — init_db() will build the full schema

    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()

        def cols(table):
            """Return set of existing column names for a table, or empty set if table missing."""
            try:
                return {row[1] for row in c.execute(f"PRAGMA table_info({table})").fetchall()}
            except Exception:
                return set()

        def add_col(table, col, col_def):
            if col not in cols(table):
                try:
                    c.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_def}")
                    print(f"[migration] Added {table}.{col}")
                except Exception as e:
                    print(f"[migration] Warning adding {table}.{col}: {e}")

        # ── tournaments ──────────────────────────────────────────────────────
        add_col("tournaments", "description", "TEXT")
        add_col("tournaments", "format",      "TEXT DEFAULT 'standard'")
        add_col("tournaments", "status",      "TEXT DEFAULT 'swiss'")

        # ── event_sources ────────────────────────────────────────────────────
        add_col("event_sources", "event_name",    "TEXT")
        add_col("event_sources", "region",        "TEXT")
        add_col("event_sources", "last_scraped",  "TEXT")
        add_col("event_sources", "players_found", "INTEGER DEFAULT 0")

        # ── leagues ──────────────────────────────────────────────────────────
        add_col("leagues", "tournament_id", "INTEGER NOT NULL DEFAULT 1")
        add_col("leagues", "description",   "TEXT DEFAULT ''")
        add_col("leagues", "phase",         "TEXT DEFAULT 'swiss'")
        add_col("leagues", "ruleset",       "TEXT DEFAULT '{}'")

        # ── players ──────────────────────────────────────────────────────────
        add_col("players", "real_name",     "TEXT DEFAULT ''")
        add_col("players", "team_abbr",     "TEXT DEFAULT ''")
        add_col("players", "country",       "TEXT DEFAULT ''")
        add_col("players", "region",        "TEXT DEFAULT ''")
        add_col("players", "role",          "TEXT DEFAULT 'flex'")
        add_col("players", "agent",         "TEXT DEFAULT ''")
        add_col("players", "rounds_played", "INTEGER DEFAULT 0")
        add_col("players", "matches_played","INTEGER DEFAULT 0")
        add_col("players", "kd_ratio",      "REAL DEFAULT 0.0")
        add_col("players", "headshot_pct",  "REAL DEFAULT 0.0")
        add_col("players", "first_kills",   "INTEGER DEFAULT 0")
        add_col("players", "first_deaths",  "INTEGER DEFAULT 0")
        add_col("players", "fk_fd_ratio",   "REAL DEFAULT 0.0")
        add_col("players", "base_points",   "REAL DEFAULT 0.0")
        add_col("players", "role_points",   "REAL DEFAULT 0.0")
        add_col("players", "fantasy_points","REAL DEFAULT 0.0")
        add_col("players", "last_updated",  "TEXT")
        add_col("players", "vlr_url",       "TEXT DEFAULT ''")

        # ── fantasy_teams ────────────────────────────────────────────────────
        add_col("fantasy_teams", "bonus_points", "REAL DEFAULT 0.0")

        # ── fantasy_roster ───────────────────────────────────────────────────
        add_col("fantasy_roster", "tournament_id", "INTEGER NOT NULL DEFAULT 1")
        add_col("fantasy_roster", "is_star",       "INTEGER DEFAULT 0")
        add_col("fantasy_roster", "is_duplicate",  "INTEGER DEFAULT 0")
        add_col("fantasy_roster", "phase",         "TEXT DEFAULT 'swiss'")

        # ── trades ───────────────────────────────────────────────────────────
        add_col("trades", "resolved_at", "TEXT")

        # ── match_player_stats new columns ──────────────────────────────────────
        mps_cols = {r[1] for r in c.execute("PRAGMA table_info(match_player_stats)").fetchall()}
        for col, defn in [("stats_incomplete","INTEGER DEFAULT 0"),("missing_fields","TEXT DEFAULT ''"),("match_team","TEXT DEFAULT ''")]:
            if col not in mps_cols:
                c.execute(f"ALTER TABLE match_player_stats ADD COLUMN {col} {defn}")

        # ── matches new columns ──────────────────────────────────────────────────
        if "matches" in {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}:
            mc = {r[1] for r in c.execute("PRAGMA table_info(matches)").fetchall()}
            for col, defn in [("score_a","INTEGER DEFAULT 0"),("score_b","INTEGER DEFAULT 0"),
                               ("map_count","INTEGER DEFAULT 0"),("match_format","TEXT DEFAULT ''"),
                               ("status","TEXT DEFAULT 'completed'"),("scheduled_at","TEXT DEFAULT ''")]:
                if col not in mc:
                    c.execute(f"ALTER TABLE matches ADD COLUMN {col} {defn}")

        # ── Re-fetch existing tables after additions ────────────────────────────
        existing_tables = {r[0] for r in
            c.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}

        # ── player_adjustments ──────────────────────────────────────────────────
        if "player_adjustments" not in existing_tables:
            c.execute("""CREATE TABLE IF NOT EXISTS player_adjustments (
                id INTEGER PRIMARY KEY AUTOINCREMENT, player_id TEXT NOT NULL,
                tournament_id INTEGER NOT NULL, delta REAL NOT NULL,
                reason TEXT DEFAULT '', created_at TEXT DEFAULT (datetime('now'))
            )""")
            print("[migration] Created table: player_adjustments")

        # ── players.manual_pts ───────────────────────────────────────────────
        p_cols = {r[1] for r in c.execute("PRAGMA table_info(players)").fetchall()}
        if "manual_pts" not in p_cols:
            c.execute("ALTER TABLE players ADD COLUMN manual_pts REAL DEFAULT 0.0")

        # ── matches extra cols ───────────────────────────────────────────────
        if "matches" in existing_tables:
            m_cols = {r[1] for r in c.execute("PRAGMA table_info(matches)").fetchall()}
            for col, defn in [("score_a","INTEGER DEFAULT 0"),("score_b","INTEGER DEFAULT 0"),
                               ("status","TEXT DEFAULT 'completed'"),("scheduled_at","TEXT DEFAULT ''")]:
                if col not in m_cols:
                    c.execute(f"ALTER TABLE matches ADD COLUMN {col} {defn}")

        # ── fantasy_teams snapshot ───────────────────────────────────────────
        ft_cols = {r[1] for r in c.execute("PRAGMA table_info(fantasy_teams)").fetchall()}
        if "swiss_pts_snapshot" not in ft_cols:
            c.execute("ALTER TABLE fantasy_teams ADD COLUMN swiss_pts_snapshot REAL DEFAULT 0.0")

        # ── Create new tables if missing ─────────────────────────────────────
        existing_tables = {r[0] for r in
            c.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}

        if "matches" not in existing_tables:
            c.execute("""CREATE TABLE IF NOT EXISTS matches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                match_id TEXT NOT NULL, tournament_id INTEGER NOT NULL,
                source_id INTEGER, match_url TEXT NOT NULL,
                team_a TEXT DEFAULT '', team_b TEXT DEFAULT '',
                scraped_at TEXT DEFAULT (datetime('now')),
                UNIQUE(match_id, tournament_id)
            )""")
            print("[migration] Created table: matches")

        if "match_player_stats" not in existing_tables:
            c.execute("""CREATE TABLE IF NOT EXISTS match_player_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                player_id TEXT NOT NULL, tournament_id INTEGER NOT NULL,
                match_id TEXT NOT NULL, match_url TEXT DEFAULT '',
                ign TEXT NOT NULL, team TEXT DEFAULT '',
                team_abbr TEXT DEFAULT '', region TEXT DEFAULT '',
                role TEXT DEFAULT 'flex', agent TEXT DEFAULT '',
                rating REAL DEFAULT 0.0, acs REAL DEFAULT 0.0,
                kills INTEGER DEFAULT 0, deaths INTEGER DEFAULT 0,
                assists INTEGER DEFAULT 0, kd_diff INTEGER DEFAULT 0,
                kast REAL DEFAULT 0.0, adr REAL DEFAULT 0.0,
                headshot_pct REAL DEFAULT 0.0, first_kills INTEGER DEFAULT 0,
                first_deaths INTEGER DEFAULT 0, fk_diff INTEGER DEFAULT 0,
                vlr_url TEXT DEFAULT '',
                scraped_at TEXT DEFAULT (datetime('now')),
                UNIQUE(player_id, match_id)
            )""")
            print("[migration] Created table: match_player_stats")

        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[db migration] Error: {e}")


_run_migrations()


def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db():
    conn = get_connection()
    c = conn.cursor()

    # ── Tournaments ─────────────────────────────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS tournaments (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            name        TEXT NOT NULL,
            description TEXT,
            format      TEXT DEFAULT 'standard',
            status      TEXT DEFAULT 'swiss',
            created_at  TEXT DEFAULT (datetime('now'))
        )
    """)

    # ── Event sources (one or many VLR URLs per tournament) ─────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS event_sources (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            tournament_id INTEGER NOT NULL,
            vlr_url       TEXT NOT NULL,
            event_name    TEXT,
            region        TEXT,
            last_scraped  TEXT,
            players_found INTEGER DEFAULT 0,
            FOREIGN KEY (tournament_id) REFERENCES tournaments(id)
        )
    """)

    # ── Players (aggregated leaderboard — rebuilt from match_player_stats) ─────
    c.execute("""
        CREATE TABLE IF NOT EXISTS players (
            player_id      TEXT NOT NULL,
            tournament_id  INTEGER NOT NULL,
            ign            TEXT NOT NULL,
            real_name      TEXT DEFAULT '',
            team           TEXT DEFAULT '',
            team_abbr      TEXT DEFAULT '',
            country        TEXT DEFAULT '',
            region         TEXT DEFAULT '',
            role           TEXT DEFAULT 'flex',
            agent          TEXT DEFAULT '',
            rounds_played  INTEGER DEFAULT 0,
            matches_played INTEGER DEFAULT 0,
            rating         REAL DEFAULT 0.0,
            acs            REAL DEFAULT 0.0,
            kills          INTEGER DEFAULT 0,
            deaths         INTEGER DEFAULT 0,
            assists        INTEGER DEFAULT 0,
            kd_ratio       REAL DEFAULT 0.0,
            kast           REAL DEFAULT 0.0,
            adr            REAL DEFAULT 0.0,
            headshot_pct   REAL DEFAULT 0.0,
            first_kills    INTEGER DEFAULT 0,
            first_deaths   INTEGER DEFAULT 0,
            fk_fd_ratio    REAL DEFAULT 0.0,
            base_points    REAL DEFAULT 0.0,
            role_points    REAL DEFAULT 0.0,
            fantasy_points REAL DEFAULT 0.0,
            last_updated   TEXT,
            vlr_url        TEXT DEFAULT '',
            PRIMARY KEY (player_id, tournament_id),
            FOREIGN KEY (tournament_id) REFERENCES tournaments(id)
        )
    """)

    # ── Fantasy leagues ───────────────────────────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS leagues (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            tournament_id INTEGER NOT NULL,
            name          TEXT NOT NULL,
            description   TEXT DEFAULT '',
            phase         TEXT DEFAULT 'swiss',
            ruleset       TEXT DEFAULT '{}',
            created_at    TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (tournament_id) REFERENCES tournaments(id)
        )
    """)

    # ── Fantasy teams ─────────────────────────────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS fantasy_teams (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            league_id     INTEGER NOT NULL,
            team_name     TEXT NOT NULL,
            manager_name  TEXT NOT NULL,
            bonus_points  REAL DEFAULT 0.0,
            created_at    TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (league_id) REFERENCES leagues(id)
        )
    """)

    # ── Fantasy roster ────────────────────────────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS fantasy_roster (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            fantasy_team_id  INTEGER NOT NULL,
            player_id        TEXT NOT NULL,
            tournament_id    INTEGER NOT NULL,
            role_slot        TEXT NOT NULL DEFAULT 'flex',
            is_star          INTEGER DEFAULT 0,
            is_duplicate     INTEGER DEFAULT 0,
            phase            TEXT DEFAULT 'swiss',
            added_at         TEXT DEFAULT (datetime('now')),
            UNIQUE(fantasy_team_id, player_id, phase),
            FOREIGN KEY (fantasy_team_id) REFERENCES fantasy_teams(id)
        )
    """)

    # ── Followed VCT teams ────────────────────────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS followed_teams (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            fantasy_team_id  INTEGER NOT NULL UNIQUE,
            team_name        TEXT NOT NULL,
            team_region      TEXT DEFAULT '',
            FOREIGN KEY (fantasy_team_id) REFERENCES fantasy_teams(id)
        )
    """)

    # ── Match results (for team win/loss points) ──────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS match_results (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            tournament_id INTEGER NOT NULL,
            team_name     TEXT NOT NULL,
            opponent      TEXT DEFAULT '',
            result        TEXT NOT NULL,
            format        TEXT DEFAULT 'bo3',
            created_at    TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (tournament_id) REFERENCES tournaments(id)
        )
    """)

    # ── Manual point adjustments ──────────────────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS point_adjustments (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            fantasy_team_id  INTEGER NOT NULL,
            amount           REAL NOT NULL,
            reason           TEXT DEFAULT '',
            created_at       TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (fantasy_team_id) REFERENCES fantasy_teams(id)
        )
    """)

    # ── Trades ────────────────────────────────────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            league_id       INTEGER NOT NULL,
            from_team_id    INTEGER NOT NULL,
            to_team_id      INTEGER NOT NULL,
            from_player_id  TEXT NOT NULL,
            to_player_id    TEXT NOT NULL,
            tournament_id   INTEGER NOT NULL,
            status          TEXT DEFAULT 'pending',
            proposed_at     TEXT DEFAULT (datetime('now')),
            resolved_at     TEXT,
            FOREIGN KEY (league_id) REFERENCES leagues(id)
        )
    """)

    # ── Draft sessions ────────────────────────────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS draft_sessions (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            league_id    INTEGER NOT NULL,
            phase        TEXT DEFAULT 'swiss',
            status       TEXT DEFAULT 'pending',
            current_pick INTEGER DEFAULT 1,
            total_picks  INTEGER DEFAULT 0,
            snake_order  TEXT DEFAULT '[]',
            picks_log    TEXT DEFAULT '[]',
            created_at   TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (league_id) REFERENCES leagues(id)
        )
    """)

    # ── Per-player point adjustments ────────────────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS player_adjustments (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            player_id     TEXT NOT NULL,
            tournament_id INTEGER NOT NULL,
            delta         REAL NOT NULL,
            reason        TEXT DEFAULT '',
            created_at    TEXT DEFAULT (datetime('now'))
        )
    """)

    # ── Matches (one row per VLR match page scraped) ──────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS matches (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            match_id      TEXT NOT NULL,
            tournament_id INTEGER NOT NULL,
            source_id     INTEGER,
            match_url     TEXT NOT NULL,
            team_a        TEXT DEFAULT '',
            team_b        TEXT DEFAULT '',
            score_a       INTEGER DEFAULT 0,
            score_b       INTEGER DEFAULT 0,
            map_count     INTEGER DEFAULT 0,
            match_format  TEXT DEFAULT '',
            status        TEXT DEFAULT 'completed',
            scheduled_at  TEXT DEFAULT '',
            scraped_at    TEXT DEFAULT (datetime('now')),
            UNIQUE(match_id, tournament_id),
            FOREIGN KEY (tournament_id) REFERENCES tournaments(id)
        )
    """)

    # ── Per-match player stats (raw, one row per player per match) ────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS match_player_stats (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            player_id     TEXT NOT NULL,
            tournament_id INTEGER NOT NULL,
            match_id      TEXT NOT NULL,
            match_url     TEXT DEFAULT '',
            ign           TEXT NOT NULL,
            team          TEXT DEFAULT '',
            team_abbr     TEXT DEFAULT '',
            region        TEXT DEFAULT '',
            role          TEXT DEFAULT 'flex',
            agent         TEXT DEFAULT '',
            rating        REAL DEFAULT 0.0,
            acs           REAL DEFAULT 0.0,
            kills         INTEGER DEFAULT 0,
            deaths        INTEGER DEFAULT 0,
            assists       INTEGER DEFAULT 0,
            kd_diff       INTEGER DEFAULT 0,
            kast          REAL DEFAULT 0.0,
            adr           REAL DEFAULT 0.0,
            headshot_pct  REAL DEFAULT 0.0,
            first_kills   INTEGER DEFAULT 0,
            first_deaths  INTEGER DEFAULT 0,
            fk_diff       INTEGER DEFAULT 0,
            match_team    TEXT DEFAULT '',
            vlr_url       TEXT DEFAULT '',
            stats_incomplete INTEGER DEFAULT 0,
            missing_fields   TEXT DEFAULT '',
            scraped_at    TEXT DEFAULT (datetime('now')),
            UNIQUE(player_id, match_id),
            FOREIGN KEY (tournament_id) REFERENCES tournaments(id)
        )
    """)

    # ── Scrape log ────────────────────────────────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS scrape_log (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            tournament_id INTEGER,
            source_id     INTEGER,
            scraped_at    TEXT DEFAULT (datetime('now')),
            players_found INTEGER DEFAULT 0,
            status        TEXT,
            notes         TEXT
        )
    """)

    # ── Migrations: add columns to existing DBs that predate new schema ─────
    existing_cols = {row[1] for row in c.execute('PRAGMA table_info(players)').fetchall()}
    if 'matches_played' not in existing_cols:
        c.execute('ALTER TABLE players ADD COLUMN matches_played INTEGER DEFAULT 0')

    conn.commit()
    conn.close()


# ═══════════════════════════════════════════════════════════════════════════════
# DEFAULT RULESET
# ═══════════════════════════════════════════════════════════════════════════════

DEFAULT_RULESET = {
    "total_players": 10,
    "role_requirements": {
        "duelist": 2,
        "initiator": 2,
        "controller": 2,
        "sentinel": 2,
        "flex": 2
    },
    "region_requirements": {
        "EMEA": 2,
        "AMER": 2,
        "APAC": 2,
        "CN": 2
    },
    "max_per_team": 1,
    "individual_locked": False,
    "swiss_duplicate_allowed": True,
    "swiss_unique_required": 4,
    "star_player_enabled": True,
    "team_following_enabled": True,
    "snake_draft": True,
    "single_phase": False
}


def get_ruleset(league_id):
    conn = get_connection()
    row = conn.execute("SELECT ruleset FROM leagues WHERE id = ?", (league_id,)).fetchone()
    conn.close()
    if not row:
        return dict(DEFAULT_RULESET)
    try:
        rs = json.loads(row["ruleset"])
        # Fill in any missing keys with defaults
        for k, v in DEFAULT_RULESET.items():
            if k not in rs:
                rs[k] = v
        return rs
    except Exception:
        return dict(DEFAULT_RULESET)


def save_ruleset(league_id, ruleset: dict):
    conn = get_connection()
    conn.execute("UPDATE leagues SET ruleset = ? WHERE id = ?",
                 (json.dumps(ruleset), league_id))
    conn.commit()
    conn.close()


# ═══════════════════════════════════════════════════════════════════════════════
# TOURNAMENTS
# ═══════════════════════════════════════════════════════════════════════════════

def create_tournament(name, description="", format="standard"):
    conn = get_connection()
    c = conn.cursor()
    c.execute("INSERT INTO tournaments (name, description, format) VALUES (?,?,?)",
              (name, description, format))
    tid = c.lastrowid
    conn.commit()
    conn.close()
    return tid


def get_all_tournaments():
    conn = get_connection()
    rows = conn.execute("SELECT * FROM tournaments ORDER BY id DESC").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_tournament(tournament_id):
    conn = get_connection()
    row = conn.execute("SELECT * FROM tournaments WHERE id = ?", (tournament_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def update_tournament_status(tournament_id, status):
    conn = get_connection()
    conn.execute("UPDATE tournaments SET status = ? WHERE id = ?", (status, tournament_id))
    conn.commit()
    conn.close()


def delete_tournament(tournament_id):
    conn = get_connection()
    # Cascade: delete all related data
    sources = conn.execute("SELECT id FROM event_sources WHERE tournament_id = ?",
                           (tournament_id,)).fetchall()
    for s in sources:
        conn.execute("DELETE FROM event_sources WHERE id = ?", (s[0],))
    conn.execute("DELETE FROM players WHERE tournament_id = ?", (tournament_id,))

    leagues = conn.execute("SELECT id FROM leagues WHERE tournament_id = ?",
                           (tournament_id,)).fetchall()
    for lg in leagues:
        _delete_league_cascade(conn, lg[0])

    conn.execute("DELETE FROM match_results WHERE tournament_id = ?", (tournament_id,))
    conn.execute("DELETE FROM tournaments WHERE id = ?", (tournament_id,))
    conn.commit()
    conn.close()


# ═══════════════════════════════════════════════════════════════════════════════
# EVENT SOURCES
# ═══════════════════════════════════════════════════════════════════════════════

def add_event_source(tournament_id, vlr_url, event_name="", region=""):
    conn = get_connection()
    c = conn.cursor()
    c.execute("""INSERT INTO event_sources (tournament_id, vlr_url, event_name, region)
                 VALUES (?,?,?,?)""", (tournament_id, vlr_url, event_name, region))
    sid = c.lastrowid
    conn.commit()
    conn.close()
    return sid


def get_event_sources(tournament_id):
    conn = get_connection()
    rows = conn.execute("SELECT * FROM event_sources WHERE tournament_id = ? ORDER BY id",
                        (tournament_id,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_event_source(source_id):
    conn = get_connection()
    row = conn.execute("SELECT * FROM event_sources WHERE id = ?", (source_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def update_source_scraped(source_id, players_found):
    conn = get_connection()
    conn.execute("""UPDATE event_sources SET last_scraped = datetime('now'), players_found = ?
                    WHERE id = ?""", (players_found, source_id))
    conn.commit()
    conn.close()


def delete_event_source(source_id):
    conn = get_connection()
    conn.execute("DELETE FROM event_sources WHERE id = ?", (source_id,))
    conn.commit()
    conn.close()


# ═══════════════════════════════════════════════════════════════════════════════
# PLAYERS
# ═══════════════════════════════════════════════════════════════════════════════

def upsert_match(data: dict):
    """Store a match record (one per scraped VLR match page)."""
    conn = get_connection()
    conn.execute("""
        INSERT INTO matches (match_id, tournament_id, source_id, match_url, team_a, team_b)
        VALUES (:match_id, :tournament_id, :source_id, :match_url, :team_a, :team_b)
        ON CONFLICT(match_id, tournament_id) DO UPDATE SET
            match_url=excluded.match_url,
            team_a=excluded.team_a,
            team_b=excluded.team_b,
            scraped_at=datetime('now')
    """, data)
    conn.commit()
    conn.close()


def upsert_match_player_stats(data: dict):
    """Store raw per-match player stats."""
    conn = get_connection()
    conn.execute("""
        INSERT INTO match_player_stats (
            player_id, tournament_id, match_id, match_url,
            ign, team, team_abbr, match_team, region, role, agent,
            rating, acs, kills, deaths, assists, kd_diff,
            kast, adr, headshot_pct, first_kills, first_deaths, fk_diff, vlr_url,
            stats_incomplete, missing_fields
        ) VALUES (
            :player_id, :tournament_id, :match_id, :match_url,
            :ign, :team, :team_abbr, :match_team, :region, :role, :agent,
            :rating, :acs, :kills, :deaths, :assists, :kd_diff,
            :kast, :adr, :headshot_pct, :first_kills, :first_deaths, :fk_diff, :vlr_url,
            :stats_incomplete, :missing_fields
        )
        ON CONFLICT(player_id, match_id) DO UPDATE SET
            rating=excluded.rating, acs=excluded.acs,
            kills=excluded.kills, deaths=excluded.deaths, assists=excluded.assists,
            kd_diff=excluded.kd_diff, kast=excluded.kast, adr=excluded.adr,
            headshot_pct=excluded.headshot_pct,
            first_kills=excluded.first_kills, first_deaths=excluded.first_deaths,
            match_team=excluded.match_team,
            stats_incomplete=excluded.stats_incomplete, missing_fields=excluded.missing_fields,
            fk_diff=excluded.fk_diff, scraped_at=datetime('now')
    """, data)
    conn.commit()
    conn.close()


def get_match_player_stats(tournament_id: int, player_id: str = None, match_id: str = None) -> list:
    """
    Query raw per-match stats.
    Filter by player_id, match_id, or both — or return all for a tournament.
    """
    conn = get_connection()
    query = "SELECT * FROM match_player_stats WHERE tournament_id = ?"
    params = [tournament_id]
    if player_id:
        query += " AND player_id = ?"
        params.append(player_id)
    if match_id:
        query += " AND match_id = ?"
        params.append(match_id)
    query += " ORDER BY match_id, ign"
    rows = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_matches(tournament_id: int) -> list:
    """Return all scraped matches for a tournament."""
    conn = get_connection()
    rows = conn.execute(
        "SELECT * FROM matches WHERE tournament_id = ? ORDER BY id DESC",
        (tournament_id,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def delete_match_data(tournament_id: int):
    """Clear all match and match_player_stats data for a tournament (for re-scraping)."""
    conn = get_connection()
    conn.execute("DELETE FROM match_player_stats WHERE tournament_id = ?", (tournament_id,))
    conn.execute("DELETE FROM matches WHERE tournament_id = ?", (tournament_id,))
    conn.execute("DELETE FROM players WHERE tournament_id = ?", (tournament_id,))
    conn.commit()
    conn.close()




def adjust_player_points(player_id: str, tournament_id: int, delta: float, reason: str = ""):
    """Add a manual point adjustment directly to a player's record."""
    conn = get_connection()
    conn.execute(
        "UPDATE players SET manual_pts = COALESCE(manual_pts,0) + ? WHERE player_id=? AND tournament_id=?",
        (delta, player_id, tournament_id)
    )
    # Store in a log table
    try:
        conn.execute(
            "INSERT INTO player_adjustments (player_id, tournament_id, delta, reason) VALUES (?,?,?,?)",
            (player_id, tournament_id, delta, reason)
        )
    except Exception:
        pass  # table may not exist yet
    conn.commit()
    conn.close()


def get_player_adjustments(player_id: str, tournament_id: int) -> list:
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT * FROM player_adjustments WHERE player_id=? AND tournament_id=? ORDER BY id DESC",
            (player_id, tournament_id)
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception:
        conn.close()
        return []


def delete_player_adjustment(adj_id: int, player_id: str, tournament_id: int, delta: float):
    """Remove a player adjustment and reverse the delta."""
    conn = get_connection()
    try:
        conn.execute("DELETE FROM player_adjustments WHERE id=?", (adj_id,))
        conn.execute(
            "UPDATE players SET manual_pts = COALESCE(manual_pts,0) - ? WHERE player_id=? AND tournament_id=?",
            (delta, player_id, tournament_id)
        )
        conn.commit()
    except Exception:
        pass
    conn.close()


def get_phase_standings(league_id: int, phase: str) -> list:
    """
    Standings for a specific phase (swiss or playoffs).
    Uses the roster for that phase and sums player fantasy_points + manual_pts.
    """
    league = get_league(league_id)
    if not league:
        return []
    teams = get_teams_in_league(league_id)
    standings = []
    for t in teams:
        roster = get_roster(t["id"], phase=phase)
        player_pts = 0.0
        star_bonus = 0.0
        for p in roster:
            base_pts = p["fantasy_points"] + p.get("manual_pts", 0.0)
            if p["is_star"]:
                star_bonus += base_pts * 0.5
            player_pts += base_pts
        follow_pts = calculate_team_follow_points(t["id"], league["tournament_id"])
        adj_pts    = get_total_adjustments(t["id"])
        total = round(player_pts + star_bonus + follow_pts + adj_pts, 2)
        followed = get_followed_team(t["id"])
        standings.append({
            **t,
            "player_pts":   round(player_pts, 2),
            "star_bonus":   round(star_bonus, 2),
            "follow_pts":   round(follow_pts, 2),
            "adj_pts":      round(adj_pts, 2),
            "total_points": total,
            "player_count": len(roster),
            "phase":        phase,
            "followed_team": followed["team_name"] if followed else None,
        })
    standings.sort(key=lambda x: x["total_points"], reverse=True)
    return standings


def get_overall_standings(league_id: int) -> list:
    """
    Overall = swiss phase + playoffs phase points combined per team.
    """
    swiss    = {s["id"]: s for s in get_phase_standings(league_id, "swiss")}
    playoffs = {s["id"]: s for s in get_phase_standings(league_id, "playoffs")}
    teams    = get_teams_in_league(league_id)

    standings = []
    for t in teams:
        s_pts = swiss.get(t["id"], {}).get("total_points", 0.0)
        p_pts = playoffs.get(t["id"], {}).get("total_points", 0.0)
        total = round(s_pts + p_pts, 2)
        standings.append({
            **t,
            "swiss_pts":    s_pts,
            "playoffs_pts": p_pts,
            "total_points": total,
            "player_count": swiss.get(t["id"], {}).get("player_count", 0),
            "followed_team": swiss.get(t["id"], {}).get("followed_team"),
        })
    standings.sort(key=lambda x: x["total_points"], reverse=True)
    return standings


def upsert_match_score(match_id: str, tournament_id: int, score_a: int, score_b: int, status: str = "completed"):
    conn = get_connection()
    conn.execute(
        "UPDATE matches SET score_a=?, score_b=?, status=? WHERE match_id=? AND tournament_id=?",
        (score_a, score_b, status, match_id, tournament_id)
    )
    conn.commit()
    conn.close()


def get_upcoming_matches(tournament_id: int) -> list:
    conn = get_connection()
    rows = conn.execute(
        "SELECT * FROM matches WHERE tournament_id=? AND status='upcoming' ORDER BY scheduled_at",
        (tournament_id,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def upsert_player(data: dict):
    """Insert or update an aggregated player row in the leaderboard."""
    conn = get_connection()
    # Provide defaults for any missing keys
    d = {"matches_played": 0, "rounds_played": 0, **data}
    conn.execute("""
        INSERT INTO players (
            player_id, tournament_id, ign, real_name, team, team_abbr, country,
            region, role, agent, rounds_played, matches_played,
            rating, acs, kills, deaths, assists, kd_ratio, kast, adr,
            headshot_pct, first_kills, first_deaths, fk_fd_ratio,
            base_points, role_points, fantasy_points, last_updated, vlr_url
        ) VALUES (
            :player_id, :tournament_id, :ign, :real_name, :team, :team_abbr, :country,
            :region, :role, :agent, :rounds_played, :matches_played,
            :rating, :acs, :kills, :deaths, :assists, :kd_ratio, :kast, :adr,
            :headshot_pct, :first_kills, :first_deaths, :fk_fd_ratio,
            :base_points, :role_points, :fantasy_points, datetime('now'), :vlr_url
        )
        ON CONFLICT(player_id, tournament_id) DO UPDATE SET
            ign=excluded.ign, real_name=excluded.real_name,
            team=excluded.team, team_abbr=excluded.team_abbr,
            country=excluded.country, region=excluded.region, agent=excluded.agent,
            rounds_played=excluded.rounds_played,
            matches_played=excluded.matches_played,
            rating=excluded.rating, acs=excluded.acs,
            kills=excluded.kills, deaths=excluded.deaths,
            assists=excluded.assists, kd_ratio=excluded.kd_ratio,
            kast=excluded.kast, adr=excluded.adr,
            headshot_pct=excluded.headshot_pct,
            first_kills=excluded.first_kills, first_deaths=excluded.first_deaths,
            fk_fd_ratio=excluded.fk_fd_ratio,
            base_points=excluded.base_points, role_points=excluded.role_points,
            fantasy_points=excluded.fantasy_points,
            last_updated=datetime('now'), vlr_url=excluded.vlr_url
    """, d)
    conn.commit()
    conn.close()



def get_all_players(sort_by="fantasy_points", order="desc", search="", role_filter=""):
    """
    Return players across ALL tournaments combined.
    Used by the dashboard index and any cross-tournament views.
    Mirrors get_players() but without a tournament_id filter.
    """
    safe = {"rating","acs","kills","deaths","kd_ratio","kast","adr","headshot_pct",
            "first_kills","first_deaths","rounds_played","matches_played","assists","ign",
            "team","fantasy_points","base_points","role_points","role","region","team_abbr"}
    if sort_by not in safe:
        sort_by = "fantasy_points"
    direction = "DESC" if order == "desc" else "ASC"
    conn = get_connection()
    query = "SELECT * FROM players WHERE 1=1"
    params = []
    if search:
        query += " AND (LOWER(ign) LIKE ? OR LOWER(team) LIKE ? OR LOWER(team_abbr) LIKE ?)"
        s = f"%{search.lower()}%"
        params += [s, s, s]
    if role_filter:
        query += " AND role = ?"
        params.append(role_filter)
    query += f" ORDER BY {sort_by} {direction}"
    rows = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_players(tournament_id, sort_by="fantasy_points", order="desc", search="", role_filter=""):
    safe = {"rating","acs","kills","deaths","kd_ratio","kast","adr","headshot_pct",
            "first_kills","first_deaths","rounds_played","matches_played","assists","ign","team",
            "fantasy_points","base_points","role_points","role","region","team_abbr"}
    if sort_by not in safe:
        sort_by = "fantasy_points"
    direction = "DESC" if order == "desc" else "ASC"
    conn = get_connection()
    query = f"SELECT * FROM players WHERE tournament_id = ?"
    params = [tournament_id]
    if search:
        query += " AND (LOWER(ign) LIKE ? OR LOWER(team) LIKE ? OR LOWER(team_abbr) LIKE ?)"
        s = f"%{search.lower()}%"
        params += [s, s, s]
    if role_filter:
        query += " AND role = ?"
        params.append(role_filter)
    query += f" ORDER BY {sort_by} {direction}"
    rows = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_player(player_id, tournament_id):
    conn = get_connection()
    row = conn.execute("SELECT * FROM players WHERE player_id = ? AND tournament_id = ?",
                       (player_id, tournament_id)).fetchone()
    conn.close()
    return dict(row) if row else None


def update_player_role(player_id, tournament_id, role):
    conn = get_connection()
    conn.execute("UPDATE players SET role = ? WHERE player_id = ? AND tournament_id = ?",
                 (role, player_id, tournament_id))
    conn.commit()
    conn.close()


def update_player_region(player_id, tournament_id, region):
    conn = get_connection()
    conn.execute("UPDATE players SET region = ? WHERE player_id = ? AND tournament_id = ?",
                 (region, player_id, tournament_id))
    conn.commit()
    conn.close()


def recalculate_tournament_points(tournament_id):
    """
    Recalculate fantasy points for all players by re-summing per-match points.
    This is the correct approach: points are calculated per match then summed,
    matching the Power Query behaviour.
    """
    from points import calculate_player_points
    from collections import defaultdict

    # Fetch all raw match rows for this tournament
    conn = get_connection()
    raw_rows = conn.execute(
        "SELECT * FROM match_player_stats WHERE tournament_id = ?",
        (tournament_id,)
    ).fetchall()
    conn.close()

    if not raw_rows:
        # Fallback: no match-level data, recalculate on aggregated player stats
        # (less accurate but better than nothing)
        conn = get_connection()
        rows = conn.execute("SELECT * FROM players WHERE tournament_id = ?",
                            (tournament_id,)).fetchall()
        conn.close()
        for row in rows:
            p = dict(row)
            base, role_pts, total = calculate_player_points(p)
            conn = get_connection()
            conn.execute("""UPDATE players SET base_points=?, role_points=?, fantasy_points=?
                            WHERE player_id=? AND tournament_id=?""",
                         (base, role_pts, total, p["player_id"], tournament_id))
            conn.commit()
            conn.close()
        return

    # Group match rows by player_id
    buckets = defaultdict(list)
    for row in raw_rows:
        buckets[dict(row)["player_id"]].append(dict(row))

    # Get each player's role from the players table
    conn = get_connection()
    player_roles = {
        r["player_id"]: r["role"]
        for r in conn.execute(
            "SELECT player_id, role FROM players WHERE tournament_id = ?",
            (tournament_id,)
        ).fetchall()
    }
    conn.close()

    for pid, rows in buckets.items():
        role = player_roles.get(pid, "flex")
        total_base = total_role = total_pts = 0.0
        for r in rows:
            bp, rp, tp = calculate_player_points({
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
            })
            total_base += bp
            total_role += rp
            total_pts  += tp

        conn = get_connection()
        conn.execute("""UPDATE players SET base_points=?, role_points=?, fantasy_points=?
                        WHERE player_id=? AND tournament_id=?""",
                     (round(total_base, 4), round(total_role, 4),
                      round(total_pts, 2), pid, tournament_id))
        conn.commit()
        conn.close()


def last_scrape(tournament_id=None):
    """
    Return the most recent scrape log entry.
    If tournament_id is given, scoped to that tournament.
    Returns a dict or None.
    """
    conn = get_connection()
    if tournament_id:
        row = conn.execute(
            """SELECT * FROM scrape_log WHERE tournament_id = ?
               ORDER BY scraped_at DESC LIMIT 1""",
            (tournament_id,)
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT * FROM scrape_log ORDER BY scraped_at DESC LIMIT 1"
        ).fetchone()
    conn.close()
    return dict(row) if row else None

def log_scrape(tournament_id, source_id, players_found, status, notes=""):
    conn = get_connection()
    conn.execute("""INSERT INTO scrape_log (tournament_id, source_id, players_found, status, notes)
                    VALUES (?,?,?,?,?)""", (tournament_id, source_id, players_found, status, notes))
    conn.commit()
    conn.close()


# ═══════════════════════════════════════════════════════════════════════════════
# LEAGUES
# ═══════════════════════════════════════════════════════════════════════════════

def create_league(tournament_id, name, description="", ruleset=None):
    rs = json.dumps(ruleset or DEFAULT_RULESET)
    conn = get_connection()
    c = conn.cursor()
    c.execute("""INSERT INTO leagues (tournament_id, name, description, ruleset)
                 VALUES (?,?,?,?)""", (tournament_id, name, description, rs))
    lid = c.lastrowid
    conn.commit()
    conn.close()
    return lid


def get_all_leagues():
    conn = get_connection()
    rows = conn.execute("""
        SELECT l.*, t.name as tournament_name
        FROM leagues l
        JOIN tournaments t ON t.id = l.tournament_id
        ORDER BY l.id DESC
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_leagues_for_tournament(tournament_id):
    conn = get_connection()
    rows = conn.execute("SELECT * FROM leagues WHERE tournament_id = ? ORDER BY id",
                        (tournament_id,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_league(league_id):
    conn = get_connection()
    row = conn.execute("""
        SELECT l.*, t.name as tournament_name
        FROM leagues l JOIN tournaments t ON t.id = l.tournament_id
        WHERE l.id = ?
    """, (league_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def update_league_phase(league_id, phase):
    conn = get_connection()
    conn.execute("UPDATE leagues SET phase = ? WHERE id = ?", (phase, league_id))
    conn.commit()
    conn.close()


def _delete_league_cascade(conn, league_id):
    teams = conn.execute("SELECT id FROM fantasy_teams WHERE league_id = ?",
                         (league_id,)).fetchall()
    for t in teams:
        conn.execute("DELETE FROM fantasy_roster WHERE fantasy_team_id = ?", (t[0],))
        conn.execute("DELETE FROM followed_teams WHERE fantasy_team_id = ?", (t[0],))
        conn.execute("DELETE FROM point_adjustments WHERE fantasy_team_id = ?", (t[0],))
    conn.execute("DELETE FROM fantasy_teams WHERE league_id = ?", (league_id,))
    conn.execute("DELETE FROM trades WHERE league_id = ?", (league_id,))
    conn.execute("DELETE FROM draft_sessions WHERE league_id = ?", (league_id,))
    conn.execute("DELETE FROM leagues WHERE id = ?", (league_id,))


def delete_league(league_id):
    conn = get_connection()
    _delete_league_cascade(conn, league_id)
    conn.commit()
    conn.close()


# ═══════════════════════════════════════════════════════════════════════════════
# FANTASY TEAMS
# ═══════════════════════════════════════════════════════════════════════════════

def create_fantasy_team(league_id, team_name, manager_name):
    conn = get_connection()
    c = conn.cursor()
    c.execute("INSERT INTO fantasy_teams (league_id, team_name, manager_name) VALUES (?,?,?)",
              (league_id, team_name, manager_name))
    tid = c.lastrowid
    conn.commit()
    conn.close()
    return tid


def get_teams_in_league(league_id):
    conn = get_connection()
    rows = conn.execute("SELECT * FROM fantasy_teams WHERE league_id = ? ORDER BY id",
                        (league_id,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_fantasy_team(team_id):
    conn = get_connection()
    row = conn.execute("""
        SELECT ft.*, l.name as league_name, l.tournament_id, l.phase, l.ruleset
        FROM fantasy_teams ft
        JOIN leagues l ON l.id = ft.league_id
        WHERE ft.id = ?
    """, (team_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def delete_fantasy_team(team_id):
    conn = get_connection()
    conn.execute("DELETE FROM fantasy_roster WHERE fantasy_team_id = ?", (team_id,))
    conn.execute("DELETE FROM followed_teams WHERE fantasy_team_id = ?", (team_id,))
    conn.execute("DELETE FROM point_adjustments WHERE fantasy_team_id = ?", (team_id,))
    conn.execute("DELETE FROM fantasy_teams WHERE id = ?", (team_id,))
    conn.commit()
    conn.close()


def rename_fantasy_team(team_id, new_name, new_manager):
    conn = get_connection()
    conn.execute("UPDATE fantasy_teams SET team_name=?, manager_name=? WHERE id=?",
                 (new_name, new_manager, team_id))
    conn.commit()
    conn.close()


# ═══════════════════════════════════════════════════════════════════════════════
# FANTASY ROSTER
# ═══════════════════════════════════════════════════════════════════════════════

def get_roster(fantasy_team_id, phase=None):
    conn = get_connection()
    if phase:
        rows = conn.execute("""
            SELECT p.*, fr.role_slot, fr.is_star, fr.is_duplicate, fr.phase, fr.id as roster_id
            FROM players p
            JOIN fantasy_roster fr ON fr.player_id = p.player_id AND fr.tournament_id = p.tournament_id
            WHERE fr.fantasy_team_id = ? AND fr.phase = ?
            ORDER BY fr.role_slot
        """, (fantasy_team_id, phase)).fetchall()
    else:
        rows = conn.execute("""
            SELECT p.*, fr.role_slot, fr.is_star, fr.is_duplicate, fr.phase, fr.id as roster_id
            FROM players p
            JOIN fantasy_roster fr ON fr.player_id = p.player_id AND fr.tournament_id = p.tournament_id
            WHERE fr.fantasy_team_id = ?
            ORDER BY fr.phase, fr.role_slot
        """, (fantasy_team_id,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def add_player_to_roster(fantasy_team_id, player_id, tournament_id, role_slot, phase="swiss", is_duplicate=0):
    conn = get_connection()
    try:
        conn.execute("""
            INSERT INTO fantasy_roster (fantasy_team_id, player_id, tournament_id, role_slot, phase, is_duplicate)
            VALUES (?,?,?,?,?,?)
        """, (fantasy_team_id, player_id, tournament_id, role_slot, phase, is_duplicate))
        conn.commit()
        result = True
    except sqlite3.IntegrityError:
        result = False
    conn.close()
    return result


def remove_player_from_roster(fantasy_team_id, player_id, phase=None):
    conn = get_connection()
    if phase:
        conn.execute("""DELETE FROM fantasy_roster WHERE fantasy_team_id=? AND player_id=? AND phase=?""",
                     (fantasy_team_id, player_id, phase))
    else:
        conn.execute("DELETE FROM fantasy_roster WHERE fantasy_team_id=? AND player_id=?",
                     (fantasy_team_id, player_id))
    conn.commit()
    conn.close()


def set_star_player(fantasy_team_id, player_id, phase="swiss"):
    conn = get_connection()
    # Clear existing star for this team/phase
    conn.execute("UPDATE fantasy_roster SET is_star=0 WHERE fantasy_team_id=? AND phase=?",
                 (fantasy_team_id, phase))
    # Set new star
    conn.execute("""UPDATE fantasy_roster SET is_star=1
                    WHERE fantasy_team_id=? AND player_id=? AND phase=?""",
                 (fantasy_team_id, player_id, phase))
    conn.commit()
    conn.close()


def clear_star_player(fantasy_team_id, phase="swiss"):
    conn = get_connection()
    conn.execute("UPDATE fantasy_roster SET is_star=0 WHERE fantasy_team_id=? AND phase=?",
                 (fantasy_team_id, phase))
    conn.commit()
    conn.close()


def get_player_roster_assignments(player_id, tournament_id):
    """Return all fantasy teams this player is on."""
    conn = get_connection()
    rows = conn.execute("""
        SELECT fr.fantasy_team_id, fr.phase, ft.team_name
        FROM fantasy_roster fr
        JOIN fantasy_teams ft ON ft.id = fr.fantasy_team_id
        WHERE fr.player_id = ? AND fr.tournament_id = ?
    """, (player_id, tournament_id)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def transition_to_playoffs(league_id, kept_player_ids: list):
    """
    Move a league to playoffs phase.
    kept_player_ids: list of (fantasy_team_id, player_id) tuples to carry over.
    Removes non-qualifying players from all rosters, sets phase to 'playoffs'.
    """
    conn = get_connection()
    # Get all teams in league
    teams = conn.execute("SELECT id FROM fantasy_teams WHERE league_id = ?",
                         (league_id,)).fetchall()
    for team in teams:
        tid = team[0]
        # Remove all current swiss roster entries for eliminated players
        roster = conn.execute(
            "SELECT player_id FROM fantasy_roster WHERE fantasy_team_id = ? AND phase = 'swiss'",
            (tid,)).fetchall()
        for r in roster:
            pid = r[0]
            if (tid, pid) not in kept_player_ids:
                conn.execute("""DELETE FROM fantasy_roster
                                WHERE fantasy_team_id=? AND player_id=? AND phase='swiss'""",
                             (tid, pid))
    # Update league phase
    conn.execute("UPDATE leagues SET phase='playoffs' WHERE id=?", (league_id,))
    conn.commit()
    conn.close()


# ═══════════════════════════════════════════════════════════════════════════════
# FOLLOWED TEAMS
# ═══════════════════════════════════════════════════════════════════════════════

def set_followed_team(fantasy_team_id, team_name, team_region=""):
    conn = get_connection()
    conn.execute("""
        INSERT INTO followed_teams (fantasy_team_id, team_name, team_region)
        VALUES (?,?,?)
        ON CONFLICT(fantasy_team_id) DO UPDATE SET team_name=excluded.team_name,
            team_region=excluded.team_region
    """, (fantasy_team_id, team_name, team_region))
    conn.commit()
    conn.close()


def get_followed_team(fantasy_team_id):
    conn = get_connection()
    row = conn.execute("SELECT * FROM followed_teams WHERE fantasy_team_id = ?",
                       (fantasy_team_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def remove_followed_team(fantasy_team_id):
    conn = get_connection()
    conn.execute("DELETE FROM followed_teams WHERE fantasy_team_id = ?", (fantasy_team_id,))
    conn.commit()
    conn.close()


# ═══════════════════════════════════════════════════════════════════════════════
# MATCH RESULTS
# ═══════════════════════════════════════════════════════════════════════════════

def add_match_result(tournament_id, team_name, opponent, result, fmt="bo3"):
    conn = get_connection()
    conn.execute("""INSERT INTO match_results (tournament_id, team_name, opponent, result, format)
                    VALUES (?,?,?,?,?)""", (tournament_id, team_name, opponent, result, fmt))
    conn.commit()
    conn.close()


def get_match_results(tournament_id, team_name=None):
    conn = get_connection()
    if team_name:
        rows = conn.execute("""SELECT * FROM match_results WHERE tournament_id=? AND team_name=?
                               ORDER BY id DESC""", (tournament_id, team_name)).fetchall()
    else:
        rows = conn.execute("""SELECT * FROM match_results WHERE tournament_id=?
                               ORDER BY id DESC""", (tournament_id,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def delete_match_result(result_id):
    conn = get_connection()
    conn.execute("DELETE FROM match_results WHERE id = ?", (result_id,))
    conn.commit()
    conn.close()


def calculate_team_follow_points(fantasy_team_id, tournament_id):
    """Calculate win/loss points for a followed team."""
    followed = get_followed_team(fantasy_team_id)
    if not followed:
        return 0.0
    results = get_match_results(tournament_id, followed["team_name"])
    pts = 0.0
    for r in results:
        fmt = r["format"]
        result = r["result"]
        if result == "win":
            pts += 100 if fmt == "bo3" else 75
        else:
            pts += -75 if fmt == "bo3" else -50
    return pts


# ═══════════════════════════════════════════════════════════════════════════════
# MANUAL ADJUSTMENTS
# ═══════════════════════════════════════════════════════════════════════════════

def add_point_adjustment(fantasy_team_id, amount, reason=""):
    conn = get_connection()
    conn.execute("INSERT INTO point_adjustments (fantasy_team_id, amount, reason) VALUES (?,?,?)",
                 (fantasy_team_id, amount, reason))
    conn.commit()
    conn.close()


def get_adjustments(fantasy_team_id):
    conn = get_connection()
    rows = conn.execute("""SELECT * FROM point_adjustments WHERE fantasy_team_id = ?
                           ORDER BY id DESC""", (fantasy_team_id,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def delete_adjustment(adjustment_id):
    conn = get_connection()
    conn.execute("DELETE FROM point_adjustments WHERE id = ?", (adjustment_id,))
    conn.commit()
    conn.close()


def get_total_adjustments(fantasy_team_id):
    conn = get_connection()
    row = conn.execute("""SELECT COALESCE(SUM(amount), 0) as total
                          FROM point_adjustments WHERE fantasy_team_id = ?""",
                       (fantasy_team_id,)).fetchone()
    conn.close()
    return row["total"] if row else 0.0


# ═══════════════════════════════════════════════════════════════════════════════
# STANDINGS
# ═══════════════════════════════════════════════════════════════════════════════

def get_league_standings(league_id):
    league = get_league(league_id)
    if not league:
        return []
    teams = get_teams_in_league(league_id)
    phase = league["phase"]
    standings = []
    for t in teams:
        roster = get_roster(t["id"], phase=phase)
        player_pts = 0.0
        star_bonus = 0.0
        for p in roster:
            pts = p["fantasy_points"] + p.get("manual_pts", 0.0)
            if p["is_star"]:
                star_bonus += pts * 0.5  # extra 50% on top
            player_pts += pts
        follow_pts = calculate_team_follow_points(t["id"], league["tournament_id"])
        adj_pts = get_total_adjustments(t["id"])
        total = round(player_pts + star_bonus + follow_pts + adj_pts, 2)
        followed = get_followed_team(t["id"])
        standings.append({
            **t,
            "player_pts": round(player_pts, 2),
            "star_bonus": round(star_bonus, 2),
            "follow_pts": round(follow_pts, 2),
            "adj_pts": round(adj_pts, 2),
            "total_points": total,
            "player_count": len(roster),
            "followed_team": followed["team_name"] if followed else None,
        })
    standings.sort(key=lambda x: x["total_points"], reverse=True)
    return standings


# ═══════════════════════════════════════════════════════════════════════════════
# TRADES
# ═══════════════════════════════════════════════════════════════════════════════

def propose_trade(league_id, from_team_id, to_team_id, from_player_id, to_player_id, tournament_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute("""INSERT INTO trades (league_id, from_team_id, to_team_id, from_player_id,
                 to_player_id, tournament_id) VALUES (?,?,?,?,?,?)""",
              (league_id, from_team_id, to_team_id, from_player_id, to_player_id, tournament_id))
    tid = c.lastrowid
    conn.commit()
    conn.close()
    return tid


def get_trades(league_id):
    conn = get_connection()
    rows = conn.execute("""SELECT t.*,
        ft1.team_name as from_team_name, ft2.team_name as to_team_name,
        p1.ign as from_player_ign, p2.ign as to_player_ign
        FROM trades t
        JOIN fantasy_teams ft1 ON ft1.id = t.from_team_id
        JOIN fantasy_teams ft2 ON ft2.id = t.to_team_id
        LEFT JOIN players p1 ON p1.player_id = t.from_player_id AND p1.tournament_id = t.tournament_id
        LEFT JOIN players p2 ON p2.player_id = t.to_player_id AND p2.tournament_id = t.tournament_id
        WHERE t.league_id = ?
        ORDER BY t.id DESC
    """, (league_id,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def resolve_trade(trade_id, action):
    """action: 'accepted' or 'rejected'"""
    conn = get_connection()
    trade = conn.execute("SELECT * FROM trades WHERE id = ?", (trade_id,)).fetchone()
    if not trade or trade["status"] != "pending":
        conn.close()
        return False
    trade = dict(trade)

    if action == "accepted":
        # Get role slots
        from_slot = conn.execute("""SELECT role_slot, phase FROM fantasy_roster
                                    WHERE fantasy_team_id=? AND player_id=?""",
                                 (trade["from_team_id"], trade["from_player_id"])).fetchone()
        to_slot = conn.execute("""SELECT role_slot, phase FROM fantasy_roster
                                  WHERE fantasy_team_id=? AND player_id=?""",
                               (trade["to_team_id"], trade["to_player_id"])).fetchone()
        if from_slot and to_slot:
            # Swap players
            conn.execute("DELETE FROM fantasy_roster WHERE fantasy_team_id=? AND player_id=?",
                         (trade["from_team_id"], trade["from_player_id"]))
            conn.execute("DELETE FROM fantasy_roster WHERE fantasy_team_id=? AND player_id=?",
                         (trade["to_team_id"], trade["to_player_id"]))
            conn.execute("""INSERT INTO fantasy_roster (fantasy_team_id, player_id, tournament_id, role_slot, phase)
                            VALUES (?,?,?,?,?)""",
                         (trade["from_team_id"], trade["to_player_id"], trade["tournament_id"],
                          from_slot["role_slot"], from_slot["phase"]))
            conn.execute("""INSERT INTO fantasy_roster (fantasy_team_id, player_id, tournament_id, role_slot, phase)
                            VALUES (?,?,?,?,?)""",
                         (trade["to_team_id"], trade["from_player_id"], trade["tournament_id"],
                          to_slot["role_slot"], to_slot["phase"]))

    conn.execute("""UPDATE trades SET status=?, resolved_at=datetime('now') WHERE id=?""",
                 (action, trade_id))
    conn.commit()
    conn.close()
    return True


def cancel_trade(trade_id):
    conn = get_connection()
    conn.execute("UPDATE trades SET status='cancelled', resolved_at=datetime('now') WHERE id=?",
                 (trade_id,))
    conn.commit()
    conn.close()


# ═══════════════════════════════════════════════════════════════════════════════
# DRAFT
# ═══════════════════════════════════════════════════════════════════════════════

def create_draft_session(league_id, phase="swiss"):
    teams = get_teams_in_league(league_id)
    league = get_league(league_id)
    if not teams or not league:
        return None
    rs = get_ruleset(league_id)
    total_players = rs.get("total_players", 10)
    n = len(teams)
    # Snake order: 1,2,3,...n,n,...3,2,1,1,2,...
    forward = list(range(n))
    backward = list(reversed(range(n)))
    snake = []
    round_num = 0
    while len(snake) < total_players * n:
        snake += forward if round_num % 2 == 0 else backward
        round_num += 1
    snake = snake[:total_players * n]
    team_ids = [t["id"] for t in teams]
    snake_team_ids = [team_ids[i] for i in snake]

    conn = get_connection()
    c = conn.cursor()
    c.execute("""INSERT INTO draft_sessions (league_id, phase, status, total_picks, snake_order)
                 VALUES (?,?,?,?,?)""",
              (league_id, phase, "active", total_players * n, json.dumps(snake_team_ids)))
    did = c.lastrowid
    conn.commit()
    conn.close()
    return did


def get_active_draft(league_id):
    conn = get_connection()
    row = conn.execute("""SELECT * FROM draft_sessions WHERE league_id=? AND status='active'
                          ORDER BY id DESC LIMIT 1""", (league_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def advance_draft(draft_id):
    conn = get_connection()
    draft = conn.execute("SELECT * FROM draft_sessions WHERE id=?", (draft_id,)).fetchone()
    if not draft:
        conn.close()
        return
    draft = dict(draft)
    new_pick = draft["current_pick"] + 1
    if new_pick > draft["total_picks"]:
        conn.execute("UPDATE draft_sessions SET status='complete', current_pick=? WHERE id=?",
                     (new_pick, draft_id))
    else:
        conn.execute("UPDATE draft_sessions SET current_pick=? WHERE id=?", (new_pick, draft_id))
    conn.commit()
    conn.close()


def get_current_drafter(draft_id):
    conn = get_connection()
    draft = conn.execute("SELECT * FROM draft_sessions WHERE id=?", (draft_id,)).fetchone()
    conn.close()
    if not draft:
        return None
    draft = dict(draft)
    order = json.loads(draft["snake_order"])
    idx = draft["current_pick"] - 1
    if idx >= len(order):
        return None
    return order[idx]


def patch_match_player_stats(player_id: str, match_id: str, tournament_id: int, fields: dict):
    """
    Admin patch: update specific stat fields for a player in a specific match.
    After patching, clears stats_incomplete if all required fields are now present.
    """
    if not fields:
        return
    allowed = {"rating","acs","kills","deaths","assists","kast","adr",
                "first_kills","first_deaths","headshot_pct"}
    safe = {k: v for k, v in fields.items() if k in allowed}
    if not safe:
        return
    conn = get_connection()
    sets = ", ".join(f"{k}=?" for k in safe)
    vals = list(safe.values())
    # Check remaining missing fields
    row = conn.execute(
        "SELECT missing_fields FROM match_player_stats WHERE player_id=? AND match_id=?",
        (player_id, match_id)
    ).fetchone()
    if row:
        still_missing = [f for f in (row["missing_fields"] or "").split(",")
                         if f.strip() and f.strip() not in safe]
        incomplete = 1 if still_missing else 0
        sets += ", missing_fields=?, stats_incomplete=?"
        vals += [",".join(still_missing), incomplete]
    vals += [player_id, match_id]
    conn.execute(f"UPDATE match_player_stats SET {sets} WHERE player_id=? AND match_id=?", vals)
    conn.commit()
    conn.close()


def get_incomplete_matches(tournament_id: int) -> list:
    """Return match_ids that have at least one player with stats_incomplete=1."""
    conn = get_connection()
    rows = conn.execute(
        """SELECT DISTINCT mps.match_id, m.team_a, m.team_b, m.match_url,
                  GROUP_CONCAT(mps.ign) as affected_players
           FROM match_player_stats mps
           JOIN matches m ON m.match_id=mps.match_id AND m.tournament_id=mps.tournament_id
           WHERE mps.tournament_id=? AND mps.stats_incomplete=1
           GROUP BY mps.match_id""",
        (tournament_id,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]

