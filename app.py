"""
app.py — VCT Fantasy League Manager v2
Full Flask application with multi-tournament support,
custom rulesets, phases, trades, and commissioner tools.
"""

import json
import time
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, session
import database as db
import scraper as sc
from points import calculate_player_points, points_breakdown, all_role_points

app = Flask(__name__)
app.secret_key = "vct_fantasy_v2_secret_2026"
db.init_db()

# ─── Auth config ──────────────────────────────────────────────────────────────

ADMIN_USERNAME = "FaiziSA"
ADMIN_PASSWORD = "@VCT_FL"


def is_admin():
    """Return True if the current session is authenticated as admin."""
    return session.get("logged_in") is True


def admin_required():
    """
    Call at the top of any write route.
    Returns a redirect response if not admin, otherwise None.
    """
    if not is_admin():
        flash("You must be logged in as admin to do that.", "error")
        return redirect(url_for("login"))
    return None


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        if username == ADMIN_USERNAME and password == ADMIN_PASSWORD:
            session["logged_in"] = True
            flash("Welcome back, FaiziSA!", "success")
            return redirect(request.form.get("next") or url_for("index"))
        flash("Invalid username or password.", "error")
    return render_template("login.html", next=request.args.get("next", ""))


@app.route("/logout")
def logout():
    session.clear()
    flash("Logged out.", "success")
    return redirect(url_for("index"))



# ─── Helpers ──────────────────────────────────────────────────────────────────

def _int(v, default=0):
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


def _float(v, default=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


# ═══════════════════════════════════════════════════════════════════════════════
# DASHBOARD
# ═══════════════════════════════════════════════════════════════════════════════


@app.context_processor
def inject_auth():
    """Make is_admin available in every template as {{ admin }}."""
    return {"admin": is_admin()}


@app.route("/")
def index():
    tournaments = db.get_all_tournaments()
    leagues     = db.get_all_leagues()
    total_players = 0
    for t in tournaments:
        players = db.get_players(t["id"])
        total_players += len(players)
        t["player_count"] = len(players)
        t["league_count"] = len(db.get_leagues_for_tournament(t["id"]))
        t["source_count"]  = len(db.get_event_sources(t["id"]))
    return render_template("index.html",
                           tournaments=tournaments, leagues=leagues,
                           total_players=total_players)


# ═══════════════════════════════════════════════════════════════════════════════
# TOURNAMENTS
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/tournaments")
def tournaments():
    all_t = db.get_all_tournaments()
    for t in all_t:
        t["sources"]  = db.get_event_sources(t["id"])
        t["leagues"]  = db.get_leagues_for_tournament(t["id"])
        t["players"]  = len(db.get_players(t["id"]))
    return render_template("tournaments.html", tournaments=all_t)


@app.route("/tournaments/create", methods=["GET", "POST"])
def create_tournament():
    auth_err = admin_required()
    if auth_err:
        return auth_err
    if request.method == "POST":
        name = request.form.get("name","").strip()
        desc = request.form.get("description","").strip()
        fmt  = request.form.get("format","standard")
        if not name:
            flash("Tournament name is required.", "error")
            return render_template("create_tournament.html")
        tid = db.create_tournament(name, desc, fmt)

        # Handle multiple event sources submitted together
        urls    = request.form.getlist("source_url")
        names   = request.form.getlist("source_name")
        regions = request.form.getlist("source_region")
        for i, url in enumerate(urls):
            url = url.strip()
            if url:
                db.add_event_source(tid, url,
                                    event_name=names[i] if i < len(names) else "",
                                    region=regions[i] if i < len(regions) else "")
        flash(f'Tournament "{name}" created!', "success")
        return redirect(url_for("tournament_detail", tournament_id=tid))
    return render_template("create_tournament.html")


@app.route("/tournaments/<int:tournament_id>")
def tournament_detail(tournament_id):
    t = db.get_tournament(tournament_id)
    if not t:
        flash("Tournament not found.", "error")
        return redirect(url_for("tournaments"))
    sources = db.get_event_sources(tournament_id)
    leagues = db.get_leagues_for_tournament(tournament_id)
    players = db.get_players(tournament_id, sort_by="fantasy_points")[:10]
    match_results = db.get_match_results(tournament_id)
    all_matches = db.get_matches(tournament_id)
    upcoming_matches = [m for m in all_matches if m.get("status") == "upcoming"][:5]
    return render_template("tournament_detail.html",
                           tournament=t, sources=sources,
                           leagues=leagues, top_players=players,
                           match_results=match_results,
                           upcoming_matches=upcoming_matches,
                           match_count=len(all_matches))


@app.route("/tournaments/<int:tournament_id>/add_source", methods=["POST"])
def add_source(tournament_id):
    auth_err = admin_required()
    if auth_err:
        return auth_err
    url    = request.form.get("url","").strip()
    name   = request.form.get("name","").strip()
    region = request.form.get("region","").strip()
    if not url:
        flash("URL is required.", "error")
    else:
        db.add_event_source(tournament_id, url, name, region)
        flash(f"Event source added: {name or url}", "success")
    return redirect(url_for("tournament_detail", tournament_id=tournament_id))


@app.route("/tournaments/<int:tournament_id>/delete_source/<int:source_id>", methods=["POST"])
def delete_source(tournament_id, source_id):
    auth_err = admin_required()
    if auth_err:
        return auth_err
    db.delete_event_source(source_id)
    flash("Event source removed.", "success")
    return redirect(url_for("tournament_detail", tournament_id=tournament_id))


@app.route("/tournaments/<int:tournament_id>/scrape_all", methods=["POST"])
def scrape_all(tournament_id):
    auth_err = admin_required()
    if auth_err:
        return auth_err
    total, msgs = sc.scrape_all_sources(tournament_id)
    for m in msgs:
        flash(m, "success" if "Successfully" in m else "error")
    if total > 0:
        db.recalculate_tournament_points(tournament_id)
        flash(f"Points recalculated for all {total} players.", "success")
    return redirect(url_for("tournament_detail", tournament_id=tournament_id))


@app.route("/tournaments/<int:tournament_id>/scrape_source/<int:source_id>", methods=["POST"])
def scrape_single_source(tournament_id, source_id):
    auth_err = admin_required()
    if auth_err:
        return auth_err
    source = db.get_event_source(source_id)
    if not source:
        flash("Source not found.", "error")
        return redirect(url_for("tournament_detail", tournament_id=tournament_id))
    count, msg = sc.scrape_source(source)
    flash(msg, "success" if count > 0 else "error")
    if count > 0:
        db.recalculate_tournament_points(tournament_id)
    return redirect(url_for("tournament_detail", tournament_id=tournament_id))


@app.route("/tournaments/<int:tournament_id>/recalculate", methods=["POST"])
def recalculate_points(tournament_id):
    auth_err = admin_required()
    if auth_err:
        return auth_err
    db.recalculate_tournament_points(tournament_id)
    flash("Points recalculated for all players.", "success")
    return redirect(url_for("tournament_detail", tournament_id=tournament_id))


@app.route("/tournaments/<int:tournament_id>/delete", methods=["POST"])
def delete_tournament(tournament_id):
    auth_err = admin_required()
    if auth_err:
        return auth_err
    t = db.get_tournament(tournament_id)
    if t:
        db.delete_tournament(tournament_id)
        flash(f'Tournament "{t["name"]}" deleted.', "success")
    return redirect(url_for("tournaments"))


@app.route("/tournaments/<int:tournament_id>/add_result", methods=["POST"])
def add_match_result(tournament_id):
    auth_err = admin_required()
    if auth_err:
        return auth_err
    team   = request.form.get("team_name","").strip()
    opp    = request.form.get("opponent","").strip()
    result = request.form.get("result","win")
    fmt    = request.form.get("format","bo3")
    if not team:
        flash("Team name is required.", "error")
    else:
        db.add_match_result(tournament_id, team, opp, result, fmt)
        flash(f"Match result added: {team} {result} vs {opp}", "success")
    return redirect(url_for("tournament_detail", tournament_id=tournament_id))


@app.route("/tournaments/<int:tournament_id>/delete_result/<int:result_id>", methods=["POST"])
def delete_match_result(tournament_id, result_id):
    auth_err = admin_required()
    if auth_err:
        return auth_err
    db.delete_match_result(result_id)
    flash("Match result removed.", "success")
    return redirect(url_for("tournament_detail", tournament_id=tournament_id))


# ═══════════════════════════════════════════════════════════════════════════════
# PLAYERS
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/tournament/<int:tournament_id>/players")
def players(tournament_id):
    t = db.get_tournament(tournament_id)
    if not t:
        flash("Tournament not found.", "error")
        return redirect(url_for("tournaments"))
    sort_by     = request.args.get("sort","fantasy_points")
    order       = request.args.get("order","desc")
    search      = request.args.get("q","").strip()
    role_filter = request.args.get("role","")
    all_players = db.get_players(tournament_id, sort_by, order, search, role_filter)
    roles = ["duelist","initiator","controller","sentinel","flex"]
    # Compute all-role points for leaderboard columns
    role_pts_map = {p["player_id"]: all_role_points(p) for p in all_players}
    return render_template("players.html", players=all_players, tournament=t,
                           sort_by=sort_by, order=order, search=search,
                           role_filter=role_filter, roles=roles,
                           role_pts_map=role_pts_map)


@app.route("/tournament/<int:tournament_id>/player/<player_id>")
def player_detail(tournament_id, player_id):
    p = db.get_player(player_id, tournament_id)
    t = db.get_tournament(tournament_id)
    if not p or not t:
        flash("Player not found.", "error")
        return redirect(url_for("players", tournament_id=tournament_id))
    breakdown  = points_breakdown(p)
    assignments = db.get_player_roster_assignments(player_id, tournament_id)
    # Build available teams for adding
    leagues = db.get_leagues_for_tournament(tournament_id)
    already_on = {a["fantasy_team_id"] for a in assignments}
    available_teams = []
    for lg in leagues:
        rs = db.get_ruleset(lg["id"])
        for team in db.get_teams_in_league(lg["id"]):
            if team["id"] not in already_on:
                roster = db.get_roster(team["id"], phase=lg["phase"])
                max_p = rs.get("total_players", 10)
                if len(roster) < max_p:
                    available_teams.append({**team, "league_name": lg["name"],
                                            "phase": lg["phase"]})
    role_pts = all_role_points(p)
    adjustments = db.get_player_adjustments(player_id, tournament_id)
    return render_template("player_detail.html", player=p, tournament=t,
                           breakdown=breakdown, assignments=assignments,
                           available_teams=available_teams,
                           role_pts=role_pts, adjustments=adjustments)


@app.route("/tournament/<int:tournament_id>/player/<player_id>/set_role", methods=["POST"])
def set_player_role(tournament_id, player_id):
    auth_err = admin_required()
    if auth_err:
        return auth_err
    role = request.form.get("role","flex")
    db.update_player_role(player_id, tournament_id, role)
    # Recalculate points with new role
    p = db.get_player(player_id, tournament_id)
    if p:
        base, role_pts, total = calculate_player_points(p, role)
        conn = db.get_connection()
        conn.execute("UPDATE players SET role=?, base_points=?, role_points=?, fantasy_points=? WHERE player_id=? AND tournament_id=?",
                     (role, base, role_pts, total, player_id, tournament_id))
        conn.commit()
        conn.close()
    flash(f"Role updated to {role}.", "success")
    return redirect(request.referrer or url_for("player_detail", tournament_id=tournament_id, player_id=player_id))


@app.route("/tournament/<int:tournament_id>/player/<player_id>/set_region", methods=["POST"])
def set_player_region(tournament_id, player_id):
    auth_err = admin_required()
    if auth_err:
        return auth_err
    region = request.form.get("region","")
    db.update_player_region(player_id, tournament_id, region)
    flash(f"Region set to {region}.", "success")
    return redirect(request.referrer or url_for("player_detail", tournament_id=tournament_id, player_id=player_id))


# ═══════════════════════════════════════════════════════════════════════════════
# LEAGUES
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/leagues")
def leagues():
    all_leagues = db.get_all_leagues()
    return render_template("leagues.html", leagues=all_leagues)


@app.route("/tournaments/<int:tournament_id>/create_league", methods=["GET", "POST"])
def create_league(tournament_id):
    auth_err = admin_required()
    if auth_err:
        return auth_err
    t = db.get_tournament(tournament_id)
    if not t:
        flash("Tournament not found.", "error")
        return redirect(url_for("tournaments"))

    if request.method == "POST":
        name = request.form.get("name","").strip()
        desc = request.form.get("description","").strip()
        if not name:
            flash("League name is required.", "error")
            return render_template("create_league.html", tournament=t,
                                   default_ruleset=db.DEFAULT_RULESET)

        # Build ruleset from form
        ruleset = {
            "total_players": _int(request.form.get("total_players"), 10),
            "role_requirements": {
                "duelist":    _int(request.form.get("role_duelist"),    2),
                "initiator":  _int(request.form.get("role_initiator"),  2),
                "controller": _int(request.form.get("role_controller"), 2),
                "sentinel":   _int(request.form.get("role_sentinel"),   2),
                "flex":       _int(request.form.get("role_flex"),       2),
            },
            "region_requirements": {
                "EMEA": _int(request.form.get("reg_EMEA"), 2),
                "AMER": _int(request.form.get("reg_AMER"), 2),
                "APAC": _int(request.form.get("reg_APAC"), 2),
                "CN":   _int(request.form.get("reg_CN"),   2),
            },
            "max_per_team":            _int(request.form.get("max_per_team"), 1),
            "individual_locked":       "individual_locked" in request.form,
            "swiss_duplicate_allowed": "swiss_duplicate_allowed" in request.form,
            "swiss_unique_required":   _int(request.form.get("swiss_unique_required"), 4),
            "star_player_enabled":     "star_player_enabled" in request.form,
            "team_following_enabled":  "team_following_enabled" in request.form,
            "snake_draft":             "snake_draft" in request.form,
            "single_phase":           "single_phase" in request.form,
        }
        lid = db.create_league(tournament_id, name, desc, ruleset)
        flash(f'League "{name}" created!', "success")
        return redirect(url_for("league_detail", league_id=lid))

    return render_template("create_league.html", tournament=t,
                           default_ruleset=db.DEFAULT_RULESET)


@app.route("/league/<int:league_id>")
def league_detail(league_id):
    league = db.get_league(league_id)
    if not league:
        flash("League not found.", "error")
        return redirect(url_for("leagues"))
    teams      = db.get_teams_in_league(league_id)
    standings  = db.get_league_standings(league_id)
    ruleset    = db.get_ruleset(league_id)
    tournament = db.get_tournament(league["tournament_id"])
    trades     = db.get_trades(league_id)
    draft      = db.get_active_draft(league_id)
    swiss_standings    = db.get_phase_standings(league_id, "swiss")
    playoffs_standings = db.get_phase_standings(league_id, "playoffs")
    overall_standings  = db.get_overall_standings(league_id)
    return render_template("league_detail.html",
                           league=league, teams=teams,
                           standings=standings, ruleset=ruleset,
                           tournament=tournament, trades=trades,
                           draft=draft,
                           swiss_standings=swiss_standings,
                           playoffs_standings=playoffs_standings,
                           overall_standings=overall_standings)


@app.route("/league/<int:league_id>/edit_ruleset", methods=["GET", "POST"])
def edit_ruleset(league_id):
    auth_err = admin_required()
    if auth_err:
        return auth_err
    league = db.get_league(league_id)
    if not league:
        flash("League not found.", "error")
        return redirect(url_for("leagues"))
    if request.method == "POST":
        ruleset = {
            "total_players": _int(request.form.get("total_players"), 10),
            "role_requirements": {
                "duelist":    _int(request.form.get("role_duelist"),    2),
                "initiator":  _int(request.form.get("role_initiator"),  2),
                "controller": _int(request.form.get("role_controller"), 2),
                "sentinel":   _int(request.form.get("role_sentinel"),   2),
                "flex":       _int(request.form.get("role_flex"),       2),
            },
            "region_requirements": {
                "EMEA": _int(request.form.get("reg_EMEA"), 2),
                "AMER": _int(request.form.get("reg_AMER"), 2),
                "APAC": _int(request.form.get("reg_APAC"), 2),
                "CN":   _int(request.form.get("reg_CN"),   2),
            },
            "max_per_team":            _int(request.form.get("max_per_team"), 1),
            "individual_locked":       "individual_locked" in request.form,
            "swiss_duplicate_allowed": "swiss_duplicate_allowed" in request.form,
            "swiss_unique_required":   _int(request.form.get("swiss_unique_required"), 4),
            "star_player_enabled":     "star_player_enabled" in request.form,
            "team_following_enabled":  "team_following_enabled" in request.form,
            "snake_draft":             "snake_draft" in request.form,
            "single_phase":           "single_phase" in request.form,
        }
        db.save_ruleset(league_id, ruleset)
        flash("Ruleset updated!", "success")
        return redirect(url_for("league_detail", league_id=league_id))
    ruleset = db.get_ruleset(league_id)
    return render_template("edit_ruleset.html", league=league, ruleset=ruleset)


@app.route("/league/<int:league_id>/advance_phase", methods=["POST"])
def advance_phase(league_id):
    auth_err = admin_required()
    if auth_err:
        return auth_err
    league = db.get_league(league_id)
    if not league:
        flash("League not found.", "error")
        return redirect(url_for("leagues"))
    new_phase = "playoffs" if league["phase"] == "swiss" else "swiss"
    db.update_league_phase(league_id, new_phase)
    flash(f"League phase changed to {new_phase.upper()}.", "success")
    return redirect(url_for("league_detail", league_id=league_id))


@app.route("/league/<int:league_id>/delete", methods=["POST"])
def delete_league(league_id):
    auth_err = admin_required()
    if auth_err:
        return auth_err
    league = db.get_league(league_id)
    if league:
        db.delete_league(league_id)
        flash(f'League "{league["name"]}" deleted.', "success")
    return redirect(url_for("leagues"))


# ═══════════════════════════════════════════════════════════════════════════════
# FANTASY TEAMS
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/league/<int:league_id>/create_team", methods=["GET", "POST"])
def create_team(league_id):
    auth_err = admin_required()
    if auth_err:
        return auth_err
    league = db.get_league(league_id)
    if not league:
        flash("League not found.", "error")
        return redirect(url_for("leagues"))
    if request.method == "POST":
        tname = request.form.get("team_name","").strip()
        mgr   = request.form.get("manager_name","").strip()
        if not tname or not mgr:
            flash("Team name and manager required.", "error")
            return render_template("create_team.html", league=league)
        tid = db.create_fantasy_team(league_id, tname, mgr)
        flash(f'Team "{tname}" created!', "success")
        return redirect(url_for("team_detail", team_id=tid))
    return render_template("create_team.html", league=league)


@app.route("/team/<int:team_id>")
def team_detail(team_id):
    team = db.get_fantasy_team(team_id)
    if not team:
        flash("Team not found.", "error")
        return redirect(url_for("leagues"))
    league   = db.get_league(team["league_id"])
    ruleset  = db.get_ruleset(team["league_id"])
    phase    = league["phase"]
    roster   = db.get_roster(team_id, phase=phase)
    followed = db.get_followed_team(team_id)
    adjustments = db.get_adjustments(team_id)

    # Points breakdown
    player_pts = sum(p["fantasy_points"] for p in roster)
    star_bonus = sum(p["fantasy_points"] * 0.5 for p in roster if p["is_star"])
    follow_pts = db.calculate_team_follow_points(team_id, team["tournament_id"])
    adj_pts    = db.get_total_adjustments(team_id)
    total_pts  = round(player_pts + star_bonus + follow_pts + adj_pts, 2)

    # Available players (not on this team in this phase)
    roster_player_ids = {p["player_id"] for p in roster}
    all_players = db.get_players(team["tournament_id"], sort_by="fantasy_points")
    available = [p for p in all_players if p["player_id"] not in roster_player_ids]

    max_p = ruleset.get("total_players", 10)
    roles = ["duelist","initiator","controller","sentinel","flex"]

    # Role slot counts in current roster
    role_counts = {r: sum(1 for p in roster if p["role_slot"] == r) for r in roles}
    role_reqs   = ruleset.get("role_requirements", {r: 0 for r in roles})

    # Region breakdown
    region_counts = {}
    for p in roster:
        reg = p.get("region","?") or "?"
        region_counts[reg] = region_counts.get(reg, 0) + 1

    # Has star player already?
    has_star = any(p["is_star"] for p in roster)

    # Roster phases
    swiss_roster    = db.get_roster(team_id, phase="swiss")
    playoffs_roster = db.get_roster(team_id, phase="playoffs")

    return render_template("team_detail.html",
                           team=team, league=league, ruleset=ruleset, phase=phase,
                           roster=roster, available=available,
                           followed=followed, adjustments=adjustments,
                           player_pts=player_pts, star_bonus=star_bonus,
                           follow_pts=follow_pts, adj_pts=adj_pts,
                           total_pts=total_pts, max_p=max_p,
                           roles=roles, role_counts=role_counts,
                           role_reqs=role_reqs, region_counts=region_counts,
                           has_star=has_star,
                           swiss_roster=swiss_roster,
                           playoffs_roster=playoffs_roster)


@app.route("/team/<int:team_id>/add_player", methods=["POST"])
def add_player(team_id):
    auth_err = admin_required()
    if auth_err:
        return auth_err
    team    = db.get_fantasy_team(team_id)
    league  = db.get_league(team["league_id"]) if team else None
    if not team or not league:
        flash("Team not found.", "error")
        return redirect(url_for("leagues"))
    player_id    = request.form.get("player_id","")
    role_slot    = request.form.get("role_slot","flex")
    phase        = request.form.get("phase", league["phase"])
    is_duplicate = 1 if "is_duplicate" in request.form else 0

    roster = db.get_roster(team_id, phase=phase)
    ruleset = db.get_ruleset(team["league_id"])
    max_p = ruleset.get("total_players", 10)

    if len(roster) >= max_p:
        flash(f"Roster is full ({max_p} max).", "error")
    else:
        p = db.get_player(player_id, team["tournament_id"])
        ok = db.add_player_to_roster(team_id, player_id, team["tournament_id"],
                                     role_slot, phase, is_duplicate)
        flash(f'Added {p["ign"] if p else player_id} to {role_slot}!' if ok
              else "Player already on roster.", "success" if ok else "warning")

    return redirect(request.referrer or url_for("team_detail", team_id=team_id))


@app.route("/team/<int:team_id>/remove_player", methods=["POST"])
def remove_player(team_id):
    auth_err = admin_required()
    if auth_err:
        return auth_err
    player_id = request.form.get("player_id","")
    phase     = request.form.get("phase", None)
    db.remove_player_from_roster(team_id, player_id, phase)
    p = db.get_fantasy_team(team_id)
    flash("Player removed from roster.", "success")
    return redirect(request.referrer or url_for("team_detail", team_id=team_id))


@app.route("/team/<int:team_id>/set_star", methods=["POST"])
def set_star(team_id):
    auth_err = admin_required()
    if auth_err:
        return auth_err
    player_id = request.form.get("player_id","")
    phase = request.form.get("phase","swiss")
    db.set_star_player(team_id, player_id, phase)
    p = db.get_player(player_id, db.get_fantasy_team(team_id)["tournament_id"])
    flash(f'⭐ {p["ign"] if p else player_id} set as Star Player!', "success")
    return redirect(request.referrer or url_for("team_detail", team_id=team_id))


@app.route("/team/<int:team_id>/clear_star", methods=["POST"])
def clear_star(team_id):
    auth_err = admin_required()
    if auth_err:
        return auth_err
    phase = request.form.get("phase","swiss")
    db.clear_star_player(team_id, phase)
    flash("Star player cleared.", "success")
    return redirect(request.referrer or url_for("team_detail", team_id=team_id))


@app.route("/team/<int:team_id>/set_followed_team", methods=["POST"])
def set_followed_team(team_id):
    auth_err = admin_required()
    if auth_err:
        return auth_err
    team_name = request.form.get("team_name","").strip()
    region    = request.form.get("team_region","").strip()
    if not team_name:
        db.remove_followed_team(team_id)
        flash("Followed team removed.", "success")
    else:
        db.set_followed_team(team_id, team_name, region)
        flash(f"Now following {team_name}!", "success")
    return redirect(request.referrer or url_for("team_detail", team_id=team_id))


@app.route("/team/<int:team_id>/rename", methods=["POST"])
def rename_team(team_id):
    auth_err = admin_required()
    if auth_err:
        return auth_err
    new_name = request.form.get("team_name","").strip()
    new_mgr  = request.form.get("manager_name","").strip()
    if new_name and new_mgr:
        db.rename_fantasy_team(team_id, new_name, new_mgr)
        flash("Team renamed.", "success")
    return redirect(request.referrer or url_for("team_detail", team_id=team_id))


@app.route("/team/<int:team_id>/delete", methods=["POST"])
def delete_team(team_id):
    auth_err = admin_required()
    if auth_err:
        return auth_err
    team = db.get_fantasy_team(team_id)
    lid  = team["league_id"] if team else None
    if team:
        db.delete_fantasy_team(team_id)
        flash(f'Team "{team["team_name"]}" deleted.', "success")
    return redirect(url_for("league_detail", league_id=lid) if lid else url_for("leagues"))


# ═══════════════════════════════════════════════════════════════════════════════
# COMMISSIONER TOOLS
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/league/<int:league_id>/commissioner")
def commissioner(league_id):
    league = db.get_league(league_id)
    if not league:
        flash("League not found.", "error")
        return redirect(url_for("leagues"))
    teams       = db.get_teams_in_league(league_id)
    tournament  = db.get_tournament(league["tournament_id"])
    standings   = db.get_league_standings(league_id)
    trades      = db.get_trades(league_id)
    all_players = db.get_players(league["tournament_id"], sort_by="ign", order="asc")

    # Build adjustments per team
    for t in teams:
        t["adjustments"] = db.get_adjustments(t["id"])
        t["adj_total"]   = db.get_total_adjustments(t["id"])

    return render_template("commissioner.html",
                           league=league, teams=teams,
                           tournament=tournament, standings=standings,
                           trades=trades, all_players=all_players)


@app.route("/team/<int:team_id>/add_adjustment", methods=["POST"])
def add_adjustment(team_id):
    auth_err = admin_required()
    if auth_err:
        return auth_err
    amount = _float(request.form.get("amount", 0))
    reason = request.form.get("reason","").strip()
    db.add_point_adjustment(team_id, amount, reason)
    flash(f"Adjustment of {amount:+.1f} pts added.", "success")
    return redirect(request.referrer or url_for("team_detail", team_id=team_id))


@app.route("/adjustment/<int:adj_id>/delete", methods=["POST"])
def delete_adjustment(adj_id):
    auth_err = admin_required()
    if auth_err:
        return auth_err
    db.delete_adjustment(adj_id)
    flash("Adjustment removed.", "success")
    return redirect(request.referrer)


# ═══════════════════════════════════════════════════════════════════════════════
# TRADES
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/league/<int:league_id>/trades")
def trades_page(league_id):
    league = db.get_league(league_id)
    if not league:
        flash("League not found.", "error")
        return redirect(url_for("leagues"))
    trades = db.get_trades(league_id)
    teams  = db.get_teams_in_league(league_id)
    # Build team→roster map for the trade form
    team_rosters = {}
    phase = league["phase"]
    for t in teams:
        team_rosters[t["id"]] = db.get_roster(t["id"], phase=phase)
    return render_template("trades.html", league=league, trades=trades,
                           teams=teams, team_rosters=team_rosters,
                           team_rosters_json=json.dumps({
                               str(k): [{"player_id": p["player_id"], "ign": p["ign"]}
                                        for p in v]
                               for k, v in team_rosters.items()
                           }))


@app.route("/league/<int:league_id>/trades/propose", methods=["POST"])
def propose_trade(league_id):
    auth_err = admin_required()
    if auth_err:
        return auth_err
    league = db.get_league(league_id)
    if not league:
        flash("League not found.", "error")
        return redirect(url_for("leagues"))
    from_team   = _int(request.form.get("from_team_id"))
    to_team     = _int(request.form.get("to_team_id"))
    from_player = request.form.get("from_player_id","")
    to_player   = request.form.get("to_player_id","")

    if not all([from_team, to_team, from_player, to_player]):
        flash("All trade fields are required.", "error")
        return redirect(url_for("trades_page", league_id=league_id))
    if from_team == to_team:
        flash("Cannot trade with yourself.", "error")
        return redirect(url_for("trades_page", league_id=league_id))

    db.propose_trade(league_id, from_team, to_team, from_player, to_player,
                     league["tournament_id"])
    flash("Trade proposed!", "success")
    return redirect(url_for("trades_page", league_id=league_id))


@app.route("/trade/<int:trade_id>/accept", methods=["POST"])
def accept_trade(trade_id):
    auth_err = admin_required()
    if auth_err:
        return auth_err
    db.resolve_trade(trade_id, "accepted")
    flash("Trade accepted and players swapped!", "success")
    return redirect(request.referrer)


@app.route("/trade/<int:trade_id>/reject", methods=["POST"])
def reject_trade(trade_id):
    auth_err = admin_required()
    if auth_err:
        return auth_err
    db.resolve_trade(trade_id, "rejected")
    flash("Trade rejected.", "success")
    return redirect(request.referrer)


@app.route("/trade/<int:trade_id>/cancel", methods=["POST"])
def cancel_trade(trade_id):
    auth_err = admin_required()
    if auth_err:
        return auth_err
    db.cancel_trade(trade_id)
    flash("Trade cancelled.", "success")
    return redirect(request.referrer)


# ═══════════════════════════════════════════════════════════════════════════════
# DRAFT
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/league/<int:league_id>/draft")
def draft(league_id):
    league  = db.get_league(league_id)
    if not league:
        flash("League not found.", "error")
        return redirect(url_for("leagues"))
    session = db.get_active_draft(league_id)
    teams   = db.get_teams_in_league(league_id)
    team_map = {t["id"]: t for t in teams}
    ruleset = db.get_ruleset(league_id)
    roles   = ["duelist","initiator","controller","sentinel","flex"]
    players = db.get_players(league["tournament_id"], sort_by="fantasy_points")

    # Mark which players are already drafted
    if session:
        order = json.loads(session["snake_order"])
        drafted_ids = set()
        for t in teams:
            roster = db.get_roster(t["id"], phase=session["phase"])
            drafted_ids.update(p["player_id"] for p in roster)
        available = [p for p in players if p["player_id"] not in drafted_ids]
        current_team_id = db.get_current_drafter(session["id"])
        current_team = team_map.get(current_team_id)
        # Build pick schedule (upcoming picks)
        pick_schedule = []
        for i, tid in enumerate(order[session["current_pick"]-1:session["current_pick"]+9], 1):
            pick_schedule.append({"pick_num": session["current_pick"] + i - 1,
                                  "team": team_map.get(tid, {})})
    else:
        available = players
        current_team = None
        current_team_id = None
        pick_schedule = []
        drafted_ids = set()

    return render_template("draft.html",
                           league=league, session=session,
                           teams=teams, team_map=team_map,
                           available=available, roles=roles,
                           ruleset=ruleset, current_team=current_team,
                           current_team_id=current_team_id,
                           pick_schedule=pick_schedule)


@app.route("/league/<int:league_id>/draft/start", methods=["POST"])
def start_draft(league_id):
    auth_err = admin_required()
    if auth_err:
        return auth_err
    phase = request.form.get("phase","swiss")
    did = db.create_draft_session(league_id, phase)
    flash(f"Snake draft started for {phase.upper()} phase!", "success")
    return redirect(url_for("draft", league_id=league_id))


@app.route("/league/<int:league_id>/draft/pick", methods=["POST"])
def draft_pick(league_id):
    auth_err = admin_required()
    if auth_err:
        return auth_err
    league  = db.get_league(league_id)
    session = db.get_active_draft(league_id)
    if not session:
        flash("No active draft.", "error")
        return redirect(url_for("draft", league_id=league_id))

    player_id = request.form.get("player_id","")
    role_slot = request.form.get("role_slot","flex")
    current_team_id = db.get_current_drafter(session["id"])
    is_dup = 1 if "is_duplicate" in request.form else 0

    if not player_id or not current_team_id:
        flash("Invalid pick.", "error")
        return redirect(url_for("draft", league_id=league_id))

    ok = db.add_player_to_roster(current_team_id, player_id,
                                 league["tournament_id"], role_slot,
                                 session["phase"], is_dup)
    if ok:
        p = db.get_player(player_id, league["tournament_id"])
        t = db.get_fantasy_team(current_team_id)
        flash(f'{t["team_name"]} picked {p["ign"] if p else player_id}!', "success")
        db.advance_draft(session["id"])
    else:
        flash("Player already drafted.", "error")

    return redirect(url_for("draft", league_id=league_id))


@app.route("/league/<int:league_id>/draft/reset", methods=["POST"])
def reset_draft(league_id):
    auth_err = admin_required()
    if auth_err:
        return auth_err
    conn = db.get_connection()
    conn.execute("UPDATE draft_sessions SET status='cancelled' WHERE league_id=? AND status='active'",
                 (league_id,))
    conn.commit()
    conn.close()
    flash("Draft reset.", "success")
    return redirect(url_for("draft", league_id=league_id))




# ═══════════════════════════════════════════════════════════════════════════════
# PLAYER POINT ADJUSTMENTS (per-player)
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/tournament/<int:tournament_id>/player/<player_id>/adjust", methods=["POST"])
def adjust_player(tournament_id, player_id):
    auth_err = admin_required()
    if auth_err:
        return auth_err
    delta  = _float(request.form.get("delta", 0))
    reason = request.form.get("reason", "").strip()
    if delta != 0:
        db.adjust_player_points(player_id, tournament_id, delta, reason)
        flash(f"Player adjustment of {delta:+.2f} pts applied.", "success")
    return redirect(request.referrer or url_for("player_detail", tournament_id=tournament_id, player_id=player_id))


@app.route("/tournament/<int:tournament_id>/player/<player_id>/adjustment/<int:adj_id>/delete", methods=["POST"])
def delete_player_adj(tournament_id, player_id, adj_id):
    auth_err = admin_required()
    if auth_err:
        return auth_err
    delta = _float(request.form.get("delta", 0))
    db.delete_player_adjustment(adj_id, player_id, tournament_id, delta)
    flash("Adjustment removed.", "success")
    return redirect(request.referrer or url_for("player_detail", tournament_id=tournament_id, player_id=player_id))


# ═══════════════════════════════════════════════════════════════════════════════
# MATCH LIST & DETAIL
# ═══════════════════════════════════════════════════════════════════════════════


@app.route("/tournament/<int:tournament_id>/scrape_rosters", methods=["POST"])
def scrape_rosters(tournament_id):
    auth_err = admin_required()
    if auth_err:
        return auth_err
    sources = db.get_event_sources(tournament_id)
    total_teams = 0
    total_players = 0
    for s in sources:
        rosters = sc.scrape_event_rosters(s["vlr_url"], tournament_id)
        total_teams   += len(rosters)
        total_players += sum(len(v) for v in rosters.values())
        time.sleep(1)
    if total_players:
        flash(f"Roster scrape done: {total_teams} teams, {total_players} players added to leaderboard.", "success")
    else:
        flash("No roster data found. The event page may not list teams yet, or selectors need adjustment.", "warning")
    return redirect(url_for("tournament_detail", tournament_id=tournament_id))


@app.route("/tournament/<int:tournament_id>/matches")
def match_list(tournament_id):
    t = db.get_tournament(tournament_id)
    if not t:
        flash("Tournament not found.", "error")
        return redirect(url_for("tournaments"))
    matches    = db.get_matches(tournament_id)
    completed  = [m for m in matches if m.get("status", "completed") != "upcoming"]
    upcoming   = [m for m in matches if m.get("status", "completed") == "upcoming"]
    return render_template("match_list.html", tournament=t,
                           completed=completed, upcoming=upcoming)


@app.route("/tournament/<int:tournament_id>/matches/<match_id>")
def match_detail(tournament_id, match_id):
    t = db.get_tournament(tournament_id)
    if not t:
        return redirect(url_for("tournaments"))

    matches = db.get_matches(tournament_id)
    match   = next((m for m in matches if str(m["match_id"]) == str(match_id)), None)
    if not match:
        flash("Match not found.", "error")
        return redirect(url_for("match_list", tournament_id=tournament_id))

    raw_stats = db.get_match_player_stats(tournament_id, match_id=match_id)

    def enrich(row):
        if row.get("stats_incomplete", 0):
            return {**row, "match_pts": None}  # None = excluded from points
        bp, rp, tp = calculate_player_points({**row, "role": row.get("role","flex")})
        return {**row, "match_pts": tp}

    enriched = [enrich(r) for r in raw_stats]

    # Smart team split: try abbr match, then partial name match, then position split
    team_a_name = (match.get("team_a") or "").lower().strip()
    team_b_name = (match.get("team_b") or "").lower().strip()

    def team_matches(row, team_name):
        abbr  = (row.get("team_abbr") or row.get("team") or "").lower().strip()
        return abbr == team_name or abbr in team_name or team_name in abbr

    team_a_players = [r for r in enriched if team_matches(r, team_a_name)]
    team_b_players = [r for r in enriched if team_matches(r, team_b_name)]

    # If overlap or empty, fall back to position split (VLR always lists team A first)
    if not team_a_players or not team_b_players or        any(p in team_b_players for p in team_a_players):
        mid = len(enriched) // 2
        team_a_players = enriched[:mid]
        team_b_players = enriched[mid:]

    # Sort each team by pts desc (incomplete rows go to bottom)
    def sort_key(p):
        return p["match_pts"] if p["match_pts"] is not None else -9999
    team_a_players.sort(key=sort_key, reverse=True)
    team_b_players.sort(key=sort_key, reverse=True)

    all_players = team_a_players + team_b_players
    complete_players = [p for p in all_players if p["match_pts"] is not None]
    player_to_watch = max(complete_players, key=lambda x: x["match_pts"]) if complete_players else None

    incomplete_players = [p for p in all_players if p.get("stats_incomplete", 0)]

    return render_template("match_detail.html", tournament=t, match=match,
                           team_a_players=team_a_players, team_b_players=team_b_players,
                           player_to_watch=player_to_watch,
                           incomplete_players=incomplete_players)



@app.route("/tournament/<int:tournament_id>/matches/<match_id>/patch_stats", methods=["POST"])
def patch_match_stats(tournament_id, match_id):
    auth_err = admin_required()
    if auth_err:
        return auth_err
    player_id = request.form.get("player_id", "")
    fields = {}
    for f in ["rating","acs","kills","deaths","assists","kast","adr","first_kills","first_deaths"]:
        val = request.form.get(f, "").strip()
        if val != "":
            try:
                fields[f] = float(val)
            except ValueError:
                pass
    if player_id and fields:
        db.patch_match_player_stats(player_id, match_id, tournament_id, fields)
        flash(f"Stats updated for player. Recalculate tournament points to apply.", "success")
    return redirect(url_for("match_detail", tournament_id=tournament_id, match_id=match_id))


@app.route("/tournament/<int:tournament_id>/scrape_upcoming", methods=["POST"])
def scrape_upcoming(tournament_id):
    auth_err = admin_required()
    if auth_err:
        return auth_err
    sources = db.get_event_sources(tournament_id)
    total = 0
    for s in sources:
        found = sc.scrape_upcoming_matches(s["vlr_url"], tournament_id)
        total += len(found)
        time.sleep(1)
    flash(f"Found {total} upcoming matches.", "success" if total else "warning")
    return redirect(url_for("match_list", tournament_id=tournament_id))


# ═══════════════════════════════════════════════════════════════════════════════
# PHASE STANDINGS & LEAGUE TABS
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/league/<int:league_id>/standings/<phase>")
def phase_standings(league_id, phase):
    league = db.get_league(league_id)
    if not league:
        return redirect(url_for("leagues"))
    if phase == "overall":
        standings = db.get_overall_standings(league_id)
    else:
        standings = db.get_phase_standings(league_id, phase)
    return jsonify(standings)


# ═══════════════════════════════════════════════════════════════════════════════
# JSON API
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/api/tournament/<int:tid>/players")
def api_players(tid):
    return jsonify(db.get_players(tid))


@app.route("/api/league/<int:lid>/standings")
def api_standings(lid):
    return jsonify(db.get_league_standings(lid))


if __name__ == "__main__":
    import os
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
