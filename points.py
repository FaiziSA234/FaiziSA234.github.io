"""
points.py — Points calculation engine
Formulae matched exactly to the Power Query scoring logic.

Base points:
    Kills*4 + Deaths*-6 + Assists*1 + ACS*(KAST/5) + FK*2 - FD*4 + ADR*(1/5)
    (KAST stored as 0-100, e.g. 72.0 for 72%)

Role adjustments (added ON TOP of base):
    Duelist:    ACS/10  +  (FK + FD) - FD*0.9          →  ACS/10 + FK + 0.1*FD
    Initiator:  Assists +  (Assists / KAD) / (KAST/100)
    Controller: Assists*0.75  +  (KAST/10) * KAD
    Sentinel:   ACS/10  +  (ADR/100) / 1.5
    Flex:       (D_total + I_total + C_total + S_total) / 4
                + Rating*10*((KAST/100) - 0.7)
                where each X_total = base + X_bonus
"""


def calculate_player_points(p: dict, role: str = None) -> tuple:
    """
    Returns (base_points, role_bonus, total_fantasy_points).
    kast expected as 0-100 float (e.g. 72.0 for 72%).
    """
    r = role or p.get("role", "flex")

    kills        = float(p.get("kills",        0))
    deaths       = float(p.get("deaths",       0)) or 1.0
    assists      = float(p.get("assists",      0))
    acs          = float(p.get("acs",          0))
    kast         = float(p.get("kast",         0))   # stored as 0-100
    adr          = float(p.get("adr",          0))
    first_kills  = float(p.get("first_kills",  0))
    first_deaths = float(p.get("first_deaths", 0))
    rating       = float(p.get("rating",       0))

    # ── Base points ──────────────────────────────────────────────────────────
    # Power Query: Kills*4 + Deaths*-6 + Assists*1 + ACS*(Kast/5) + FK*2 - FD*4 + ADR*(1/5)
    # Kast in PQ is decimal (0.72), our kast is 72 → kast/100/5 = kast/500
    kast_dec  = kast / 100 if kast > 0 else 0.0   # 72 → 0.72
    ka_d      = (kills + assists) / deaths

    kill_pts  = kills        * 4
    death_pts = deaths       * -6
    ast_pts   = assists      * 1
    acs_kast  = acs          * (kast_dec / 5)      # ACS * (KAST_dec / 5)
    fk_pts    = first_kills  * 2
    fd_pts    = first_deaths * -4
    adr_pts   = adr          * 0.2                 # ADR * (1/5)

    base = kill_pts + death_pts + ast_pts + acs_kast + fk_pts + fd_pts + adr_pts
    base = round(base, 4)

    # ── Role bonuses (each is the bonus ON TOP of base) ──────────────────────

    # Duelist: ACS/10 + (FK+FD) - FD*0.9  =  ACS/10 + FK + 0.1*FD
    def _duelist():
        return acs / 10 + (first_kills + first_deaths) - first_deaths * 0.9

    # Initiator: Assists + (Assists / KAD) / KAST_dec
    def _initiator():
        safe_kad = ka_d if ka_d > 0 else 0.001
        safe_kast = kast_dec if kast_dec > 0 else 0.01
        return assists + (assists / safe_kad) / safe_kast

    # Controller: Assists*0.75 + (KAST/10) * KAD
    # Power Query: [Kast]*10 * KAD where Kast is decimal → kast_dec*10 = kast/10
    def _controller():
        return assists * 0.75 + (kast / 10) * ka_d

    # Sentinel: ACS/10 + (ADR/100)/1.5
    def _sentinel():
        return acs / 10 + (adr / 100) / 1.5

    # ── Compute role bonus and total ─────────────────────────────────────────
    if r == "duelist":
        role_bonus = _duelist()
        total = base + role_bonus

    elif r == "initiator":
        role_bonus = _initiator()
        total = base + role_bonus

    elif r == "controller":
        role_bonus = _controller()
        total = base + role_bonus

    elif r == "sentinel":
        role_bonus = _sentinel()
        total = base + role_bonus

    elif r == "flex":
        # Power Query Flex:
        #   (Duelist_total + Initiator_total + Controller_total + Sentinel_total) / 4
        #   + Rating*10*(KAST_dec - 0.7)
        # Each X_total = base + X_bonus, so:
        #   sum / 4 = (4*base + all_bonuses) / 4 = base + all_bonuses/4
        d_bonus = _duelist()
        i_bonus = _initiator()
        c_bonus = _controller()
        s_bonus = _sentinel()
        all_bonuses = d_bonus + i_bonus + c_bonus + s_bonus
        rating_bonus = rating * 10 * (kast_dec - 0.7)
        total = base + all_bonuses / 4 + rating_bonus
        role_bonus = total - base

    else:
        # Unknown role — use base only
        role_bonus = 0.0
        total = base

    role_bonus = round(role_bonus, 4)
    total      = round(total, 2)
    return base, role_bonus, total


def apply_star_multiplier(points: float) -> float:
    """Star player gets 1.5× total points."""
    return round(points * 1.5, 2)


def points_breakdown(p: dict) -> dict:
    """Detailed breakdown dict for the player detail view."""
    kills        = float(p.get("kills",        0))
    deaths       = float(p.get("deaths",       0)) or 1.0
    assists      = float(p.get("assists",      0))
    acs          = float(p.get("acs",          0))
    kast         = float(p.get("kast",         0))
    adr          = float(p.get("adr",          0))
    fk           = float(p.get("first_kills",  0))
    fd           = float(p.get("first_deaths", 0))
    rating       = float(p.get("rating",       0))
    kast_dec     = kast / 100 if kast > 0 else 0.0
    ka_d         = (kills + assists) / deaths

    # Base components
    kill_pts  = round(kills   * 4,                2)
    death_pts = round(deaths  * -6,               2)
    ast_pts   = round(assists * 1,                2)
    acs_kast  = round(acs * (kast_dec / 5),       2)
    fk_pts    = round(fk  * 2,                    2)
    fd_pts    = round(fd  * -4,                   2)
    adr_pts   = round(adr * 0.2,                  2)
    base_calc = round(kill_pts + death_pts + ast_pts + acs_kast + fk_pts + fd_pts + adr_pts, 4)

    # Role bonus components (for display)
    safe_kad  = ka_d    if ka_d    > 0 else 0.001
    safe_kast = kast_dec if kast_dec > 0 else 0.01
    d_bonus = round(acs / 10 + (fk + fd) - fd * 0.9,               2)
    i_bonus = round(assists + (assists / safe_kad) / safe_kast,     2)
    c_bonus = round(assists * 0.75 + (kast / 10) * ka_d,            2)
    s_bonus = round(acs / 10 + (adr / 100) / 1.5,                   2)
    r_bonus = round(rating * 10 * (kast_dec - 0.7),                 2)

    return {
        # Base
        "kill_pts":        kill_pts,
        "death_pts":       death_pts,
        "assist_pts":      ast_pts,
        "acs_kast":        acs_kast,
        "fk_pts":          fk_pts,
        "fd_pts":          fd_pts,
        "adr_pts":         adr_pts,
        # Stored totals (from DB)
        "base":            p.get("base_points",    base_calc),
        "role_bonus":      p.get("role_points",    0),
        "total":           p.get("fantasy_points", 0),
        "role":            p.get("role",           "flex"),
        # Derived
        "ka_d":            round(ka_d,    3),
        "kast_dec":        round(kast_dec, 3),
        # Per-role bonuses (for tooltip / breakdown table)
        "duelist_bonus":   d_bonus,
        "initiator_bonus": i_bonus,
        "controller_bonus":c_bonus,
        "sentinel_bonus":  s_bonus,
        "rating_bonus":    r_bonus,
    }


def all_role_points(p: dict) -> dict:
    """
    Compute total fantasy points for every role for a given player stats dict.
    Returns dict with keys: duelist, initiator, controller, sentinel, flex.
    Each value is the TOTAL (base + role bonus) for that role.
    """
    results = {}
    for role in ("duelist", "initiator", "controller", "sentinel", "flex"):
        _, _, total = calculate_player_points(p, role)
        results[role] = total
    return results
