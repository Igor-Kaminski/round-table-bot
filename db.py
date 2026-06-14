import json
import re
import sqlite3
import time as time_module
import unicodedata

from core.constants import CHAMPION_ROLES, get_champions_for_role, resolve_champion_name

CHAMPION_NAME_FIXES = {
    "Ghrok": "Grohk",
}

EXCLUDED_MAP_DISPLAY_NAMES = {"Trade District Classic"}

MAP_POOL_DISPLAY_NAMES = {
    "Ascension Peak",
    "Bazaar",
    "Brightmarsh",
    "Dawnforge",
    "Fish Market",
    "Frog Isle",
    "Frozen Guard",
    "Ice Mines",
    "Jaguar Falls",
    "Serpent Beach",
    "Shattered Desert",
    "Splitstone Quarry",
    "Stone Keep",
    "Timber Mill",
    "Warder's Gate",
}

MAP_DISPLAY_ALIASES = {
    "Dawn Forge": "Dawnforge",
    "Dawnforge": "Dawnforge",
    "Fish Market": "Fish Market",
    "Frozen Guard": "Frozen Guard",
    "Serpent Beach": "Serpent Beach",
    "Serpent Beach V2": "Serpent Beach",
    "Stone Keep": "Stone Keep",
    "Stone Keep (Day)": "Stone Keep",
    "Stone Keep (Night)": "Stone Keep",
    "Trade District": "Trade District Classic",
    "Trade District Classic": "Trade District Classic",
    "Warder's Gate": "Warder's Gate",
    "Warder's Gate Custom": "Warder's Gate",
    "Warder Gate": "Warder's Gate",
    "Warder Gate Custom": "Warder's Gate",
    "Warders Gate": "Warder's Gate",
    "Warders Gate Custom": "Warder's Gate",
}


def _norm(value):
    """Return the NFC form of a string, trimmed of surrounding whitespace.

    Scoreboard text and user input can arrive with composed (NFC) or decomposed
    (NFD) accents. SQLite compares raw bytes, so matching requires normalising
    both sides to the same form before comparison.
    """
    if value is None:
        return None
    return unicodedata.normalize("NFC", str(value)).strip()


def _norm_lower(value):
    """NFC-normalised, lower-cased key for case-insensitive equality checks."""
    norm = _norm(value)
    return norm.lower() if norm is not None else ""


def display_map_name(map_name):
    normalized = _norm(map_name)
    return MAP_DISPLAY_ALIASES.get(normalized, normalized)


def related_map_names(map_name):
    display_name = display_map_name(map_name)
    names = {map_name, display_name}
    names.update(raw_name for raw_name, shown_name in MAP_DISPLAY_ALIASES.items() if shown_name == display_name)
    return sorted(name for name in names if name)


def _strip_wrapping_quotes(value):
    value = _norm(value)
    if not value:
        return value
    while len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
        value = value[1:-1].strip()
    return value


def _normalize_champion_name(champ):
    champ = _strip_wrapping_quotes(champ)
    return CHAMPION_NAME_FIXES.get(champ, champ)


def _team_score_expr(player_alias):
    return f"CASE WHEN {player_alias}.team = 1 THEN m.team1_score ELSE m.team2_score END"


def _opponent_score_expr(player_alias):
    return f"CASE WHEN {player_alias}.team = 1 THEN m.team2_score ELSE m.team1_score END"


def _win_condition(player_alias):
    return (
        f"(({player_alias}.team = 1 AND m.team1_score > m.team2_score) "
        f"OR ({player_alias}.team = 2 AND m.team2_score > m.team1_score))"
    )


def _apply_match_filters(where_conditions, params, filters=None, player_alias="ps"):
    filters = filters or {}

    if filters.get("registered_after") is not None:
        where_conditions.append("m.registered_at >= ?")
        params.append(filters["registered_after"])

    if filters.get("registered_before") is not None:
        where_conditions.append("m.registered_at < ?")
        params.append(filters["registered_before"])

    if filters.get("map"):
        map_names = related_map_names(filters["map"])
        placeholders = ", ".join("?" for _ in map_names)
        where_conditions.append(f"m.map IN ({placeholders})")
        params.extend(map_names)

    if filters.get("result") == "wins":
        where_conditions.append(_win_condition(player_alias))
    elif filters.get("result") == "losses":
        where_conditions.append(f"NOT {_win_condition(player_alias)}")

    if filters.get("team"):
        where_conditions.append(f"{player_alias}.team = ?")
        params.append(filters["team"])

    if filters.get("scoreline"):
        team_score, opponent_score = filters["scoreline"]
        where_conditions.append(
            f"{_team_score_expr(player_alias)} = ? AND {_opponent_score_expr(player_alias)} = ?"
        )
        params.extend([team_score, opponent_score])
    elif filters.get("score_category") == "close":
        where_conditions.append(
            "((m.team1_score = 4 AND m.team2_score = 3) OR (m.team1_score = 3 AND m.team2_score = 4))"
        )
    elif filters.get("score_category") == "stomp":
        where_conditions.append("ABS(m.team1_score - m.team2_score) >= 3")
    elif filters.get("score_category") == "sweep":
        where_conditions.append("ABS(m.team1_score - m.team2_score) = 4")

    for champ in filters.get("vs_champions", []):
        where_conditions.append(
            f"""
            EXISTS (
                SELECT 1 FROM player_stats enemy_champ_ps
                WHERE enemy_champ_ps.match_id = {player_alias}.match_id
                  AND enemy_champ_ps.champ = ?
                  AND enemy_champ_ps.team != {player_alias}.team
            )
            """
        )
        params.append(champ)

    for champ in filters.get("not_vs_champions", []):
        where_conditions.append(
            f"""
            NOT EXISTS (
                SELECT 1 FROM player_stats enemy_champ_ps
                WHERE enemy_champ_ps.match_id = {player_alias}.match_id
                  AND enemy_champ_ps.champ = ?
                  AND enemy_champ_ps.team != {player_alias}.team
            )
            """
        )
        params.append(champ)

    if filters.get("with_player_id"):
        where_conditions.append(
            f"""
            EXISTS (
                SELECT 1 FROM player_stats teammate_ps
                WHERE teammate_ps.match_id = {player_alias}.match_id
                  AND teammate_ps.player_id = ?
                  AND teammate_ps.team = {player_alias}.team
            )
            """
        )
        params.append(filters["with_player_id"])

    if filters.get("against_player_id"):
        where_conditions.append(
            f"""
            EXISTS (
                SELECT 1 FROM player_stats opponent_ps
                WHERE opponent_ps.match_id = {player_alias}.match_id
                  AND opponent_ps.player_id = ?
                  AND opponent_ps.team != {player_alias}.team
            )
            """
        )
        params.append(filters["against_player_id"])


def _find_player_row_by_ign(cursor, ign):
    """Find a ``players`` row whose main or alt IGN matches ``ign`` (NFC + case-insensitive).

    Returns ``(player_id, player_ign, discord_id, alt_igns_list, matched_as_main)``
    or ``None``. Uses a full scan so it transparently handles decomposed vs
    composed accent forms that may be stored in the DB.
    """
    key = _norm_lower(ign)
    if not key:
        return None
    cursor.execute("SELECT player_id, player_ign, discord_id, alt_igns FROM players;")
    for player_id, player_ign, discord_id, alt_igns_json in cursor.fetchall():
        try:
            alts = json.loads(alt_igns_json) if alt_igns_json else []
        except (json.JSONDecodeError, TypeError):
            alts = []
        if _norm_lower(player_ign) == key:
            return player_id, player_ign, discord_id, alts, True
        if any(_norm_lower(a) == key for a in alts):
            return player_id, player_ign, discord_id, alts, False
    return None


def _refresh_match_completeness(cursor):
    cursor.execute(
        """
        UPDATE matches
        SET
            player_count = (
                SELECT COUNT(*)
                FROM player_stats ps
                WHERE ps.match_id = matches.match_id
            ),
            is_complete = CASE
                WHEN (
                    SELECT COUNT(*)
                    FROM player_stats ps
                    WHERE ps.match_id = matches.match_id
                ) = 10
                AND (
                    SELECT COUNT(*)
                    FROM player_stats ps
                    WHERE ps.match_id = matches.match_id AND ps.team = 1
                ) = 5
                AND (
                    SELECT COUNT(*)
                    FROM player_stats ps
                    WHERE ps.match_id = matches.match_id AND ps.team = 2
                ) = 5
                THEN 1 ELSE 0
            END;
        """
    )


def backfill_match_registered_at(match_timestamps):
    """Backfill missing match registration timestamps from Discord history."""
    if not match_timestamps:
        return 0

    conn = sqlite3.connect("match_data.db")
    cursor = conn.cursor()
    try:
        updated = 0
        for match_id, registered_at in match_timestamps.items():
            cursor.execute(
                """
                UPDATE matches
                SET registered_at = ?
                WHERE match_id = ?
                  AND registered_at IS NULL;
                """,
                (int(registered_at), int(match_id)),
            )
            updated += cursor.rowcount
        conn.commit()
        return updated
    except sqlite3.Error as e:
        print(f"Backfill registered_at failed: {e}")
        conn.rollback()
        return 0
    finally:
        conn.close()


def create_database(match_registered_at=None):
    conn = sqlite3.connect("match_data.db")
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS matches (
            match_id INTEGER PRIMARY KEY,
            queue_num INTEGER,
            time INTEGER,
            region TEXT,
            map TEXT,
            team1_score INTEGER,
            team2_score INTEGER,
            registered_at INTEGER,
            player_count INTEGER,
            is_complete INTEGER DEFAULT 1
        );
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS players (
            player_id INTEGER PRIMARY KEY AUTOINCREMENT,
            player_ign TEXT UNIQUE,
            discord_id TEXT,
            alt_igns TEXT
        );
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS player_stats (
            player_stats_id INTEGER PRIMARY KEY AUTOINCREMENT,
            match_id INTEGER,
            player_id INTEGER,
            champ TEXT,
            talent TEXT,
            credits INTEGER,
            kills INTEGER,
            deaths INTEGER,
            assists INTEGER,
            damage INTEGER,
            taken INTEGER,
            objective_time INTEGER,
            shielding INTEGER,
            healing INTEGER,
            self_healing INTEGER,
            team INTEGER,
            FOREIGN KEY (match_id) REFERENCES matches(match_id),
            FOREIGN KEY (player_id) REFERENCES players(player_id)
        );
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS embeds (
            embed_id INTEGER PRIMARY KEY AUTOINCREMENT,
            queue_num TEXT UNIQUE,
            embed_data TEXT
        );
        """
    )

    cursor.execute("PRAGMA table_info(matches);")
    match_columns = [row[1] for row in cursor.fetchall()]
    if "registered_at" not in match_columns:
        print("Adding 'registered_at' column to matches table...")
        cursor.execute("ALTER TABLE matches ADD COLUMN registered_at INTEGER;")
    if "player_count" not in match_columns:
        print("Adding 'player_count' column to matches table...")
        cursor.execute("ALTER TABLE matches ADD COLUMN player_count INTEGER;")
    if "is_complete" not in match_columns:
        print("Adding 'is_complete' column to matches table...")
        cursor.execute("ALTER TABLE matches ADD COLUMN is_complete INTEGER DEFAULT 1;")
    _refresh_match_completeness(cursor)
    if match_registered_at:
        updated = 0
        for match_id, registered_at in match_registered_at.items():
            cursor.execute(
                """
                UPDATE matches
                SET registered_at = ?
                WHERE match_id = ?
                  AND registered_at IS NULL;
                """,
                (int(registered_at), int(match_id)),
            )
            updated += cursor.rowcount
        if updated:
            print(f"Backfilled registered_at for {updated} match rows.")
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_matches_registered_at
        ON matches(registered_at);
        """
    )

    cursor.execute("PRAGMA table_info(player_stats);")
    columns = [row[1] for row in cursor.fetchall()]
    needs_team_migration = False
    if "team" not in columns:
        print("Adding 'team' column to player_stats table...")
        cursor.execute("ALTER TABLE player_stats ADD COLUMN team INTEGER;")
        conn.commit()
        needs_team_migration = True
    else:
        cursor.execute(
            "SELECT 1 FROM player_stats WHERE team IS NULL OR team NOT IN (1, 2) LIMIT 1;"
        )
        needs_team_migration = cursor.fetchone() is not None

    _migrate_normalize_igns(cursor)
    _migrate_normalize_champions(cursor)
    conn.commit()
    conn.close()
    if needs_team_migration:
        migrate_team_column()


def _migrate_normalize_igns(cursor):
    """One-shot migration: rewrite any non-NFC ``player_ign`` / ``alt_igns`` rows.

    Safe to run repeatedly; rows already in NFC are left untouched. If two rows
    collide after normalisation (e.g. one composed and one decomposed copy of
    the same name) we merge their ``player_stats`` into the earlier ``player_id``.
    """
    try:
        cursor.execute("SELECT player_id, player_ign, alt_igns FROM players;")
        rows = cursor.fetchall()
        seen = {}  # normalized lower IGN -> player_id
        for player_id, player_ign, alt_igns_json in rows:
            normalized = _norm(player_ign)
            key = normalized.lower() if normalized else None

            try:
                alts = json.loads(alt_igns_json) if alt_igns_json else []
            except (json.JSONDecodeError, TypeError):
                alts = []
            normalized_alts, seen_alts = [], set()
            for alt in alts:
                n_alt = _norm(alt)
                if not n_alt:
                    continue
                if n_alt.lower() in seen_alts:
                    continue
                seen_alts.add(n_alt.lower())
                normalized_alts.append(n_alt)

            if key and key in seen and seen[key] != player_id:
                # Duplicate (e.g. NFC vs NFD copies) — fold this row into the first one.
                _merge_player_rows(cursor, seen[key], player_id)
                continue

            if player_ign != normalized or alts != normalized_alts:
                cursor.execute(
                    "UPDATE players SET player_ign = ?, alt_igns = ? WHERE player_id = ?;",
                    (normalized, json.dumps(normalized_alts), player_id),
                )
            if key:
                seen[key] = player_id
    except sqlite3.Error as e:
        print(f"Migration _migrate_normalize_igns failed: {e}")


def _migrate_normalize_champions(cursor):
    """Clean stored champion names so role filters see the same names as constants."""
    try:
        cursor.execute("SELECT DISTINCT champ FROM player_stats;")
        for (champ,) in cursor.fetchall():
            normalized = _normalize_champion_name(champ)
            if champ != normalized:
                cursor.execute(
                    "UPDATE player_stats SET champ = ? WHERE champ = ?;",
                    (normalized, champ),
                )
    except sqlite3.Error as e:
        print(f"Migration _migrate_normalize_champions failed: {e}")


def insert_scoreboard(scoreboard, queue_num):
    conn = sqlite3.connect("match_data.db")
    cursor = conn.cursor()
    try:
        match_id = scoreboard["match_id"]
        time = scoreboard.get("time", None)
        region = scoreboard["region"]
        map_name = scoreboard["map"]
        team1_score = scoreboard["team1_score"]
        team2_score = scoreboard["team2_score"]
        players = scoreboard["players"]
        registered_at = int(scoreboard.get("registered_at") or time_module.time())
        player_count = len(players)
        is_complete = int(
            player_count == 10
            and sum(1 for player in players if player.get("team") == 1) == 5
            and sum(1 for player in players if player.get("team") == 2) == 5
        )

        cursor.execute("SELECT 1 FROM matches WHERE match_id = ?;", (match_id,))
        if cursor.fetchone():
            print(f"Warning: Match with match_id {match_id} already exists. Skipping.")
            return

        cursor.execute(
            """
            INSERT INTO matches (
                match_id, time, region, map, team1_score, team2_score, queue_num,
                registered_at, player_count, is_complete
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (
                match_id, time, region, map_name, team1_score, team2_score, queue_num,
                registered_at, player_count, is_complete,
            ),
        )

        for player in players:
            ign = _norm(player["name"])
            champ = _normalize_champion_name(player["champ"])
            talent = player["talent"]
            credits = player["credits"]
            kills = player["kills"]
            deaths = player["deaths"]
            assists = player["assists"]
            damage = player["damage"]
            taken = player["taken"]
            objective_time = player["obj_time"]
            shielding = player["shielding"]
            healing = player["healing"]
            self_healing = player["self_healing"]
            team = player["team"]

            match = _find_player_row_by_ign(cursor, ign)
            if match:
                player_id, _pign, _did, _alts, matched_as_main = match
                if not matched_as_main:
                    print(f"alt ign matched: player_id={player_id} -> {ign}")
            else:
                cursor.execute(
                    "INSERT INTO players (player_ign, alt_igns) VALUES (?, ?);",
                    (ign, json.dumps([])),
                )
                player_id = cursor.lastrowid

            cursor.execute(
                """
                INSERT INTO player_stats (
                    match_id, player_id, champ, talent, credits, kills, deaths, assists,
                    damage, taken, objective_time, shielding, healing, self_healing, team
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    match_id, player_id, champ, talent, credits, kills, deaths, assists,
                    damage, taken, objective_time, shielding, healing, self_healing, team,
                ),
            )
        conn.commit()
        if is_complete:
            print(f"Scoreboard for match_id {match_id} inserted successfully.")
        else:
            print(f"Scoreboard for match_id {match_id} inserted as incomplete ({player_count}/10 players).")
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
        conn.rollback()
    finally:
        conn.close()


def _merge_player_rows(cursor, keep_player_id, remove_player_id):
    """Reassign all player_stats from one player row to another and delete the orphan row."""
    if keep_player_id == remove_player_id:
        return
    cursor.execute(
        "UPDATE player_stats SET player_id = ? WHERE player_id = ?;",
        (keep_player_id, remove_player_id),
    )
    cursor.execute(
        "DELETE FROM players WHERE player_id = ?;",
        (remove_player_id,),
    )


def link_ign(player_ign, discord_id, force=False):
    """Link `discord_id` to `player_ign`.

    Handles merging stats from an unclaimed player row (created automatically
    when scoreboards were ingested before the player linked) into the user's
    existing row. Use `force=True` to replace an already-linked primary IGN.
    Unicode-normalised (NFC) and case-insensitive throughout.
    """
    discord_id = str(discord_id)
    player_ign = _norm(player_ign)
    if not player_ign:
        return False
    ign_key = _norm_lower(player_ign)

    conn = sqlite3.connect("match_data.db")
    cursor = conn.cursor()
    try:
        ign_match = _find_player_row_by_ign(cursor, player_ign)
        ign_row = None
        if ign_match:
            matched_player_id, _matched_pign, matched_discord_id, _matched_alts, matched_as_main = ign_match
            if not matched_as_main and matched_discord_id and str(matched_discord_id) != discord_id:
                # IGN already registered as another user's alt — refuse.
                return False
            if matched_as_main:
                ign_row = (matched_player_id, matched_discord_id)

        cursor.execute(
            "SELECT player_id, player_ign, alt_igns FROM players WHERE discord_id = ?;",
            (discord_id,),
        )
        disc_row = cursor.fetchone()

        if ign_row and disc_row and ign_row[0] == disc_row[0]:
            return True

        if ign_row and ign_row[1] and str(ign_row[1]) != discord_id:
            return False

        if ign_row and disc_row:
            if not force:
                return False
            alts = json.loads(disc_row[2]) if disc_row[2] else []
            old_main = disc_row[1]
            _merge_player_rows(cursor, disc_row[0], ign_row[0])
            if old_main and _norm_lower(old_main) != ign_key and not any(
                _norm_lower(a) == _norm_lower(old_main) for a in alts
            ):
                alts.append(_norm(old_main))
            cursor.execute(
                "UPDATE players SET player_ign = ?, alt_igns = ? WHERE player_id = ?;",
                (player_ign, json.dumps(alts), disc_row[0]),
            )
            conn.commit()
            return True

        if ign_row and not disc_row:
            cursor.execute(
                "UPDATE players SET discord_id = ?, player_ign = ? WHERE player_id = ?;",
                (discord_id, player_ign, ign_row[0]),
            )
            conn.commit()
            return True

        if disc_row and not ign_row:
            if not force:
                return False
            alts = json.loads(disc_row[2]) if disc_row[2] else []
            old_main = disc_row[1]
            if old_main and _norm_lower(old_main) != ign_key and not any(
                _norm_lower(a) == _norm_lower(old_main) for a in alts
            ):
                alts.append(_norm(old_main))
            cursor.execute(
                "UPDATE players SET player_ign = ?, alt_igns = ? WHERE player_id = ?;",
                (player_ign, json.dumps(alts), disc_row[0]),
            )
            conn.commit()
            return True

        cursor.execute(
            "INSERT INTO players (player_ign, discord_id, alt_igns) VALUES (?, ?, ?);",
            (player_ign, discord_id, "[]"),
        )
        conn.commit()
        return True
    except sqlite3.Error as e:
        print(f"An error occurred in link_ign: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()


def update_discord_id(old_discord_id, new_discord_id):
    conn = sqlite3.connect("match_data.db")
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT player_id FROM players WHERE discord_id = ?;", (old_discord_id,)
        )
        result = cursor.fetchone()
        if result:
            player_id = result[0]
            cursor.execute(
                "UPDATE players SET discord_id = ? WHERE player_id = ?;",
                (new_discord_id, player_id),
            )
            conn.commit()
            print(
                f"Updated Discord ID for player_id {player_id} from {old_discord_id} to {new_discord_id}."
            )
        else:
            print(f"No player found with Discord ID {old_discord_id}.")
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    finally:
        conn.close()


def execute_select_query(sql_query):
    conn = sqlite3.connect("match_data.db")
    cursor = conn.cursor()
    try:
        cursor.execute(sql_query)
        return cursor.fetchall()
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        raise e
    finally:
        conn.close()


def insert_embed(queue_num, embed_data):
    conn = sqlite3.connect("match_data.db")
    cursor = conn.cursor()
    try:
        queue_num = int(re.search(r"\d+", queue_num).group())
        discord_ids = []
        for field in embed_data.get("fields", []):
            matches = re.findall(r"<@!?(\d+)>", field["value"])
            discord_ids.extend(matches)
        discord_ids = discord_ids[:10]

        cursor.execute("SELECT 1 FROM embeds WHERE queue_num = ?;", (queue_num,))
        if cursor.fetchone():
            return

        cursor.execute(
            "INSERT INTO embeds (queue_num, embed_data) VALUES (?, ?);",
            (queue_num, json.dumps(discord_ids)),
        )
        conn.commit()
    except sqlite3.Error as e:
        print(f"An error occurred while inserting embed: {e}")
    finally:
        conn.close()


def read_embeds(queue_num):
    conn = sqlite3.connect("match_data.db")
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT embed_data FROM embeds WHERE queue_num = ?;", (queue_num,))
        result = cursor.fetchone()
        return json.loads(result[0]) if result else None
    except sqlite3.Error as e:
        print(f"Database error in read_embeds: {e}")
        return None
    finally:
        conn.close()


def verify_registered_users(discord_ids):
    conn = sqlite3.connect("match_data.db")
    cursor = conn.cursor()
    try:
        placeholders = ",".join("?" for _ in discord_ids)
        query = f"SELECT discord_id FROM players WHERE discord_id IN ({placeholders});"
        cursor.execute(query, discord_ids)
        registered_ids = {row[0] for row in cursor.fetchall()}
        unregistered_ids = [
            discord_id for discord_id in discord_ids if discord_id not in registered_ids
        ]
        return registered_ids, unregistered_ids
    except sqlite3.Error as e:
        print(f"Database error in verify_registered_users: {e}")
        return set(), discord_ids
    finally:
        conn.close()


def match_exists(match_id):
    conn = sqlite3.connect("match_data.db")
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT 1 FROM matches WHERE match_id = ?", (match_id,))
        return cursor.fetchone() is not None
    finally:
        conn.close()


def queue_exists(queue_num):
    conn = sqlite3.connect("match_data.db")
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT 1 FROM matches WHERE queue_num = ?", (queue_num,))
        return cursor.fetchone() is not None
    finally:
        conn.close()


def resolve_map_name(partial_name):
    def map_key(value):
        return " ".join(re.sub(r"[^a-z0-9]+", " ", _norm_lower(value).replace("'", "")).split())

    needle = map_key(partial_name)
    if not needle:
        return None

    known_display_maps = {
        *MAP_POOL_DISPLAY_NAMES,
        *(display_name for display_name in MAP_DISPLAY_ALIASES.values() if display_name not in EXCLUDED_MAP_DISPLAY_NAMES),
    }
    for display_name in known_display_maps:
        if map_key(display_name) == needle:
            return display_name

    for raw_name, display_name in MAP_DISPLAY_ALIASES.items():
        if display_name in EXCLUDED_MAP_DISPLAY_NAMES:
            continue
        if map_key(raw_name) == needle:
            return display_name

    display_matches = [display_name for display_name in known_display_maps if needle in map_key(display_name)]
    if len(display_matches) == 1:
        return display_matches[0]

    alias_matches = [
        display_name
        for raw_name, display_name in MAP_DISPLAY_ALIASES.items()
        if display_name not in EXCLUDED_MAP_DISPLAY_NAMES and needle in map_key(raw_name)
    ]
    if len(set(alias_matches)) == 1:
        return alias_matches[0]

    display_starts = [display_name for display_name in known_display_maps if map_key(display_name).startswith(needle)]
    if len(display_starts) == 1:
        return display_starts[0]

    alias_starts = [
        display_name
        for raw_name, display_name in MAP_DISPLAY_ALIASES.items()
        if display_name not in EXCLUDED_MAP_DISPLAY_NAMES and map_key(raw_name).startswith(needle)
    ]
    if len(set(alias_starts)) == 1:
        return alias_starts[0]

    conn = sqlite3.connect("match_data.db")
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT DISTINCT map FROM matches WHERE map IS NOT NULL;")
        maps = [row[0] for row in cursor.fetchall()]
    finally:
        conn.close()

    display_to_raw = {}
    for map_name in maps:
        display_name = display_map_name(map_name)
        if display_name in EXCLUDED_MAP_DISPLAY_NAMES:
            continue
        display_to_raw.setdefault(display_name, map_name)

    for display_name, raw_name in display_to_raw.items():
        if map_key(display_name) == needle:
            return raw_name

    for map_name in maps:
        if map_key(map_name) == needle:
            return map_name

    display_matches = [raw_name for display_name, raw_name in display_to_raw.items() if needle in map_key(display_name)]
    if len(display_matches) == 1:
        return display_matches[0]

    matches = [map_name for map_name in maps if needle in map_key(map_name)]
    if len(matches) == 1:
        return matches[0]

    display_starts = [raw_name for display_name, raw_name in display_to_raw.items() if map_key(display_name).startswith(needle)]
    if len(display_starts) == 1:
        return display_starts[0]

    starts = [map_name for map_name in maps if map_key(map_name).startswith(needle)]
    if len(starts) == 1:
        return starts[0]

    return None


def get_registered_igns(ign_list):
    """Return ``(registered, not_registered)`` split of the supplied IGN list.

    An IGN counts as *registered* only if it matches the main ``player_ign`` or
    any entry in ``alt_igns`` of a row that has a linked Discord account.
    Matching is NFC-normalised and case-insensitive so that accented names like
    ``Fúriä`` resolve regardless of whether the scoreboard text uses composed
    or decomposed codepoints.
    """
    conn = sqlite3.connect("match_data.db")
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT player_ign, alt_igns FROM players WHERE discord_id IS NOT NULL;"
        )
        registered_keys = {}
        for player_ign, alt_igns_json in cursor.fetchall():
            if player_ign:
                registered_keys.setdefault(_norm_lower(player_ign), player_ign)
            if alt_igns_json:
                try:
                    alts = json.loads(alt_igns_json)
                except (json.JSONDecodeError, TypeError):
                    alts = []
                for alt in alts:
                    if alt:
                        registered_keys.setdefault(_norm_lower(alt), alt)

        registered = []
        not_registered = []
        for ign in ign_list:
            key = _norm_lower(ign)
            if key and key in registered_keys:
                registered.append(registered_keys[key])
            else:
                not_registered.append(ign)
        return registered, not_registered
    finally:
        conn.close()


def add_alt_ign(discord_id, alt_ign):
    """Add an alternate IGN to a linked player.

    If an unclaimed ``players`` row already exists for this IGN (because matches
    were ingested under it before the link), its ``player_stats`` are merged
    into the main player's row so ``!stats`` shows combined history.
    Unicode-normalised (NFC) and case-insensitive so accented names like
    ``Fúriä`` match reliably.

    Returns a dict with:
        ``success`` (bool), ``merged_matches`` (int), and ``reason`` (str).
    Possible ``reason`` values on failure: ``no_main_ign``, ``already_linked``,
    ``duplicate_alt``, ``conflict_other_user``, ``empty``, ``db_error``.
    """
    result = {"success": False, "merged_matches": 0, "reason": None}

    discord_id = str(discord_id)
    alt_ign = _norm(alt_ign)
    if not alt_ign:
        result["reason"] = "empty"
        return result

    conn = sqlite3.connect("match_data.db")
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT player_id, player_ign, alt_igns FROM players WHERE discord_id = ?;", (discord_id,)
        )
        row = cursor.fetchone()
        if not row:
            result["reason"] = "no_main_ign"
            return result
        player_id, main_ign, alt_igns_json = row
        alt_igns = json.loads(alt_igns_json) if alt_igns_json else []

        alt_key = _norm_lower(alt_ign)
        if alt_key == _norm_lower(main_ign):
            result["reason"] = "already_linked"
            return result
        if any(_norm_lower(existing) == alt_key for existing in alt_igns):
            result["reason"] = "duplicate_alt"
            return result

        match = _find_player_row_by_ign(cursor, alt_ign)
        if match and match[4]:  # matched as main IGN on some row
            other_player_id, _other_ign, other_discord_id, _other_alts, _ = match
            if other_discord_id and str(other_discord_id) != discord_id:
                result["reason"] = "conflict_other_user"
                return result
            if other_player_id != player_id:
                cursor.execute(
                    "SELECT COUNT(*) FROM player_stats WHERE player_id = ?;",
                    (other_player_id,),
                )
                result["merged_matches"] = cursor.fetchone()[0]
                _merge_player_rows(cursor, player_id, other_player_id)

        alt_igns.append(alt_ign)
        cursor.execute(
            "UPDATE players SET alt_igns = ? WHERE player_id = ?;",
            (json.dumps(alt_igns), player_id),
        )
        conn.commit()
        result["success"] = True
        return result
    except sqlite3.Error as e:
        print(f"An error occurred in add_alt_ign: {e}")
        conn.rollback()
        result["reason"] = "db_error"
        return result
    finally:
        conn.close()


def get_ign_link_info(ign):
    """Look up an IGN (NFC + case-insensitive) as a main ``player_ign``.

    Returns ``(discord_id, True, stored_ign)`` if the IGN exists as the main
    IGN of some row (``discord_id`` may be ``None`` if the row is unclaimed),
    or ``(None, False, None)`` otherwise.
    """
    conn = sqlite3.connect("match_data.db")
    cursor = conn.cursor()
    try:
        match = _find_player_row_by_ign(cursor, ign)
        if match and match[4]:  # matched as main IGN
            _pid, stored_ign, discord_id, _alts, _ = match
            return discord_id, True, stored_ign
        return None, False, None
    finally:
        conn.close()


def get_ign_for_discord_id(discord_id):
    conn = sqlite3.connect("match_data.db")
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT player_ign FROM players WHERE discord_id = ?;", (discord_id,))
        result = cursor.fetchone()
        return result[0] if result else None
    finally:
        conn.close()


def get_alt_igns(discord_id):
    conn = sqlite3.connect("match_data.db")
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT alt_igns FROM players WHERE discord_id = ?;", (discord_id,))
        result = cursor.fetchone()
        if result and result[0]:
            return json.loads(result[0])
        return []
    finally:
        conn.close()


def unlink_ign(discord_id):
    """Remove the Discord link from a player, keeping their IGN and stats intact."""
    conn = sqlite3.connect("match_data.db")
    cursor = conn.cursor()
    try:
        cursor.execute(
            "UPDATE players SET discord_id = NULL WHERE discord_id = ?;",
            (discord_id,)
        )
        conn.commit()
        return cursor.rowcount > 0
    except sqlite3.Error as e:
        print(f"An error occurred in unlink_ign: {e}")
        return False
    finally:
        conn.close()


def get_player_info(discord_id):
    """Get complete player information including main IGN and alts."""
    conn = sqlite3.connect("match_data.db")
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT player_ign, alt_igns FROM players WHERE discord_id = ?;",
            (discord_id,)
        )
        result = cursor.fetchone()
        if result:
            main_ign = result[0]
            alt_igns = json.loads(result[1]) if result[1] else []
            return {
                "main_ign": main_ign,
                "alt_igns": alt_igns,
                "all_igns": [main_ign] + alt_igns
            }
        return None
    finally:
        conn.close()


def delete_alt_ign(discord_id, alt_ign):
    alt_key = _norm_lower(alt_ign)
    if not alt_key:
        return False
    conn = sqlite3.connect("match_data.db")
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT player_id, alt_igns FROM players WHERE discord_id = ?;", (str(discord_id),)
        )
        result = cursor.fetchone()
        if not result:
            return False
        player_id, alt_igns_json = result
        alt_igns = json.loads(alt_igns_json) if alt_igns_json else []
        new_alts = [a for a in alt_igns if _norm_lower(a) != alt_key]
        if len(new_alts) == len(alt_igns):
            return False
        cursor.execute(
            "UPDATE players SET alt_igns = ? WHERE player_id = ?;",
            (json.dumps(new_alts), player_id),
        )
        conn.commit()
        return True
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
        return False
    finally:
        conn.close()


def get_player_id(discord_id):
    conn = sqlite3.connect("match_data.db")
    cursor = conn.cursor()
    cursor.execute("SELECT player_id FROM players WHERE discord_id = ?", (discord_id,))
    result = cursor.fetchone()
    conn.close()
    return result[0] if result else None


def get_player_stats(player_id, champions=None, filters=None):
    """
    Fetches aggregated player stats, now including Kill Participation and Damage Share.
    If no champion/role filter is provided, 'healing' stats are calculated
    from games played on Support champions only, while 'self_healing' uses all games.
    """
    conn = sqlite3.connect("match_data.db")
    conn.row_factory = sqlite3.Row 
    cursor = conn.cursor()
    
    try:
        where_conditions = ["ps.player_id = ?"]
        params = [player_id]

        if champions and isinstance(champions, list):
            placeholders = ', '.join('?' for _ in champions)
            where_conditions.append(f"ps.champ IN ({placeholders})")
            params.extend(champions)

        _apply_match_filters(where_conditions, params, filters, player_alias="ps")
        where_clause = " AND ".join(where_conditions)

        query = f"""
            WITH TeamTotals AS (
                SELECT
                    match_id,
                    team,
                    SUM(kills + assists) AS team_kill_participations,
                    SUM(damage) AS team_damage
                FROM player_stats
                GROUP BY match_id, team
            )
            SELECT
                COUNT(ps.match_id) AS games_played,
                SUM(ps.kills) AS total_kills,
                SUM(ps.deaths) AS total_deaths,
                SUM(ps.assists) AS total_assists,
                SUM(ps.damage) AS total_damage,
                SUM(ps.taken) AS total_taken,
                SUM(ps.objective_time) AS total_obj_time,
                SUM(ps.shielding) AS total_shielding,
                SUM(ps.healing) AS total_healing,
                SUM(ps.self_healing) AS total_self_healing,
                SUM(ps.credits) AS total_credits,
                SUM(COALESCE(ott.team_damage, 0)) AS total_enemy_damage,
                SUM(m.time) AS total_time_in_minutes,
                SUM(CASE WHEN (ps.team = 1 AND m.team1_score > m.team2_score) OR (ps.team = 2 AND m.team2_score > m.team1_score) THEN 1 ELSE 0 END) AS total_wins,
                
                AVG(CASE WHEN tt.team_kill_participations > 0 THEN CAST(ps.kills + ps.assists AS REAL) * 100.0 / tt.team_kill_participations ELSE 0 END) AS avg_kill_share,
                AVG(CASE WHEN tt.team_damage > 0 THEN CAST(ps.damage AS REAL) * 100.0 / tt.team_damage ELSE 0 END) AS avg_damage_share

            FROM player_stats ps
            JOIN matches m ON ps.match_id = m.match_id
            JOIN TeamTotals tt ON ps.match_id = tt.match_id AND ps.team = tt.team
            LEFT JOIN TeamTotals ott ON ps.match_id = ott.match_id AND ps.team != ott.team
            WHERE {where_clause}
        """

        cursor.execute(query, params)
        data = cursor.fetchone()

        if not data or data["games_played"] == 0:
            return None

        games_played = data["games_played"]
        total_wins = data["total_wins"]
        total_time_in_minutes = data["total_time_in_minutes"]

        total_enemy_damage = data["total_enemy_damage"] or 0

        stats_dict = {
            "games": games_played,
            "wins": total_wins,
            "losses": games_played - total_wins,
            "raw_k": data["total_kills"], "raw_d": data["total_deaths"], "raw_a": data["total_assists"],
            "winrate": round((total_wins / games_played) * 100, 2) if games_played > 0 else 0,
            "kda": f"{data['total_kills']}/{data['total_deaths']}/{data['total_assists']}",
            "kda_ratio": round((data['total_kills'] + data['total_assists']) / max(1, data['total_deaths']), 2),
            "kills_pm": round(data["total_kills"] / max(1, total_time_in_minutes), 2),
            "deaths_pm": round(data["total_deaths"] / max(1, total_time_in_minutes), 2),
            "damage_dealt_pm": round(data["total_damage"] / max(1, total_time_in_minutes), 2),
            "damage_taken_pm": round(data["total_taken"] / max(1, total_time_in_minutes), 2),
            "self_healing_pm": round(data["total_self_healing"] / max(1, total_time_in_minutes), 2),
            "credits_pm": round(data["total_credits"] / max(1, total_time_in_minutes), 2),
            "obj_time": round(data["total_obj_time"] / games_played, 2) if games_played > 0 else 0,
            "avg_kills": round(data["total_kills"] / games_played, 2) if games_played > 0 else 0,
            "avg_deaths": round(data["total_deaths"] / games_played, 2) if games_played > 0 else 0,
            "avg_damage_dealt": round(data["total_damage"] / games_played) if games_played > 0 else 0,
            "avg_damage_taken": round(data["total_taken"] / games_played) if games_played > 0 else 0,
            "avg_self_healing": round(data["total_self_healing"] / games_played) if games_played > 0 else 0,
            "avg_shielding": round(data["total_shielding"] / games_played) if games_played > 0 else 0,
            "avg_credits": round(data["total_credits"] / games_played) if games_played > 0 else 0,
            "damage_delta": round((data["total_damage"] - data["total_taken"]) / games_played) if games_played > 0 else 0,
            "kill_share": data["avg_kill_share"] or 0,
            "damage_share": data["avg_damage_share"] or 0,
            "damage_healed_pct": round((data["total_healing"] / total_enemy_damage) * 100, 2) if total_enemy_damage > 0 else 0,
        }

        total_healing = data["total_healing"]
        support_time = total_time_in_minutes
        support_games = games_played

        if not champions:
            support_champs = [champ for champ, role in CHAMPION_ROLES.items() if role == "Support"]
            placeholders = ', '.join('?' for _ in support_champs)
            healing_conditions = [f"ps.player_id = ?", f"ps.champ IN ({placeholders})"]
            healing_params = [player_id] + support_champs
            _apply_match_filters(healing_conditions, healing_params, filters, player_alias="ps")
            healing_where_clause = " AND ".join(healing_conditions)
            cursor.execute(f"""
                WITH TeamTotals AS (
                    SELECT match_id, team, SUM(damage) AS team_damage
                    FROM player_stats
                    GROUP BY match_id, team
                )
                SELECT SUM(ps.healing), SUM(m.time), COUNT(ps.match_id), SUM(COALESCE(ott.team_damage, 0))
                FROM player_stats ps
                JOIN matches m ON ps.match_id = m.match_id
                LEFT JOIN TeamTotals ott ON ps.match_id = ott.match_id AND ps.team != ott.team
                WHERE {healing_where_clause}
            """, healing_params)
            healing_row = cursor.fetchone()
            total_healing = healing_row[0] or 0
            support_time = healing_row[1] or 0
            support_games = healing_row[2] or 0
            support_enemy_damage = healing_row[3] or 0
            stats_dict["damage_healed_pct"] = round((total_healing / support_enemy_damage) * 100, 2) if support_enemy_damage > 0 else 0
            
        stats_dict["healing_pm"] = round(total_healing / max(1, support_time), 2)
        stats_dict["avg_healing"] = round(total_healing / max(1, support_games))
        stats_dict["damage_healing_pm"] = round(stats_dict["damage_dealt_pm"] + stats_dict["healing_pm"], 2)

        return stats_dict
    finally:
        conn.close()


def get_top_champs(player_id):
    conn = sqlite3.connect("match_data.db")
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT
            champ,
            COUNT(*),
            SUM(CASE WHEN (ps.team = 1 AND m.team1_score > m.team2_score) OR (ps.team = 2 AND m.team2_score > m.team1_score) THEN 1 ELSE 0 END) as wins,
            SUM(kills), SUM(deaths), SUM(assists),
            SUM(damage), SUM(objective_time), SUM(shielding), SUM(healing), SUM(m.time)
        FROM player_stats ps
        JOIN matches m ON ps.match_id = m.match_id
            WHERE player_id = ?
        GROUP BY champ
        ORDER BY COUNT(*) DESC
        LIMIT 5
        """,
        (player_id,),
    )
    rows = cursor.fetchall()
    champs = []
    for row in rows:
        (
            champ, games, wins, kills, deaths, assists,
            damage, obj_time, shielding, healing, total_time_in_minutes
        ) = row
        
        if total_time_in_minutes == 0:
            total_time_in_minutes = 1

        champ_stats = {
            "champ": champ, "games": games,
            "winrate": round(100 * wins / games, 1) if games else 0,
            "kda": f"{round(kills/games, 1)}/{round(deaths/games, 1)}/{round(assists/games, 1)}",
            "damage": round(damage / total_time_in_minutes, 2),
            "objective_time": round(obj_time, 2),
            "shielding": round(shielding / total_time_in_minutes, 2),
            "healing": round(healing / total_time_in_minutes, 2),
        }
        champs.append(champ_stats)
    conn.close()
    return champs

def get_winrate_with_against(pid1, pid2):
    conn = sqlite3.connect("match_data.db")
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT m.team1_score, m.team2_score, ps1.team
        FROM matches m
        JOIN player_stats ps1 ON m.match_id = ps1.match_id
        JOIN player_stats ps2 ON m.match_id = ps2.match_id
        WHERE ps1.player_id = ?
          AND ps2.player_id = ?
          AND ps1.team = ps2.team
        """,
        (pid1, pid2),
    )
    rows = cursor.fetchall()
    with_games = len(rows)
    with_wins = sum(1 for t1, t2, team in rows if (team == 1 and t1 > t2) or (team == 2 and t2 > t1))
    with_winrate = round(100 * with_wins / with_games, 1) if with_games else 0

    cursor.execute(
        """
        SELECT m.team1_score, m.team2_score, ps1.team
        FROM matches m
        JOIN player_stats ps1 ON m.match_id = ps1.match_id
        JOIN player_stats ps2 ON m.match_id = ps2.match_id
        WHERE ps1.player_id = ?
          AND ps2.player_id = ?
          AND ps1.team != ps2.team
        """,
        (pid1, pid2),
    )
    rows = cursor.fetchall()
    against_games = len(rows)
    against_wins = sum(1 for t1, t2, team in rows if (team == 1 and t1 > t2) or (team == 2 and t2 > t1))
    against_winrate = round(100 * against_wins / against_games, 1) if against_games else 0
    conn.close()
    return with_winrate, with_games, against_winrate, against_games

def compare_by_player_ids(pid1, pid2):
    if not pid1 or not pid2:
        return None

    stats1 = get_player_stats(pid1)
    stats2 = get_player_stats(pid2)
    if not stats1 or not stats2:
        return None
    champs1 = get_top_champs(pid1)
    champs2 = get_top_champs(pid2)
    with_winrate, with_games, against_winrate, against_games = get_winrate_with_against(pid1, pid2)

    return {
        "player1": stats1, "player2": stats2, "top_champs1": champs1, "top_champs2": champs2,
        "with_winrate": with_winrate, "with_games": with_games,
        "against_winrate": against_winrate, "against_games": against_games,
    }


def compare_players(discord_id1, discord_id2):
    pid1 = get_player_id(discord_id1)
    pid2 = get_player_id(discord_id2)
    return compare_by_player_ids(pid1, pid2)

def get_teammate_records(player_id, limit=10, show_bottom=False, min_games=1, champion=None, role=None, filters=None):
    return get_player_relationship_records(
        player_id, relation="with", limit=limit, show_bottom=show_bottom,
        min_games=min_games, champion=champion, role=role, filters=filters,
    )

def get_enemy_records(player_id, limit=10, show_bottom=False, min_games=1, champion=None, role=None, filters=None):
    return get_player_relationship_records(
        player_id, relation="against", limit=limit, show_bottom=show_bottom,
        min_games=min_games, champion=champion, role=role, filters=filters,
    )

def get_player_relationship_records(player_id, relation="with", limit=10, show_bottom=False, min_games=1, champion=None, role=None, filters=None):
    conn = sqlite3.connect("match_data.db")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    try:
        team_operator = "=" if relation == "with" else "!="
        where_conditions = ["ps.player_id = ?", "other.player_id != ps.player_id"]
        params = [player_id]

        if champion:
            champion = resolve_champion_name(champion) or champion
            where_conditions.append("ps.champ LIKE ?")
            params.append(f"%{champion}%")
        elif role:
            champions_in_role = get_champions_for_role(role)
            if not champions_in_role:
                return []
            placeholders = ", ".join("?" for _ in champions_in_role)
            where_conditions.append(f"ps.champ IN ({placeholders})")
            params.extend(champions_in_role)

        _apply_match_filters(where_conditions, params, filters, player_alias="ps")
        where_clause = " AND ".join(where_conditions)
        order = "ASC" if show_bottom else "DESC"
        final_params = params + [min_games, limit]

        cursor.execute(f"""
            SELECT
                teammate.player_id,
                teammate.player_ign,
                teammate.discord_id,
                COUNT(ps.match_id) AS games,
                SUM(CASE
                    WHEN (ps.team = 1 AND m.team1_score > m.team2_score)
                      OR (ps.team = 2 AND m.team2_score > m.team1_score)
                    THEN 1 ELSE 0
                END) AS wins
            FROM player_stats ps
            JOIN matches m ON ps.match_id = m.match_id
            JOIN player_stats other
              ON ps.match_id = other.match_id
             AND ps.team {team_operator} other.team
             AND ps.player_id != other.player_id
            JOIN players teammate ON other.player_id = teammate.player_id
            WHERE {where_clause}
            GROUP BY teammate.player_id, teammate.player_ign, teammate.discord_id
            HAVING games >= ?
            ORDER BY (wins * 100.0 / games) {order}, games DESC, teammate.player_ign COLLATE NOCASE ASC
            LIMIT ?;
        """, final_params)

        rows = []
        for row in cursor.fetchall():
            games = row["games"] or 0
            wins = row["wins"] or 0
            rows.append({
                "player_id": row["player_id"],
                "player_ign": row["player_ign"],
                "discord_id": row["discord_id"],
                "games": games,
                "wins": wins,
                "losses": games - wins,
                "winrate": round(wins * 100.0 / games, 2) if games else 0,
            })
        return rows
    finally:
        conn.close()

def get_related_champion_records(player_id, relation="with", limit=10, show_bottom=False, min_games=1, champion=None, role=None, filters=None):
    conn = sqlite3.connect("match_data.db")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    try:
        team_operator = "=" if relation == "with" else "!="
        where_conditions = ["ps.player_id = ?"]
        params = [player_id]

        if champion:
            champion = resolve_champion_name(champion) or champion
            where_conditions.append("other.champ = ?")
            params.append(champion)
        elif role:
            champions_in_role = get_champions_for_role(role)
            if not champions_in_role:
                return []
            placeholders = ", ".join("?" for _ in champions_in_role)
            where_conditions.append(f"other.champ IN ({placeholders})")
            params.extend(champions_in_role)

        _apply_match_filters(where_conditions, params, filters, player_alias="ps")
        where_clause = " AND ".join(where_conditions)
        order = "ASC" if show_bottom else "DESC"
        final_params = params + [min_games, limit]

        cursor.execute(f"""
            SELECT
                other.champ,
                COUNT(ps.match_id) AS games,
                SUM(CASE
                    WHEN (ps.team = 1 AND m.team1_score > m.team2_score)
                      OR (ps.team = 2 AND m.team2_score > m.team1_score)
                    THEN 1 ELSE 0
                END) AS wins
            FROM player_stats ps
            JOIN matches m ON ps.match_id = m.match_id
            JOIN player_stats other
              ON ps.match_id = other.match_id
             AND ps.team {team_operator} other.team
             AND ps.player_id != other.player_id
            WHERE {where_clause}
            GROUP BY other.champ
            HAVING games >= ?
            ORDER BY (wins * 100.0 / games) {order}, games DESC, other.champ COLLATE NOCASE ASC
            LIMIT ?;
        """, final_params)

        rows = []
        for row in cursor.fetchall():
            games = row["games"] or 0
            wins = row["wins"] or 0
            rows.append({
                "champ": row["champ"],
                "games": games,
                "wins": wins,
                "losses": games - wins,
                "winrate": round(wins * 100.0 / games, 2) if games else 0,
            })
        return rows
    finally:
        conn.close()

def get_match_history(player_id, limit: int = 30, filters=None):
    conn = sqlite3.connect("match_data.db")
    cursor = conn.cursor()
    try:
        where_conditions = ["ps.player_id = ?"]
        params = [player_id]
        _apply_match_filters(where_conditions, params, filters, player_alias="ps")
        where_clause = " AND ".join(where_conditions)
        params.append(limit)

        cursor.execute("""
            SELECT
                m.map, ps.champ, ps.kills, ps.deaths, ps.assists,
                CASE
                    WHEN (ps.team = 1 AND m.team1_score > m.team2_score) OR (ps.team = 2 AND m.team2_score > m.team1_score) THEN 'W'
                    ELSE 'L'
                END as result,
                m.match_id, m.time
            FROM player_stats ps
            JOIN matches m ON ps.match_id = m.match_id
            WHERE {where_clause}
            ORDER BY m.registered_at DESC, ps.player_stats_id DESC
            LIMIT ?;
        """.format(where_clause=where_clause), params)
        return cursor.fetchall()
    finally:
        conn.close()


def _get_all_map_names(cursor):
    cursor.execute("SELECT DISTINCT map FROM matches WHERE map IS NOT NULL AND TRIM(map) != '' ORDER BY map ASC;")
    return sorted(
        MAP_POOL_DISPLAY_NAMES
        | {
            _display_map_name(row[0])
            for row in cursor.fetchall()
            if _display_map_name(row[0]) not in EXCLUDED_MAP_DISPLAY_NAMES
        },
        key=str.lower,
    )


def _display_map_name(map_name):
    return display_map_name(map_name)


def _map_winrate_rows(cursor, query, params, min_games=1, include_all_maps=True, sort_by_winrate=False):
    all_maps = _get_all_map_names(cursor) if include_all_maps else []
    cursor.execute(query, params + [min_games])
    rows_by_map = {}
    for row in cursor.fetchall():
        map_name = _display_map_name(row["map"])
        if map_name in EXCLUDED_MAP_DISPLAY_NAMES:
            continue
        wins = row["wins"] or 0
        games = row["games"] or 0
        existing = rows_by_map.setdefault(map_name, {"map": map_name, "games": 0, "wins": 0, "losses": 0, "winrate": 0})
        existing["games"] += games
        existing["wins"] += wins
        existing["losses"] = existing["games"] - existing["wins"]
        existing["winrate"] = round((existing["wins"] / existing["games"]) * 100, 2) if existing["games"] else 0

    if include_all_maps:
        for map_name in all_maps:
            rows_by_map.setdefault(
                map_name,
                {"map": map_name, "games": 0, "wins": 0, "losses": 0, "winrate": 0},
            )

    rows = list(rows_by_map.values())
    if sort_by_winrate:
        rows.sort(key=lambda row: (-row["winrate"], -row["games"], row["map"].lower()))
    else:
        rows.sort(key=lambda row: row["map"].lower())
    return rows


def get_player_map_winrates(player_id, champions=None, filters=None, min_games=1, include_all_maps=True, sort_by_winrate=False):
    conn = sqlite3.connect("match_data.db")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    try:
        where_conditions = ["ps.player_id = ?"]
        params = [player_id]

        if champions:
            placeholders = ', '.join('?' for _ in champions)
            where_conditions.append(f"ps.champ IN ({placeholders})")
            params.extend(champions)

        _apply_match_filters(where_conditions, params, filters, player_alias="ps")
        where_clause = " AND ".join(where_conditions)

        query = f"""
            SELECT
                m.map,
                COUNT(ps.match_id) AS games,
                SUM(
                    CASE
                        WHEN (ps.team = 1 AND m.team1_score > m.team2_score)
                          OR (ps.team = 2 AND m.team2_score > m.team1_score)
                        THEN 1 ELSE 0
                    END
                ) AS wins
            FROM player_stats ps
            JOIN matches m ON ps.match_id = m.match_id
            WHERE {where_clause}
            GROUP BY m.map
            HAVING games >= ?
        """
        return _map_winrate_rows(cursor, query, params, min_games, include_all_maps, sort_by_winrate)
    finally:
        conn.close()


def get_champion_map_winrates(champion, filters=None, min_games=1, include_all_maps=True, sort_by_winrate=False):
    champion = resolve_champion_name(champion) or champion
    conn = sqlite3.connect("match_data.db")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    try:
        where_conditions = ["ps.champ = ?"]
        params = [champion]
        _apply_match_filters(where_conditions, params, filters, player_alias="ps")
        where_clause = " AND ".join(where_conditions)

        query = f"""
            SELECT
                m.map,
                COUNT(ps.match_id) AS games,
                SUM(
                    CASE
                        WHEN (ps.team = 1 AND m.team1_score > m.team2_score)
                          OR (ps.team = 2 AND m.team2_score > m.team1_score)
                        THEN 1 ELSE 0
                    END
                ) AS wins
            FROM player_stats ps
            JOIN matches m ON ps.match_id = m.match_id
            WHERE {where_clause}
            GROUP BY m.map
            HAVING games >= ?
        """
        return _map_winrate_rows(cursor, query, params, min_games, include_all_maps, sort_by_winrate)
    finally:
        conn.close()


def get_champion_overall_stats(champion, filters=None):
    champion = resolve_champion_name(champion) or champion
    conn = sqlite3.connect("match_data.db")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    try:
        where_conditions = ["ms.champ = ?", "m.time > 0"]
        params = [champion]
        _apply_match_filters(where_conditions, params, filters, player_alias="ms")
        where_clause = " AND ".join(where_conditions)

        query = f"""
            WITH TeamTotals AS (
                SELECT
                    match_id,
                    team,
                    SUM(kills + assists) AS team_kill_participations,
                    SUM(damage) AS team_damage
                FROM player_stats
                GROUP BY match_id, team
            ),
            MatchShares AS (
                SELECT
                    ps.*,
                    tt.team_kill_participations,
                    tt.team_damage,
                    COALESCE(ott.team_damage, 0) AS enemy_team_damage,
                    CASE WHEN tt.team_kill_participations > 0 THEN CAST(ps.kills + ps.assists AS REAL) * 100.0 / tt.team_kill_participations ELSE 0 END AS kill_share,
                    CASE WHEN tt.team_damage > 0 THEN CAST(ps.damage AS REAL) * 100.0 / tt.team_damage ELSE 0 END AS damage_share
                FROM player_stats ps
                JOIN TeamTotals tt ON ps.match_id = tt.match_id AND ps.team = tt.team
                LEFT JOIN TeamTotals ott ON ps.match_id = ott.match_id AND ps.team != ott.team
            )
            SELECT
                COUNT(ms.match_id) AS games,
                SUM(CASE WHEN (ms.team = 1 AND m.team1_score > m.team2_score) OR (ms.team = 2 AND m.team2_score > m.team1_score) THEN 1 ELSE 0 END) AS wins,
                SUM(ms.kills) AS kills,
                SUM(ms.deaths) AS deaths,
                SUM(ms.assists) AS assists,
                SUM(ms.damage) AS damage,
                SUM(ms.taken) AS taken,
                SUM(ms.healing) AS healing,
                SUM(ms.self_healing) AS self_healing,
                SUM(ms.shielding) AS shielding,
                SUM(ms.credits) AS credits,
                SUM(ms.objective_time) AS objective_time,
                SUM(ms.enemy_team_damage) AS enemy_damage,
                SUM(m.time) AS minutes,
                AVG(ms.kill_share) AS kp,
                AVG(ms.damage_share) AS dmg_share
            FROM MatchShares ms
            JOIN matches m ON ms.match_id = m.match_id
            WHERE {where_clause}
        """
        cursor.execute(query, params)
        row = cursor.fetchone()
        if not row or not row["games"]:
            return None

        games = row["games"]
        minutes = row["minutes"] or 1
        wins = row["wins"] or 0
        damage = row["damage"] or 0
        healing = row["healing"] or 0
        enemy_damage = row["enemy_damage"] or 0
        return {
            "champ": champion,
            "games": games,
            "wins": wins,
            "losses": games - wins,
            "winrate": round(wins * 100.0 / games, 2),
            "kda": round(((row["kills"] or 0) + (row["assists"] or 0)) / max(1, row["deaths"] or 0), 2),
            "raw_k": row["kills"] or 0,
            "raw_d": row["deaths"] or 0,
            "raw_a": row["assists"] or 0,
            "dpm": round(damage / minutes, 2),
            "taken_pm": round((row["taken"] or 0) / minutes, 2),
            "hpm": round(healing / minutes, 2),
            "self_heal_pm": round((row["self_healing"] or 0) / minutes, 2),
            "credits_pm": round((row["credits"] or 0) / minutes, 2),
            "avg_kills": round((row["kills"] or 0) / games, 2),
            "avg_deaths": round((row["deaths"] or 0) / games, 2),
            "avg_damage": round(damage / games),
            "avg_taken": round((row["taken"] or 0) / games),
            "avg_healing": round(healing / games),
            "avg_self_healing": round((row["self_healing"] or 0) / games),
            "shield_avg": round((row["shielding"] or 0) / games),
            "avg_credits": round((row["credits"] or 0) / games),
            "obj_avg": round((row["objective_time"] or 0) / games, 2),
            "damage_healed_pct": round(healing * 100.0 / enemy_damage, 2) if enemy_damage > 0 else 0,
            "kp": round(row["kp"] or 0, 2),
            "dmg_share": round(row["dmg_share"] or 0, 2),
        }
    finally:
        conn.close()


def get_leaderboard(stat_key, limit, show_bottom=False, champion=None, role=None, min_games=1, filters=None):
    stat_expressions = {
        "winrate": "SUM(CASE WHEN (ps.team = 1 AND m.team1_score > m.team2_score) OR (ps.team = 2 AND m.team2_score > m.team1_score) THEN 1 ELSE 0 END) * 100.0 / COUNT(ps.match_id)",
        "kda": "CAST(SUM(ps.kills) + SUM(ps.assists) AS REAL) / MAX(1, SUM(ps.deaths))",
        "kills_pm": "SUM(CAST(ps.kills AS REAL)) / SUM(m.time)",
        "deaths_pm": "SUM(CAST(ps.deaths AS REAL)) / SUM(m.time)",
        "damage_dealt_pm": "SUM(CAST(ps.damage AS REAL)) / SUM(m.time)",
        "damage_taken_pm": "SUM(CAST(ps.taken AS REAL)) / SUM(m.time)",
        "healing_pm": "SUM(CAST(ps.healing AS REAL)) / SUM(m.time)",
        "damage_healing_pm": "SUM(CAST(ps.damage + ps.healing AS REAL)) / SUM(m.time)",
        "self_healing_pm": "SUM(CAST(ps.self_healing AS REAL)) / SUM(m.time)",
        "credits_pm": "SUM(CAST(ps.credits AS REAL)) / SUM(m.time)",
        "avg_kills": "AVG(ps.kills)",
        "avg_deaths": "AVG(ps.deaths)",
        "avg_damage_dealt": "AVG(ps.damage)",
        "avg_damage_taken": "AVG(ps.taken)",
        "damage_delta": "AVG(ps.damage - ps.taken)",
        "avg_healing": "AVG(ps.healing)",
        "avg_self_healing": "AVG(ps.self_healing)",
        "avg_shielding": "AVG(ps.shielding)",
        "avg_credits": "AVG(ps.credits)",
        "obj_time": "AVG(ps.objective_time)",
        "kp": "AVG(ps.kill_share)",
        "dmg_share": "AVG(ps.damage_share)",
    }

    if stat_key not in stat_expressions:
        return None

    order = "ASC" if show_bottom else "DESC"
    params = []
    where_conditions = ["p.discord_id IS NOT NULL", "m.time > 0"]
    healing_only_stats = ["healing_pm", "avg_healing", "damage_healing_pm"]

    if champion:
        champion = resolve_champion_name(champion) or champion
        where_conditions.append("ps.champ LIKE ?")
        params.append(f"%{champion}%")
    elif role:
        champions_in_role = get_champions_for_role(role)
        if not champions_in_role: return None
        placeholders = ', '.join('?' for _ in champions_in_role)
        where_conditions.append(f"ps.champ IN ({placeholders})")
        params.extend(champions_in_role)
    elif stat_key in healing_only_stats:
        champions_in_role = get_champions_for_role("Support")
        placeholders = ', '.join('?' for _ in champions_in_role)
        where_conditions.append(f"ps.champ IN ({placeholders})")
        params.extend(champions_in_role)

    _apply_match_filters(where_conditions, params, filters, player_alias="ps")

    where_clause = " AND ".join(where_conditions)
    final_params = params + [min_games, limit]

    query = f"""
        WITH TeamTotals AS (
            SELECT
                match_id,
                team,
                SUM(kills + assists) as team_kill_participations,
                SUM(damage) as team_damage
            FROM player_stats GROUP BY match_id, team
        ),
        MatchShares AS (
            SELECT 
                ps.*,
                CASE WHEN tt.team_kill_participations > 0 THEN CAST(ps.kills + ps.assists AS REAL) * 100.0 / tt.team_kill_participations ELSE 0 END as kill_share,
                CASE WHEN tt.team_damage > 0 THEN CAST(ps.damage AS REAL) * 100.0 / tt.team_damage ELSE 0 END as damage_share
            FROM player_stats ps
            JOIN TeamTotals tt ON ps.match_id = tt.match_id AND ps.team = tt.team
        ),
        PlayerAggregates AS (
            SELECT
                p.discord_id, p.player_ign,
                COUNT(ps.match_id) AS games_played,
                SUM(CASE WHEN (ps.team = 1 AND m.team1_score > m.team2_score) OR (ps.team = 2 AND m.team2_score > m.team1_score) THEN 1 ELSE 0 END) as wins,
                SUM(ps.kills) as total_k, SUM(ps.deaths) as total_d, SUM(ps.assists) as total_a,
                ({stat_expressions[stat_key]}) AS value
            FROM MatchShares ps
            JOIN players p ON ps.player_id = p.player_id
            JOIN matches m ON ps.match_id = m.match_id
            WHERE {where_clause}
            GROUP BY p.discord_id
            HAVING games_played >= ?
        )
        SELECT
            pa.discord_id, pa.player_ign, pa.value, pa.wins,
            (pa.games_played - pa.wins) AS losses,
            pa.total_k as k, pa.total_d as d, pa.total_a as a,
            (SELECT COUNT(*) FROM PlayerAggregates) as total_players
        FROM PlayerAggregates pa
        ORDER BY pa.value {order}, pa.games_played DESC
        LIMIT ?;
    """

    conn = sqlite3.connect("match_data.db")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    try:
        cursor.execute(query, tuple(final_params))
        return [dict(row) for row in cursor.fetchall()]
    except sqlite3.Error as e:
        print(f"Database error in get_leaderboard: {e}")
        return None
    finally:
        conn.close()

def get_old_stats(player_id):
    conn = sqlite3.connect("match_data.db")
    cursor = conn.cursor()
    cursor.execute(
        "SELECT kills, deaths, assists, damage, objective_time, shielding, healing, match_id FROM player_stats WHERE player_id = ?",
        (player_id,),
    )
    rows = cursor.fetchall()
    if not rows:
        conn.close()
        return None
    match_ids = [row[7] for row in rows]
    total_time = 0
    if match_ids:
        placeholders = ','.join('?' for _ in match_ids)
        cursor.execute(f"SELECT SUM(time) FROM matches WHERE match_id IN ({placeholders})", match_ids)
        total_time_result = cursor.fetchone()
        if total_time_result and total_time_result[0] is not None:
            total_time = total_time_result[0]
    conn.close()
    if total_time == 0: total_time = 1
    agg_stats = [sum(col) for col in zip(*[row[:7] for row in rows])]
    norm_stats = [round(val / total_time, 2) for val in agg_stats]
    return {
        "kills": norm_stats[0], "deaths": norm_stats[1], "assists": norm_stats[2], "damage": norm_stats[3],
        "objective_time": norm_stats[4], "shielding": norm_stats[5], "healing": norm_stats[6], "games": len(rows),
    }

def migrate_team_column():
    conn = sqlite3.connect("match_data.db")
    cursor = conn.cursor()
    try:
        cursor.execute("PRAGMA table_info(player_stats);")
        columns = [row[1] for row in cursor.fetchall()]
        if "team" not in columns: return

        cursor.execute(
            """
            SELECT DISTINCT match_id
            FROM player_stats
            WHERE team IS NULL OR team NOT IN (1, 2);
            """
        )
        match_ids = [row[0] for row in cursor.fetchall()]
        for match_id in match_ids:
            cursor.execute(
                """
                SELECT
                    player_stats_id,
                    CASE WHEN team IS NULL OR team NOT IN (1, 2) THEN 1 ELSE 0 END as needs_team
                FROM player_stats
                WHERE match_id = ?
                ORDER BY player_stats_id ASC;
                """,
                (match_id,),
            )
            rows = cursor.fetchall()
            for idx, (ps_id, needs_team) in enumerate(rows):
                if not needs_team:
                    continue
                team = 1 if idx < 5 else 2
                cursor.execute(
                    "UPDATE player_stats SET team = ? WHERE player_stats_id = ?;",
                    (team, ps_id),
                )
        _refresh_match_completeness(cursor)
        conn.commit()
        print("Migration: Populated 'team' column in player_stats.")
    except Exception as e:
        print(f"Migration error: {e}")
    finally:
        conn.close()


def get_player_by_ign(ign):
    """Look up a player (linked or not) by main IGN or alt IGN.

    Matches are Unicode-normalised (NFC) and case-insensitive. Returns a dict
    with ``player_id``, ``player_ign``, and ``discord_id`` (may be ``None`` for
    unclaimed rows created by scoreboard ingestion), or ``None``.
    """
    conn = sqlite3.connect("match_data.db")
    cursor = conn.cursor()
    try:
        match = _find_player_row_by_ign(cursor, ign)
        if match:
            player_id, player_ign, discord_id, _alts, _ = match
            return {"player_id": player_id, "player_ign": player_ign, "discord_id": discord_id}
        return None
    finally:
        conn.close()


def get_discord_id_for_ign(ign):
    """Look up a linked Discord ID by main IGN or any stored alt IGN (NFC + case-insensitive)."""
    conn = sqlite3.connect("match_data.db")
    cursor = conn.cursor()
    try:
        match = _find_player_row_by_ign(cursor, ign)
        if match and match[2]:  # discord_id not null
            return match[2]
        return None
    finally:
        conn.close()


def get_champion_name(player_id, partial_name):
    resolved_name = resolve_champion_name(partial_name)
    conn = sqlite3.connect("match_data.db")
    cursor = conn.cursor()
    if resolved_name:
        cursor.execute(
            "SELECT DISTINCT champ FROM player_stats WHERE player_id = ? AND champ = ? LIMIT 1;",
            (player_id, resolved_name)
        )
        result = cursor.fetchone()
        if result:
            conn.close()
            return result[0]

    cursor.execute(
        "SELECT DISTINCT champ FROM player_stats WHERE player_id = ? AND champ LIKE ? LIMIT 1;",
        (player_id, f"%{partial_name}%")
    )
    result = cursor.fetchone()
    conn.close()
    return result[0] if result else None



def get_all_champion_stats(player_id):
    conn = sqlite3.connect("match_data.db")
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT
            champ,
            COUNT(*) as games,
            SUM(CASE WHEN (ps.team = 1 AND m.team1_score > m.team2_score) OR (ps.team = 2 AND m.team2_score > m.team1_score) THEN 1 ELSE 0 END) as wins,
            SUM(kills), SUM(deaths), SUM(assists),
            SUM(m.time) as total_minutes
        FROM player_stats ps
        JOIN matches m ON ps.match_id = m.match_id
        WHERE player_id = ?
        GROUP BY champ
        """,
        (player_id,),
    )
    rows = cursor.fetchall()
    champs = []
    for row in rows:
        champ, games, wins, kills, deaths, assists, total_minutes = row
        
        champ_stats = {
            "champ": champ,
            "games": games,
            "winrate": round(100 * wins / games, 2) if games else 0,
            "kda_ratio": round((kills + assists) / max(1, deaths), 2),
            "time_played": f"{total_minutes // 60}h {total_minutes % 60}m"
        }
        champs.append(champ_stats)
    conn.close()
    return champs


def get_player_champion_stats(player_id, role_filter=None, min_games=1, filters=None):
    """
    Gets comprehensive stats for all champions played by a player.
    Returns a list of champion stats with all available metrics.
    """
    conn = sqlite3.connect("match_data.db")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    try:
        # Build where conditions for player filter
        where_conditions = ["ps.player_id = ?"]
        params = [player_id]
        
        # Apply role filter if specified
        if role_filter:
            champions_in_role = get_champions_for_role(role_filter)
            if not champions_in_role:
                return []
            placeholders = ', '.join('?' for _ in champions_in_role)
            where_conditions.append(f"ps.champ IN ({placeholders})")
            params.extend(champions_in_role)
        
        where_clause = " AND ".join(where_conditions)
        match_conditions = ["m.time > 0"]
        _apply_match_filters(match_conditions, params, filters, player_alias="pms")
        match_where_clause = " AND ".join(match_conditions)
        
        query = f"""
            WITH TeamTotals AS (
                SELECT
                    match_id,
                    team,
                    SUM(kills + assists) as team_kill_participations,
                    SUM(damage) as team_damage
                FROM player_stats GROUP BY match_id, team
            ),
            PlayerMatchShares AS (
                SELECT 
                    ps.*,
                    CASE WHEN tt.team_kill_participations > 0 THEN CAST(ps.kills + ps.assists AS REAL) * 100.0 / tt.team_kill_participations ELSE 0 END as kill_share,
                    CASE WHEN tt.team_damage > 0 THEN CAST(ps.damage AS REAL) * 100.0 / tt.team_damage ELSE 0 END as damage_share
                FROM player_stats ps
                JOIN TeamTotals tt ON ps.match_id = tt.match_id AND ps.team = tt.team
                WHERE {where_clause}
            )
            SELECT
                pms.champ,
                COUNT(pms.match_id) AS games,
                SUM(CASE WHEN (pms.team = 1 AND m.team1_score > m.team2_score) OR (pms.team = 2 AND m.team2_score > m.team1_score) THEN 1 ELSE 0 END) as wins,
                SUM(pms.kills) as total_kills,
                SUM(pms.deaths) as total_deaths,
                SUM(pms.assists) as total_assists,
                SUM(pms.damage) as total_damage,
                SUM(pms.taken) as total_taken,
                SUM(pms.objective_time) as total_obj_time,
                SUM(pms.shielding) as total_shielding,
                SUM(pms.healing) as total_healing,
                SUM(pms.self_healing) as total_self_healing,
                SUM(pms.credits) as total_credits,
                SUM(m.time) as total_minutes,
                
                -- Averages
                AVG(pms.kills) as avg_kills,
                AVG(pms.deaths) as avg_deaths,
                AVG(pms.damage) as avg_damage,
                AVG(pms.taken) as avg_taken,
                AVG(pms.objective_time) as avg_obj_time,
                AVG(pms.shielding) as avg_shielding,
                AVG(pms.healing) as avg_healing,
                AVG(pms.self_healing) as avg_self_healing,
                AVG(pms.credits) as avg_credits,
                AVG(pms.kill_share) as avg_kill_share,
                AVG(pms.damage_share) as avg_damage_share
                
            FROM PlayerMatchShares pms
            JOIN matches m ON pms.match_id = m.match_id
            WHERE {match_where_clause}
            GROUP BY pms.champ
            HAVING games >= ?
        """
        
        params.append(min_games)
        cursor.execute(query, params)
        rows = cursor.fetchall()
        
        champs = []
        for row in rows:
            champ_data = dict(row)
            games = champ_data['games']
            wins = champ_data['wins']
            total_minutes = champ_data['total_minutes'] or 1
            
            # Calculate all possible stats
            champ_stats = {
                "champ": champ_data['champ'],
                "games": games,
                "wins": wins,
                "losses": games - wins,
                "winrate": round(100 * wins / games, 2) if games else 0,
                "kda": f"{champ_data['total_kills']}/{champ_data['total_deaths']}/{champ_data['total_assists']}",
                "kda_ratio": round((champ_data['total_kills'] + champ_data['total_assists']) / max(1, champ_data['total_deaths']), 2),
                "kills_pm": round(champ_data['total_kills'] / total_minutes, 2),
                "deaths_pm": round(champ_data['total_deaths'] / total_minutes, 2),
                "damage_dealt_pm": round(champ_data['total_damage'] / total_minutes, 2),
                "damage_taken_pm": round(champ_data['total_taken'] / total_minutes, 2),
                "healing_pm": round(champ_data['total_healing'] / total_minutes, 2),
                "damage_healing_pm": round((champ_data['total_damage'] + champ_data['total_healing']) / total_minutes, 2),
                "self_healing_pm": round(champ_data['total_self_healing'] / total_minutes, 2),
                "credits_pm": round(champ_data['total_credits'] / total_minutes, 2),
                "avg_kills": round(champ_data['avg_kills'], 2),
                "avg_deaths": round(champ_data['avg_deaths'], 2),
                "avg_damage_dealt": round(champ_data['avg_damage']),
                "avg_damage_taken": round(champ_data['avg_taken']),
                "damage_delta": round(champ_data['avg_damage'] - champ_data['avg_taken']),
                "avg_healing": round(champ_data['avg_healing']),
                "avg_self_healing": round(champ_data['avg_self_healing']),
                "avg_shielding": round(champ_data['avg_shielding']),
                "avg_credits": round(champ_data['avg_credits']),
                "obj_time": round(champ_data['avg_obj_time'], 2),
                "kp": round(champ_data['avg_kill_share'], 2),
                "dmg_share": round(champ_data['avg_damage_share'], 2),
                "time_played": f"{int(total_minutes // 60)}h {int(total_minutes % 60)}m"
            }
            champs.append(champ_stats)
            
        return champs
    finally:
        conn.close()


def delete_match(match_id):
    conn = sqlite3.connect("match_data.db")
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT 1 FROM matches WHERE match_id = ?", (match_id,))
        if not cursor.fetchone():
            return 0

        cursor.execute("DELETE FROM player_stats WHERE match_id = ?", (match_id,))
        stats_deleted_count = cursor.rowcount

        cursor.execute("DELETE FROM matches WHERE match_id = ?", (match_id,))
        match_deleted_count = cursor.rowcount

        conn.commit()
        
        return stats_deleted_count + match_deleted_count
    except sqlite3.Error as e:
        print(f"An error occurred while deleting match {match_id}: {e}")
        conn.rollback()
        return 0
    finally:
        conn.close()


def get_champion_leaderboard(stat_key, limit, show_bottom=False, role=None, min_games=1, filters=None):
    """
    Gets leaderboard of champions (not players) aggregated across all games.
    Shows which champions have the best stats overall.
    """
    # CORRECTED: The 'ps.' alias has been replaced with 'ms.' to match the query below.
    stat_expressions = {
        "winrate": "SUM(CASE WHEN (ms.team = 1 AND m.team1_score > m.team2_score) OR (ms.team = 2 AND m.team2_score > m.team1_score) THEN 1 ELSE 0 END) * 100.0 / COUNT(ms.match_id)",
        "kda": "CAST(SUM(ms.kills) + SUM(ms.assists) AS REAL) / MAX(1, SUM(ms.deaths))",
        "kills_pm": "SUM(CAST(ms.kills AS REAL)) / SUM(m.time)",
        "deaths_pm": "SUM(CAST(ms.deaths AS REAL)) / SUM(m.time)",
        "damage_dealt_pm": "SUM(CAST(ms.damage AS REAL)) / SUM(m.time)",
        "damage_taken_pm": "SUM(CAST(ms.taken AS REAL)) / SUM(m.time)",
        "healing_pm": "SUM(CAST(ms.healing AS REAL)) / SUM(m.time)",
        "damage_healing_pm": "SUM(CAST(ms.damage + ms.healing AS REAL)) / SUM(m.time)",
        "self_healing_pm": "SUM(CAST(ms.self_healing AS REAL)) / SUM(m.time)",
        "credits_pm": "SUM(CAST(ms.credits AS REAL)) / SUM(m.time)",
        "avg_kills": "AVG(ms.kills)",
        "avg_deaths": "AVG(ms.deaths)",
        "avg_damage_dealt": "AVG(ms.damage)",
        "avg_damage_taken": "AVG(ms.taken)",
        "damage_delta": "AVG(ms.damage - ms.taken)",
        "avg_healing": "AVG(ms.healing)",
        "avg_self_healing": "AVG(ms.self_healing)",
        "avg_shielding": "AVG(ms.shielding)",
        "avg_credits": "AVG(ms.credits)",
        "obj_time": "AVG(ms.objective_time)",
        "kp": "AVG(ms.kill_share)",
        "dmg_share": "AVG(ms.damage_share)",
    }

    if stat_key not in stat_expressions:
        return None

    order = "ASC" if show_bottom else "DESC"
    params = []
    # CORRECTED: This alias was also incorrect in the original code. It should be ms.champ or ps.champ. 
    # Since the main table is ms, we'll check it, but since it's filtered before this CTE, we can just check champ.
    where_conditions = ["m.time > 0"] 
    
    if role:
        champions_in_role = get_champions_for_role(role)
        if not champions_in_role:
            return None
        placeholders = ', '.join('?' for _ in champions_in_role)
        # Use 'ms.champ' here to be explicit
        where_conditions.append(f"ms.champ IN ({placeholders})")
        params.extend(champions_in_role)

    _apply_match_filters(where_conditions, params, filters, player_alias="ms")

    where_clause = " AND ".join(where_conditions)
    final_params = params + [min_games, limit]

    query = f"""
        WITH TeamTotals AS (
            SELECT
                match_id,
                team,
                SUM(kills + assists) as team_kill_participations,
                SUM(damage) as team_damage
            FROM player_stats GROUP BY match_id, team
        ),
        MatchShares AS (
            SELECT 
                ps.*,
                CASE WHEN tt.team_kill_participations > 0 THEN CAST(ps.kills + ps.assists AS REAL) * 100.0 / tt.team_kill_participations ELSE 0 END as kill_share,
                CASE WHEN tt.team_damage > 0 THEN CAST(ps.damage AS REAL) * 100.0 / tt.team_damage ELSE 0 END as damage_share
            FROM player_stats ps
            JOIN TeamTotals tt ON ps.match_id = tt.match_id AND ps.team = tt.team
        ),
        ChampionAggregates AS (
            SELECT
                ms.champ,
                COUNT(ms.match_id) AS games_played,
                SUM(CASE WHEN (ms.team = 1 AND m.team1_score > m.team2_score) OR (ms.team = 2 AND m.team2_score > m.team1_score) THEN 1 ELSE 0 END) as wins,
                SUM(ms.kills) as total_k,
                SUM(ms.deaths) as total_d,
                SUM(ms.assists) as total_a,
                ({stat_expressions[stat_key]}) AS value
            FROM MatchShares ms
            JOIN matches m ON ms.match_id = m.match_id
            WHERE {where_clause}
            GROUP BY ms.champ
            HAVING games_played >= ?
        )
        SELECT
            ca.champ,
            ca.value,
            ca.wins,
            ca.games_played,
            (ca.games_played - ca.wins) AS losses,
            ca.total_k as k,
            ca.total_d as d,
            ca.total_a as a,
            (SELECT COUNT(*) FROM ChampionAggregates) as total_champions
        FROM ChampionAggregates ca
        ORDER BY ca.value {order}, ca.games_played DESC
        LIMIT ?;
    """

    conn = sqlite3.connect("match_data.db")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    try:
        cursor.execute(query, tuple(final_params))
        return [dict(row) for row in cursor.fetchall()]
    except sqlite3.Error as e:
        print(f"Database error in get_champion_leaderboard: {e}")
        return None
    finally:
        conn.close()
