import json
import re
import sqlite3


def create_database():
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
            team2_score INTEGER
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

    # Ensure 'team' column exists in player_stats
    cursor.execute("PRAGMA table_info(player_stats);")
    columns = [row[1] for row in cursor.fetchall()]
    if "team" not in columns:
        print("Adding 'team' column to player_stats table...")
        cursor.execute("ALTER TABLE player_stats ADD COLUMN team INTEGER;")
        conn.commit()
        migrate_team_column()

    conn.commit()
    conn.close()


def insert_scoreboard(scoreboard, queue_num):
    """
    Insert match data and player stats into the database.
    Prints when an IGN matches an alt_ign for a user.
    """
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

        cursor.execute("SELECT 1 FROM matches WHERE match_id = ?;", (match_id,))
        if cursor.fetchone():
            print(f"Warning: Match with match_id {match_id} already exists. Skipping.")
            return

        cursor.execute(
            """
            INSERT INTO matches (match_id, time, region, map, team1_score, team2_score, queue_num)
            VALUES (?, ?, ?, ?, ?, ?, ?);
            """,
            (match_id, time, region, map_name, team1_score, team2_score, queue_num),
        )

        for idx, player in enumerate(players):
            ign = player["name"]
            champ = player["champ"]
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
            team = 1 if idx < 5 else 2

            # Find player by main IGN
            cursor.execute(
                "SELECT player_id, discord_id, alt_igns FROM players WHERE player_ign = ?;",
                (ign,),
            )
            result = cursor.fetchone()
            if not result:
                # If not found, check alt_igns for all users
                cursor.execute("SELECT player_id, discord_id, alt_igns FROM players;")
                player_id = None
                for row in cursor.fetchall():
                    alt_player_id, discord_id, alt_igns_json = row
                    alt_igns = json.loads(alt_igns_json) if alt_igns_json else []
                    if ign in alt_igns:
                        print(f"alt ign for user: {discord_id} -> {ign}")
                        player_id = alt_player_id
                        break
                if not player_id:
                    # If still not found, create new player
                    cursor.execute(
                        "INSERT INTO players (player_ign, alt_igns) VALUES (?, ?);",
                        (ign, json.dumps([])),
                    )
                    player_id = cursor.lastrowid
            else:
                player_id = result[0]

            cursor.execute(
                """
                INSERT INTO player_stats (
                    match_id, player_id, champ, talent, credits, kills, deaths, assists,
                    damage, taken, objective_time, shielding, healing, self_healing, team
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    match_id,
                    player_id,
                    champ,
                    talent,
                    credits,
                    kills,
                    deaths,
                    assists,
                    damage,
                    taken,
                    objective_time,
                    shielding,
                    healing,
                    self_healing,
                    team,
                ),
            )
        conn.commit()
        print(f"Scoreboard for match_id {match_id} inserted successfully.")
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
        conn.rollback()
    finally:
        conn.close()


def link_ign(player_ign, discord_id, force=False):
    """
    Links a discord_id to an IGN.
    If force is False, only link if IGN and Discord ID are not already linked.
    If force is True, replace the IGN for the Discord ID.
    """
    conn = sqlite3.connect("match_data.db")
    cursor = conn.cursor()
    try:
        # Check if IGN is already linked
        cursor.execute(
            "SELECT discord_id FROM players WHERE player_ign = ?;", (player_ign,)
        )
        ign_result = cursor.fetchone()
        # Check if Discord ID is already linked
        cursor.execute(
            "SELECT player_ign FROM players WHERE discord_id = ?;", (discord_id,)
        )
        disc_result = cursor.fetchone()

        if ign_result:
            if not force:  # IGN already linked, do not overwrite
                return False
            # Overwrite existing link for IGN
            cursor.execute(
                "UPDATE players SET discord_id = ? WHERE player_ign = ?;",
                (discord_id, player_ign),
            )
        elif disc_result:  # Discord ID exists, update IGN
            cursor.execute(
                "UPDATE players SET player_ign = ? WHERE discord_id = ?;",
                (player_ign, discord_id),
            )
        else:  # Neither exists, insert new
            cursor.execute(
                "INSERT INTO players (player_ign, discord_id, alt_igns) VALUES (?, ?, ?);",
                (player_ign, discord_id, "[]"),
            )
        conn.commit()
        return True
    except sqlite3.Error as e:
        print(f"An error occurred in link_ign: {e}")
        return False
    finally:
        conn.close()


def update_discord_id(old_discord_id, new_discord_id):
    """
    Updates the Discord ID for an existing player by looking up the player
    via old_discord_id and linking the new Discord ID to it.
    """
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
    """Execute a SELECT SQL query and return the results."""
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
    """
    Inserts embed data into the embeds table, storing only the queue number
    and the 10 Discord IDs.
    """
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
            print(
                f"Embed for queue_num {queue_num} already exists. Skipping insertion."
            )
            return

        cursor.execute(
            "INSERT INTO embeds (queue_num, embed_data) VALUES (?, ?);",
            (queue_num, json.dumps(discord_ids)),
        )
        conn.commit()
        print(
            f"Embed for queue_num {queue_num} inserted with Discord IDs: {discord_ids}"
        )
    except sqlite3.Error as e:
        print(f"An error occurred while inserting embed: {e}")
    finally:
        conn.close()


def read_embeds(queue_num):
    """Reads embed data from the database for the given queue number."""
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
    """Verifies if all provided Discord IDs are registered in the database."""
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


def get_registered_igns(ign_list):
    conn = sqlite3.connect("match_data.db")
    cursor = conn.cursor()
    try:
        placeholders = ",".join("?" for _ in ign_list)
        query = f"SELECT player_ign FROM players WHERE player_ign IN ({placeholders});"
        cursor.execute(query, ign_list)
        registered = {row[0] for row in cursor.fetchall()}
        not_registered = [ign for ign in ign_list if ign not in registered]
        return registered, not_registered
    finally:
        conn.close()


def add_alt_ign(discord_id, alt_ign):
    """Adds an alt IGN to a player's alt_igns list."""
    conn = sqlite3.connect("match_data.db")
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT player_id, alt_igns FROM players WHERE discord_id = ?;", (discord_id,)
        )
        result = cursor.fetchone()
        if not result:
            print(f"No player found with Discord ID {discord_id}.")
            return False
        player_id, alt_igns_json = result
        alt_igns = json.loads(alt_igns_json) if alt_igns_json else []
        if alt_ign not in alt_igns:
            alt_igns.append(alt_ign)
            cursor.execute(
                "UPDATE players SET alt_igns = ? WHERE player_id = ?;",
                (json.dumps(alt_igns), player_id),
            )
            conn.commit()
            print(f"Added alt IGN {alt_ign} for Discord ID {discord_id}.")
            return True
        else:
            print(f"Alt IGN {alt_ign} already exists for Discord ID {discord_id}.")
            return False
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
        return False
    finally:
        conn.close()


def get_ign_link_info(ign):
    """Returns (discord_id, exists) for the given IGN."""
    conn = sqlite3.connect("match_data.db")
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT discord_id FROM players WHERE player_ign = ?;", (ign,))
        result = cursor.fetchone()
        if result:
            return result[0], True
        return None, False
    finally:
        conn.close()


def get_ign_for_discord_id(discord_id):
    """Returns the IGN linked to this Discord ID, or None if not found."""
    conn = sqlite3.connect("match_data.db")
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT player_ign FROM players WHERE discord_id = ?;", (discord_id,))
        result = cursor.fetchone()
        return result[0] if result else None
    finally:
        conn.close()


def get_alt_igns(discord_id):
    """Returns the list of alt IGNs for a given Discord ID."""
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


def delete_alt_ign(discord_id, alt_ign):
    """Removes an alt IGN from a player's alt_igns list."""
    conn = sqlite3.connect("match_data.db")
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT player_id, alt_igns FROM players WHERE discord_id = ?;", (discord_id,)
        )
        result = cursor.fetchone()
        if not result:
            print(f"No player found with Discord ID {discord_id}.")
            return False
        player_id, alt_igns_json = result
        alt_igns = json.loads(alt_igns_json) if alt_igns_json else []
        if alt_ign in alt_igns:
            alt_igns.remove(alt_ign)
            cursor.execute(
                "UPDATE players SET alt_igns = ? WHERE player_id = ?;",
                (json.dumps(alt_igns), player_id),
            )
            conn.commit()
            print(f"Deleted alt IGN {alt_ign} for Discord ID {discord_id}.")
            return True
        else:
            print(f"Alt IGN {alt_ign} not found for Discord ID {discord_id}.")
            return False
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


def get_stats(player_id):
    conn = sqlite3.connect("match_data.db")
    cursor = conn.cursor()
    # This function is now a wrapper for the more detailed get_player_stats
    stats = get_player_stats(player_id)
    conn.close()
    if not stats or stats["games"] == 0:
        return None
    return {
        "kills": stats["damage_dealt_pm"],
        "deaths": stats["deaths_pm"],
        "assists": stats["assists_pm"],
        "damage": stats["damage_dealt_pm"],
        "objective_time": stats["obj_time_pm"],
        "shielding": stats["shielding_pm"],
        "healing": stats["healing_pm"],
        "games": stats["games"],
    }


def get_player_stats(player_id, champion=None):
    """
    NEW: Fetches detailed stats for a player, with an optional champion filter.
    Used by the new !stats command.
    """
    conn = sqlite3.connect("match_data.db")
    cursor = conn.cursor()
    try:
        query = """
            SELECT
                ps.kills, ps.deaths, ps.assists, ps.damage, ps.objective_time,
                ps.shielding, ps.healing, m.time,
                CASE
                    WHEN (ps.team = 1 AND m.team1_score > m.team2_score) OR (ps.team = 2 AND m.team2_score > m.team1_score) THEN 1
                    ELSE 0
                END AS win
            FROM player_stats ps
            JOIN matches m ON ps.match_id = m.match_id
            WHERE ps.player_id = ?
        """
        params = [player_id]
        if champion:
            query += " AND ps.champ LIKE ?"
            params.append(f"%{champion}%")

        cursor.execute(query, params)
        rows = cursor.fetchall()
        if not rows:
            return None

        total_kills = sum(r[0] for r in rows)
        total_deaths = sum(r[1] for r in rows)
        total_assists = sum(r[2] for r in rows)
        total_damage = sum(r[3] for r in rows)
        total_obj_time = sum(r[4] for r in rows)
        total_shielding = sum(r[5] for r in rows)
        total_healing = sum(r[6] for r in rows)
        total_seconds = sum(r[7] for r in rows)
        total_wins = sum(r[8] for r in rows)
        games_played = len(rows)

        # FIX: Correctly calculate per-minute stats
        total_minutes = total_seconds / 60.0 if total_seconds > 0 else 1.0

        return {
            "games": games_played,
            "winrate": round((total_wins / games_played) * 100, 1) if games_played > 0 else 0,
            "kda": f"{total_kills}/{total_deaths}/{total_assists}",
            "kda_ratio": round((total_kills + total_assists) / max(1, total_deaths), 2),
            "kills_pm": round(total_kills / total_minutes, 2),
            "deaths_pm": round(total_deaths / total_minutes, 2),
            "assists_pm": round(total_assists / total_minutes, 2),
            "damage_dealt_pm": round(total_damage / total_minutes, 2),
            "healing_pm": round(total_healing / total_minutes, 2),
            "shielding_pm": round(total_shielding / total_minutes, 2),
            "obj_time_pm": round(total_obj_time / total_minutes, 2),
        }
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
            damage, obj_time, shielding, healing, total_seconds
        ) = row

        # FIX: Correctly calculate per-minute stats
        total_minutes = total_seconds / 60.0 if total_seconds > 0 else 1.0

        champ_stats = {
            "champ": champ,
            "games": games,
            "winrate": round(100 * wins / games, 1) if games else 0,
            "kda": f"{round(kills/games, 1)}/{round(deaths/games, 1)}/{round(assists/games, 1)}",
            "damage": round(damage / total_minutes, 2),
            "objective_time": round(obj_time / total_minutes, 2),
            "shielding": round(shielding / total_minutes, 2),
            "healing": round(healing / total_minutes, 2),
        }
        champs.append(champ_stats)
    conn.close()
    return champs


def get_winrate_with_against(pid1, pid2):
    conn = sqlite3.connect("match_data.db")
    cursor = conn.cursor()
    # With: both players on the same team
    cursor.execute(
        """
        SELECT m.team1_score, m.team2_score, ps1.team
        FROM matches m
        JOIN player_stats ps1 ON m.match_id = ps1.match_id
        JOIN player_stats ps2 ON m.match_id = ps2.match_id
        WHERE ps1.player_id = ? AND ps2.player_id = ? AND ps1.team = ps2.team
        """,
        (pid1, pid2),
    )
    rows = cursor.fetchall()
    with_games = len(rows)
    with_wins = 0
    for t1, t2, team in rows:
        if (team == 1 and t1 > t2) or (team == 2 and t2 > t1):
            with_wins += 1
    with_winrate = round(100 * with_wins / with_games, 1) if with_games else 0

    # Against: both players on opposite teams
    cursor.execute(
        """
        SELECT m.team1_score, m.team2_score, ps1.team
        FROM matches m
        JOIN player_stats ps1 ON m.match_id = ps1.match_id
        JOIN player_stats ps2 ON m.match_id = ps2.match_id
        WHERE ps1.player_id = ? AND ps2.player_id = ? AND ps1.team != ps2.team
        """,
        (pid1, pid2),
    )
    rows = cursor.fetchall()
    against_games = len(rows)
    against_wins = 0
    for t1, t2, team in rows:
        if (team == 1 and t1 > t2) or (team == 2 and t2 > t1):
            against_wins += 1
    against_winrate = round(100 * against_wins / against_games, 1) if against_games else 0
    conn.close()
    return with_winrate, with_games, against_winrate, against_games


def compare_players(discord_id1, discord_id2):
    """
    Returns stats for two players for the !compare command.
    """
    pid1 = get_player_id(discord_id1)
    pid2 = get_player_id(discord_id2)
    if not pid1 or not pid2:
        return None

    # Use the new detailed stats function
    stats1 = get_player_stats(pid1)
    stats2 = get_player_stats(pid2)
    champs1 = get_top_champs(pid1)
    champs2 = get_top_champs(pid2)
    with_winrate, with_games, against_winrate, against_games = get_winrate_with_against(
        pid1, pid2
    )

    return {
        "player1": stats1,
        "player2": stats2,
        "top_champs1": champs1,
        "top_champs2": champs2,
        "with_winrate": with_winrate,
        "with_games": with_games,
        "against_winrate": against_winrate,
        "against_games": against_games,
    }


def get_match_history(player_id):
    """
    NEW: Fetches the last 5 matches for a player for the !history command.
    """
    conn = sqlite3.connect("match_data.db")
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT
                m.map,
                ps.champ,
                ps.kills, ps.deaths, ps.assists,
                CASE
                    WHEN (ps.team = 1 AND m.team1_score > m.team2_score) OR (ps.team = 2 AND m.team2_score > m.team1_score) THEN 'W'
                    ELSE 'L'
                END as result
            FROM player_stats ps
            JOIN matches m ON ps.match_id = m.match_id
            WHERE ps.player_id = ?
            ORDER BY ps.player_stats_id DESC
            LIMIT 5;
        """, (player_id,))
        return cursor.fetchall()
    finally:
        conn.close()


def get_leaderboard(stat, limit):
    """
    NEW: Fetches leaderboard data based on a validated stat.
    Uses pre-defined queries to prevent SQL injection.
    """
    # Whitelist of allowed stats and their corresponding SQL aggregations
    # A minimum of 10 games is required for KDA and Winrate leaderboards
    queries = {
        "damage": """
            SELECT p.discord_id, SUM(ps.damage) * 60.0 / SUM(m.time) as value
            FROM player_stats ps
            JOIN players p ON ps.player_id = p.player_id
            JOIN matches m ON ps.match_id = m.match_id
            WHERE p.discord_id IS NOT NULL AND m.time > 0
            GROUP BY p.discord_id
            ORDER BY value DESC
            LIMIT ?;
        """,
        "healing": """
            SELECT p.discord_id, SUM(ps.healing) * 60.0 / SUM(m.time) as value
            FROM player_stats ps
            JOIN players p ON ps.player_id = p.player_id
            JOIN matches m ON ps.match_id = m.match_id
            WHERE p.discord_id IS NOT NULL AND m.time > 0
            GROUP BY p.discord_id
            ORDER BY value DESC
            LIMIT ?;
        """,
        "obj_time": """
            SELECT p.discord_id, SUM(ps.objective_time) * 60.0 / SUM(m.time) as value
            FROM player_stats ps
            JOIN players p ON ps.player_id = p.player_id
            JOIN matches m ON ps.match_id = m.match_id
            WHERE p.discord_id IS NOT NULL AND m.time > 0
            GROUP BY p.discord_id
            ORDER BY value DESC
            LIMIT ?;
        """,
        "kda": """
            SELECT p.discord_id, CAST(SUM(ps.kills) + SUM(ps.assists) AS REAL) / MAX(1, SUM(ps.deaths)) as value
            FROM player_stats ps
            JOIN players p ON ps.player_id = p.player_id
            WHERE p.discord_id IS NOT NULL
            GROUP BY p.discord_id
            HAVING COUNT(ps.match_id) >= 10
            ORDER BY value DESC
            LIMIT ?;
        """,
        "winrate": """
            SELECT p.discord_id,
                   SUM(CASE WHEN (ps.team = 1 AND m.team1_score > m.team2_score) OR (ps.team = 2 AND m.team2_score > m.team1_score) THEN 1 ELSE 0 END) * 100.0 / COUNT(ps.match_id) as value
            FROM player_stats ps
            JOIN players p ON ps.player_id = p.player_id
            JOIN matches m ON ps.match_id = m.match_id
            WHERE p.discord_id IS NOT NULL
            GROUP BY p.discord_id
            HAVING COUNT(ps.match_id) >= 10
            ORDER BY value DESC
            LIMIT ?;
        """
    }

    if stat not in queries:
        return None # Invalid stat

    conn = sqlite3.connect("match_data.db")
    cursor = conn.cursor()
    try:
        cursor.execute(queries[stat], (limit,))
        return cursor.fetchall()
    finally:
        conn.close()


def migrate_team_column():
    conn = sqlite3.connect("match_data.db")
    cursor = conn.cursor()
    try:
        # Check if 'team' column exists
        cursor.execute("PRAGMA table_info(player_stats);")
        columns = [row[1] for row in cursor.fetchall()]
        if "team" not in columns:
            print("No 'team' column found in player_stats. Migration skipped.")
            return

        # For each match, set team=1 for first 5, team=2 for next 5
        cursor.execute("SELECT match_id FROM matches;")
        match_ids = [row[0] for row in cursor.fetchall()]
        for match_id in match_ids:
            cursor.execute(
                "SELECT player_stats_id FROM player_stats WHERE match_id = ? ORDER BY player_stats_id ASC;",
                (match_id,),
            )
            ids = [row[0] for row in cursor.fetchall()]
            for idx, ps_id in enumerate(ids):
                team = 1 if idx < 5 else 2
                cursor.execute(
                    "UPDATE player_stats SET team = ? WHERE player_stats_id = ?;",
                    (team, ps_id),
                )
        conn.commit()
        print("Migration: Populated 'team' column in player_stats.")
    except Exception as e:
        print(f"Migration error: {e}")
    finally:
        conn.close()


def get_old_stats(player_id):
    """
    [LEGACY] This function replicates the original get_stats logic for the
    admin-only !old_stats command. It uses the original per-second normalization.
    """
    conn = sqlite3.connect("match_data.db")
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT kills, deaths, assists, damage, objective_time, shielding, healing, match_id
        FROM player_stats
        WHERE player_id = ?
        """,
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

    if total_time == 0:
        total_time = 1 # Avoid division by zero

    agg_stats = [sum(col) for col in zip(*[row[:7] for row in rows])]
    norm_stats = [round(val / total_time, 2) for val in agg_stats]

    return {
        "kills": norm_stats[0],
        "deaths": norm_stats[1],
        "assists": norm_stats[2],
        "damage": norm_stats[3],
        "objective_time": norm_stats[4],
        "shielding": norm_stats[5],
        "healing": norm_stats[6],
        "games": len(rows),
    }