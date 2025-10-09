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

        for player in players:
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
            team = player["team"]

            cursor.execute(
                "SELECT player_id, discord_id, alt_igns FROM players WHERE player_ign = ? COLLATE NOCASE;",
                (ign,),
            )
            result = cursor.fetchone()
            if not result:
                cursor.execute("SELECT player_id, discord_id, alt_igns FROM players;")
                player_id = None
                for row in cursor.fetchall():
                    alt_player_id, discord_id, alt_igns_json = row
                    alt_igns = json.loads(alt_igns_json) if alt_igns_json else []
                    # Case-insensitive check for alt IGNs
                    if any(alt.lower() == ign.lower() for alt in alt_igns):
                        print(f"alt ign for user: {discord_id} -> {ign}")
                        player_id = alt_player_id
                        break
                if not player_id:
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
                    match_id, player_id, champ, talent, credits, kills, deaths, assists,
                    damage, taken, objective_time, shielding, healing, self_healing, team,
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
    conn = sqlite3.connect("match_data.db")
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT discord_id FROM players WHERE player_ign = ? COLLATE NOCASE;", (player_ign,)
        )
        ign_result = cursor.fetchone()
        cursor.execute(
            "SELECT player_ign FROM players WHERE discord_id = ?;", (discord_id,)
        )
        disc_result = cursor.fetchone()

        if ign_result:
            if not force:
                return False
            cursor.execute(
                "UPDATE players SET discord_id = ? WHERE player_ign = ? COLLATE NOCASE;",
                (discord_id, player_ign),
            )
        elif disc_result:
            cursor.execute(
                "UPDATE players SET player_ign = ? WHERE discord_id = ?;",
                (player_ign, discord_id),
            )
        else:
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


def get_registered_igns(ign_list):
    conn = sqlite3.connect("match_data.db")
    cursor = conn.cursor()
    try:
        # Use case-insensitive comparison
        placeholders = ",".join("?" for _ in ign_list)
        query = f"SELECT player_ign FROM players WHERE player_ign COLLATE NOCASE IN ({placeholders});"
        cursor.execute(query, ign_list)
        registered_db_igns = {row[0] for row in cursor.fetchall()}
        
        # Check which IGNs from the input list are registered (case-insensitive)
        registered = []
        not_registered = []
        
        for ign in ign_list:
            found = False
            for db_ign in registered_db_igns:
                if db_ign.lower() == ign.lower():
                    registered.append(db_ign)  # Use the DB version (preserves case)
                    found = True
                    break
            if not found:
                not_registered.append(ign)
        
        return registered, not_registered
    finally:
        conn.close()


def add_alt_ign(discord_id, alt_ign):
    conn = sqlite3.connect("match_data.db")
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT player_id, alt_igns FROM players WHERE discord_id = ?;", (discord_id,)
        )
        result = cursor.fetchone()
        if not result:
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
            return True
        else:
            return False
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
        return False
    finally:
        conn.close()


def get_ign_link_info(ign):
    conn = sqlite3.connect("match_data.db")
    cursor = conn.cursor()
    try:
        # Use COLLATE NOCASE for case-insensitive comparison
        cursor.execute("SELECT discord_id, player_ign FROM players WHERE player_ign = ? COLLATE NOCASE;", (ign,))
        result = cursor.fetchone()
        if result:
            return result[0], True, result[1]  # Return actual stored IGN
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
    conn = sqlite3.connect("match_data.db")
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT player_id, alt_igns FROM players WHERE discord_id = ?;", (discord_id,)
        )
        result = cursor.fetchone()
        if not result:
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
            return True
        else:
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


def get_player_stats(player_id, champions=None):
    """
    Fetches aggregated player stats, now including Kill Participation and Damage Share.
    If no champion/role filter is provided, 'healing' stats are calculated
    from games played on Support champions only, while 'self_healing' uses all games.
    """
    conn = sqlite3.connect("match_data.db")
    conn.row_factory = sqlite3.Row 
    cursor = conn.cursor()
    
    try:
        query = """
            WITH TeamTotals AS (
                SELECT
                    match_id,
                    team,
                    SUM(kills) AS team_kills,
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
                SUM(m.time) AS total_time_in_minutes,
                SUM(CASE WHEN (ps.team = 1 AND m.team1_score > m.team2_score) OR (ps.team = 2 AND m.team2_score > m.team1_score) THEN 1 ELSE 0 END) AS total_wins,
                
                -- MODIFIED: Kill Participation now includes assists
                AVG(CASE WHEN tt.team_kills > 0 THEN CAST(ps.kills + ps.assists AS REAL) * 100.0 / tt.team_kills ELSE 0 END) AS avg_kill_share,
                AVG(CASE WHEN tt.team_damage > 0 THEN CAST(ps.damage AS REAL) * 100.0 / tt.team_damage ELSE 0 END) AS avg_damage_share

            FROM player_stats ps
            JOIN matches m ON ps.match_id = m.match_id
            JOIN TeamTotals tt ON ps.match_id = tt.match_id AND ps.team = tt.team
            WHERE ps.player_id = ?
        """
        
        params = [player_id]
        
        if champions and isinstance(champions, list):
            placeholders = ', '.join('?' for _ in champions)
            query += f" AND ps.champ IN ({placeholders})"
            params.extend(champions)

        cursor.execute(query, params)
        data = cursor.fetchone()

        if not data or data["games_played"] == 0:
            return None

        games_played = data["games_played"]
        total_wins = data["total_wins"]
        total_time_in_minutes = data["total_time_in_minutes"]

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
        }

        total_healing = data["total_healing"]
        support_time = total_time_in_minutes
        support_games = games_played

        if not champions:
            support_champs = [champ for champ, role in CHAMPION_ROLES.items() if role == "Support"]
            placeholders = ', '.join('?' for _ in support_champs)
            cursor.execute(f"""
                SELECT SUM(ps.healing), SUM(m.time), COUNT(ps.match_id)
                FROM player_stats ps JOIN matches m ON ps.match_id = m.match_id
                WHERE ps.player_id = ? AND ps.champ IN ({placeholders})
            """, [player_id] + support_champs)
            healing_row = cursor.fetchone()
            total_healing = healing_row[0] or 0
            support_time = healing_row[1] or 0
            support_games = healing_row[2] or 0
            
        stats_dict["healing_pm"] = round(total_healing / max(1, support_time), 2)
        stats_dict["avg_healing"] = round(total_healing / max(1, support_games))

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
        WHERE ps1.player_id = ? AND ps2.player_id = ? AND ps1.team = ps2.team
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
        WHERE ps1.player_id = ? AND ps2.player_id = ? AND ps1.team != ps2.team
        """,
        (pid1, pid2),
    )
    rows = cursor.fetchall()
    against_games = len(rows)
    against_wins = sum(1 for t1, t2, team in rows if (team == 1 and t1 > t2) or (team == 2 and t2 > t1))
    against_winrate = round(100 * against_wins / against_games, 1) if against_games else 0
    conn.close()
    return with_winrate, with_games, against_winrate, against_games

def compare_players(discord_id1, discord_id2):
    pid1 = get_player_id(discord_id1)
    pid2 = get_player_id(discord_id2)
    if not pid1 or not pid2:
        return None

    stats1 = get_player_stats(pid1)
    stats2 = get_player_stats(pid2)
    champs1 = get_top_champs(pid1)
    champs2 = get_top_champs(pid2)
    with_winrate, with_games, against_winrate, against_games = get_winrate_with_against(pid1, pid2)

    return {
        "player1": stats1, "player2": stats2, "top_champs1": champs1, "top_champs2": champs2,
        "with_winrate": with_winrate, "with_games": with_games,
        "against_winrate": against_winrate, "against_games": against_games,
    }

def get_match_history(player_id, limit: int = 30):
    conn = sqlite3.connect("match_data.db")
    cursor = conn.cursor()
    try:
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
            WHERE ps.player_id = ?
            ORDER BY ps.player_stats_id DESC
            LIMIT ?;
        """, (player_id, limit))
        return cursor.fetchall()
    finally:
        conn.close()

CHAMPION_ROLES = {
    "Bomb King": "Damage", "Cassie": "Damage", "Dredge": "Damage", "Drogoz": "Damage",
    "Imani": "Damage", "Kinessa": "Damage", "Lian": "Damage", "Octavia": "Damage",
    "Saati": "Damage", "Sha Lin": "Damage", "Strix": "Damage", "Tiberius": "Damage",
    "Tyra": "Damage", "Viktor": "Damage", "Willo": "Damage", "Betty la Bomba": "Damage",
    "Omen": "Damage",
    "Androxus": "Flank", "Buck": "Flank", "Caspian": "Flank", "Evie": "Flank",
    "Koga": "Flank", "Lex": "Flank", "Maeve": "Flank", "Moji": "Flank",
    "Skye": "Flank", "Talus": "Flank", "Vatu": "Flank", "Vora": "Flank",
    "VII": "Flank", "Zhin": "Flank",
    "Ash": "Tank", "Atlas": "Tank", "Azaan": "Tank", "Barik": "Tank", "Fernando": "Tank",
    "Inara": "Tank", "Khan": "Tank", "Makoa": "Tank", "Raum": "Tank", "Ruckus": "Tank",
    "Terminus": "Tank", "Torvald": "Tank", "Yagorath": "Tank", "Nyx": "Tank",
    "Corvus": "Support", "Furia": "Support", "Ghrok": "Support", "Grover": "Support",
    "Io": "Support", "Jenos": "Support", "Lillith": "Support", "Mal'Damba": "Support",
    "Pip": "Support", "Rei": "Support", "Seris": "Support", "Ying": "Support",
}

def get_leaderboard(stat_key, limit, show_bottom=False, champion=None, role=None, min_games=1):
    stat_expressions = {
        "winrate": "SUM(CASE WHEN (ps.team = 1 AND m.team1_score > m.team2_score) OR (ps.team = 2 AND m.team2_score > m.team1_score) THEN 1 ELSE 0 END) * 100.0 / COUNT(ps.match_id)",
        "kda": "CAST(SUM(ps.kills) + SUM(ps.assists) AS REAL) / MAX(1, SUM(ps.deaths))",
        "kills_pm": "SUM(CAST(ps.kills AS REAL)) / SUM(m.time)",
        "deaths_pm": "SUM(CAST(ps.deaths AS REAL)) / SUM(m.time)",
        "damage_dealt_pm": "SUM(CAST(ps.damage AS REAL)) / SUM(m.time)",
        "damage_taken_pm": "SUM(CAST(ps.taken AS REAL)) / SUM(m.time)",
        "healing_pm": "SUM(CAST(ps.healing AS REAL)) / SUM(m.time)",
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
    healing_only_stats = ["healing_pm", "avg_healing"]

    if champion:
        where_conditions.append("ps.champ LIKE ?")
        params.append(f"%{champion}%")
    elif role:
        champions_in_role = [c for c, r in CHAMPION_ROLES.items() if r == role]
        if not champions_in_role: return None
        placeholders = ', '.join('?' for _ in champions_in_role)
        where_conditions.append(f"ps.champ IN ({placeholders})")
        params.extend(champions_in_role)
    elif stat_key in healing_only_stats:
        champions_in_role = [c for c, r in CHAMPION_ROLES.items() if r == "Support"]
        placeholders = ', '.join('?' for _ in champions_in_role)
        where_conditions.append(f"ps.champ IN ({placeholders})")
        params.extend(champions_in_role)

    where_clause = " AND ".join(where_conditions)
    final_params = params + [min_games, limit]

    query = f"""
        WITH TeamTotals AS (
            SELECT match_id, team, SUM(kills) as team_kills, SUM(damage) as team_damage
            FROM player_stats GROUP BY match_id, team
        ),
        MatchShares AS (
            SELECT 
                ps.*,
                -- MODIFIED: Kill Participation now includes assists
                CASE WHEN tt.team_kills > 0 THEN CAST(ps.kills + ps.assists AS REAL) * 100.0 / tt.team_kills ELSE 0 END as kill_share,
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


def get_discord_id_for_ign(ign):
    conn = sqlite3.connect("match_data.db")
    cursor = conn.cursor()
    cursor.execute(
        "SELECT discord_id FROM players WHERE player_ign LIKE ? COLLATE NOCASE AND discord_id IS NOT NULL;",
        (ign,)
    )
    result = cursor.fetchone()
    conn.close()
    return result[0] if result else None


def get_champion_name(player_id, partial_name):
    conn = sqlite3.connect("match_data.db")
    cursor = conn.cursor()
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


def get_player_champion_stats(player_id, role_filter=None, min_games=1):
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
            champions_in_role = [c for c, r in CHAMPION_ROLES.items() if r == role_filter]
            if not champions_in_role:
                return []
            placeholders = ', '.join('?' for _ in champions_in_role)
            where_conditions.append(f"ps.champ IN ({placeholders})")
            params.extend(champions_in_role)
        
        where_clause = " AND ".join(where_conditions)
        
        query = f"""
            WITH TeamTotals AS (
                SELECT match_id, team, SUM(kills) as team_kills, SUM(damage) as team_damage
                FROM player_stats GROUP BY match_id, team
            ),
            PlayerMatchShares AS (
                SELECT 
                    ps.*,
                    CASE WHEN tt.team_kills > 0 THEN CAST(ps.kills + ps.assists AS REAL) * 100.0 / tt.team_kills ELSE 0 END as kill_share,
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
            WHERE m.time > 0
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


def get_champion_leaderboard(stat_key, limit, show_bottom=False, role=None, min_games=1):
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
        champions_in_role = [c for c, r in CHAMPION_ROLES.items() if r == role]
        if not champions_in_role:
            return None
        placeholders = ', '.join('?' for _ in champions_in_role)
        # Use 'ms.champ' here to be explicit
        where_conditions.append(f"ms.champ IN ({placeholders})")
        params.extend(champions_in_role)

    where_clause = " AND ".join(where_conditions)
    final_params = params + [min_games, limit]

    query = f"""
        WITH TeamTotals AS (
            SELECT match_id, team, SUM(kills) as team_kills, SUM(damage) as team_damage
            FROM player_stats GROUP BY match_id, team
        ),
        MatchShares AS (
            SELECT 
                ps.*,
                CASE WHEN tt.team_kills > 0 THEN CAST(ps.kills + ps.assists AS REAL) * 100.0 / tt.team_kills ELSE 0 END as kill_share,
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