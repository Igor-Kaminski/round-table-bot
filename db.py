import sqlite3


def create_database():
    # Connect to SQLite database (or create it if it doesn't exist)
    conn = sqlite3.connect("match_data.db")
    cursor = conn.cursor()

    # Create matches table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS matches (
        match_id INTEGER PRIMARY KEY, -- Unique match ID
        time_minutes INTEGER,
        region TEXT,
        map TEXT,
        team1_score INTEGER,
        team2_score INTEGER,
        won INTEGER CHECK(won IN (0, 1)) -- 1 if team1 won, 0 otherwise
    );
    """)

    # Create players table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS players (
        player_id INTEGER PRIMARY KEY AUTOINCREMENT,
        player_ign TEXT UNIQUE,
        discord_name TEXT,
        discord_id TEXT UNIQUE
    );
    """)

    # Create player_stats table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS player_stats (
        player_stats_id INTEGER PRIMARY KEY AUTOINCREMENT,
        match_id INTEGER,
        player_id INTEGER,
        team TEXT CHECK(team IN ('team1', 'team2')),
        champion TEXT,
        credits INTEGER,
        kills INTEGER,
        deaths INTEGER,
        assists INTEGER,
        damage INTEGER,
        taken INTEGER,
        objective_time INTEGER,
        shielding INTEGER,
        healing INTEGER,
        FOREIGN KEY (match_id) REFERENCES matches(match_id),
        FOREIGN KEY (player_id) REFERENCES players(player_id)
    );
    """)

    # Commit changes and close connection
    conn.commit()
    conn.close()


def insert_scoreboard(scoreboard):
    # Connect to SQLite database
    conn = sqlite3.connect("match_data.db")
    cursor = conn.cursor()

    try:
        # Extract match data
        match = scoreboard["match"]
        match_id = match.get("match_id")
        team1_score = match["team1_score"]
        team2_score = match["team2_score"]
        won = 1 if team1_score > team2_score else 0

        # Check if the match already exists
        cursor.execute("""
        SELECT 1 FROM matches WHERE match_id = ?;
        """, (match_id,))
        if cursor.fetchone():
            print(
                f"Warning: Match with match_id {match_id} already exists. Skipping insertion.")
            return

        # Insert match data
        cursor.execute("""
        INSERT INTO matches (match_id, time_minutes, region, map, team1_score, team2_score, won)
        VALUES (?, ?, ?, ?, ?, ?, ?);
        """, (match_id, match["time_minutes"], match["region"], match["map"], team1_score, team2_score, won))

        # Insert players and their stats for team1
        for player in scoreboard["teams"]["team1"]:
            # Check if player exists in the players table
            cursor.execute("""
            SELECT player_id FROM players WHERE player_ign = ?;
            """, (player["player"],))
            result = cursor.fetchone()

            if result:
                player_id = result[0]
                # Insert player stats
                cursor.execute("""
                INSERT INTO player_stats (match_id, player_id, team, champion, credits, kills, deaths, assists, damage, taken, objective_time, shielding, healing)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """, (match_id, player_id, "team1", player["champion"], player["credits"], player["kills"], player["deaths"],
                      player["assists"], player["damage"], player["taken"], player["objective_time"], player["shielding"], player["healing"]))
            else:
                print(f"Error: Player '{player['player']}' is not registered.")

        # Insert players and their stats for team2
        for player in scoreboard["teams"]["team2"]:
            # Check if player exists in the players table
            cursor.execute("""
            SELECT player_id FROM players WHERE player_ign = ?;
            """, (player["player"],))
            result = cursor.fetchone()

            if result:
                player_id = result[0]
                # Insert player stats
                cursor.execute("""
                INSERT INTO player_stats (match_id, player_id, team, champion, credits, kills, deaths, assists, damage, taken, objective_time, shielding, healing)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """, (match_id, player_id, "team2", player["champion"], player["credits"], player["kills"], player["deaths"],
                      player["assists"], player["damage"], player["taken"], player["objective_time"], player["shielding"], player["healing"]))
            else:
                print(f"Error: Player '{player['player']}' is not registered.")

        # Commit changes
        conn.commit()
        print(f"Scoreboard for match_id {match_id} inserted successfully.")

    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
        conn.rollback()

    finally:
        # Close the connection
        conn.close()


def register_player(player_ign, discord_name, discord_id):
    """
    Registers a new player by adding their IGN, Discord name, and Discord ID to the players table.
    """
    conn = sqlite3.connect("match_data.db")
    cursor = conn.cursor()

    try:
        cursor.execute("""
        INSERT INTO players (player_ign, discord_name, discord_id)
        VALUES (?, ?, ?);
        """, (player_ign, discord_name, discord_id))
        conn.commit()
        print(f"Player {player_ign} registered successfully.")

    except sqlite3.IntegrityError:
        print(
            f"Player with IGN '{player_ign}' or Discord ID '{discord_id}' already exists.")

    except sqlite3.Error as e:
        print(f"An error occurred: {e}")

    finally:
        conn.close()
