import sqlite3
import json
import unicodedata
from db import insert_scoreboard, register_player, create_database


def normalize_string(input_string):
    """
    Normalize a string to ensure consistent Unicode representation.
    """
    return unicodedata.normalize("NFC", input_string)


def insert_scoreboard_from_file(json_file_path):
    """
    Reads a JSON file and inserts the scoreboard data into the database.
    """
    try:
        with open(json_file_path, 'r', encoding='utf-8') as file:
            scoreboard = json.load(file)
            # Normalize player names in the scoreboard
            for team in scoreboard["teams"].values():
                for player in team:
                    player["player"] = normalize_string(player["player"])
            insert_scoreboard(scoreboard)
            print(f"Scoreboard from {json_file_path} inserted successfully.")
    except FileNotFoundError:
        print(f"File not found: {json_file_path}")
    except json.JSONDecodeError:
        print(f"Invalid JSON format in file: {json_file_path}")
    except Exception as e:
        print(f"An error occurred: {e}")


def register_player(player_ign, discord_name, discord_id):
    """
    Registers a new player by adding their IGN, Discord name, and Discord ID to the players table.
    """
    player_ign = normalize_string(player_ign)  # Normalize the player IGN
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


def get_player_winrate(player_ign):
    """
    Calculates and returns the winrate of a given player based on their IGN.
    """
    player_ign = normalize_string(player_ign)  # Normalize the player IGN
    conn = sqlite3.connect("match_data.db")
    cursor = conn.cursor()

    try:
        # Query to count wins
        cursor.execute("""
        SELECT COUNT(*) FROM matches
        JOIN player_stats ON matches.match_id = player_stats.match_id
        JOIN players ON player_stats.player_id = players.player_id
        WHERE players.player_ign = ? AND (
            (player_stats.team = 'team1' AND matches.team1_score > matches.team2_score) OR
            (player_stats.team = 'team2' AND matches.team2_score > matches.team1_score)
        );
        """, (player_ign,))
        wins = cursor.fetchone()[0]

        # Query to count total matches
        cursor.execute("""
        SELECT COUNT(*) FROM matches
        JOIN player_stats ON matches.match_id = player_stats.match_id
        JOIN players ON player_stats.player_id = players.player_id
        WHERE players.player_ign = ?;
        """, (player_ign,))
        total_matches = cursor.fetchone()[0]

        if total_matches == 0:
            print(f"No matches found for player: {player_ign}")
            return 0.0

        winrate = (wins / total_matches) * 100
        print(f"Player: {player_ign}, Winrate: {winrate:.2f}%")
        return winrate

    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
        return 0.0

    finally:
        conn.close()


def get_top_champions_with_winrate(player_ign):
    """
    Lists the top 5 most played champions by a player based on frequency and their winrate per champion.
    """
    player_ign = normalize_string(player_ign)  # Normalize the player IGN
    conn = sqlite3.connect("match_data.db")
    cursor = conn.cursor()

    try:
        # Query to get the top 5 most played champions with their winrate
        cursor.execute("""
        SELECT 
            player_stats.champion,
            COUNT(player_stats.champion) AS frequency,
            SUM(CASE 
                WHEN (player_stats.team = 'team1' AND matches.team1_score > matches.team2_score) OR
                     (player_stats.team = 'team2' AND matches.team2_score > matches.team1_score)
                THEN 1 ELSE 0 END) AS wins
        FROM player_stats
        JOIN players ON player_stats.player_id = players.player_id
        JOIN matches ON player_stats.match_id = matches.match_id
        WHERE players.player_ign = ?
        GROUP BY player_stats.champion
        ORDER BY frequency DESC
        LIMIT 5;
        """, (player_ign,))
        champions = cursor.fetchall()

        if not champions:
            print(f"No champions found for player: {player_ign}")
        else:
            print(f"Top champions for {player_ign}:")
            for champion, frequency, wins in champions:
                winrate = (wins / frequency) * 100 if frequency > 0 else 0
                print(
                    f"- {champion}: {frequency} matches, Winrate: {winrate:.2f}%")

    except sqlite3.Error as e:
        print(f"An error occurred: {e}")

    finally:
        conn.close()


# Example usage
if __name__ == "__main__":
    create_database()

    # Example 1: Register players first
    register_player("ComradeNick", "Nick#1234", "123456789012345678")
    register_player("AyeltśLëvyy", "Levy#1234", "023456789012345678")

    # Example 2: Insert scoreboards from JSON files
    json_file_path = "parsed_scoreboard1.json"
    insert_scoreboard_from_file(json_file_path)

    json_file_path2 = "parsed_scoreboard2.json"
    insert_scoreboard_from_file(json_file_path2)

    # Example 3: Get the player's winrate
    get_player_winrate("ComradeNick")
    get_player_winrate("AyeltśLëvyy")

    # Example 4: Get the top 5 most played champions with winrate for each player
    get_top_champions_with_winrate("ComradeNick")
    get_top_champions_with_winrate("AyeltśLëvyy")
