# ROUND TABLE BOT

A Discord bot for tracking and analyzing Paladins match statistics with a modular cog-based architecture.

## Project Structure

```
Paladins/
├── run.py                 # Main entry point - loads cogs and starts bot
├── db.py                  # Database functions and queries
├── match_data.db          # SQLite database (auto-generated)
├── requirements.txt       # Python dependencies
├── .env                   # Environment variables (you need to create this)
├── core/
│   ├── __init__.py
│   └── constants.py       # Static data (champion roles, aliases, etc.)
├── utils/
│   ├── __init__.py
│   ├── checks.py          # Permission checking functions
│   ├── converters.py      # Custom argument converters
│   └── views.py           # Discord UI components (buttons, modals, etc.)
└── cogs/
    ├── admin.py           # Admin-only commands
    ├── general.py         # Public commands (link, etc.)
    ├── stats.py           # Statistics commands
    └── listeners.py       # Event listeners (on_message, etc.)
```

## Requirements

* Python 3.8+
* Bot Token from the Discord Developer portal
* EasyOCR for text recognition capabilities

## Setup & Configuration

1.  **Dependencies:** Install the required Python libraries using the requirements file:
    ```bash
    pip install -r requirements.txt
    ```

2.  **Environment File:** Create a file named `.env` in the project root directory. This file will store your secret keys. Add the following lines, replacing the placeholder values with your own:
    ```env
    BOT_TOKEN=YOUR_DISCORD_BOT_TOKEN_HERE
    GUILD_ID=YOUR_SERVER_ID_HERE
    ```

## Running the Bot

Once the setup is complete, you can start the bot by running the main Python script from your terminal:
```bash
python run.py
```

The bot will automatically:
- Initialize the database
- Load all cogs (admin, general, stats, listeners)
- Sync slash commands to your guild
- Connect to Discord

## Features

### For All Users
Prefix commands (`!`) and slash commands (`/`) are both supported.

- `!link <ign>` / `/link` - Link your Discord account to your in-game name
- `!stats [@user] [champion/role] [filters]` / `/stats` - View player statistics
- `!top [@user]` / `/top` - Interactive champion breakdown
- `!history [@user] [limit] [filters]` / `/history` - View recent match history
- `!leaderboard [stat] [filters]` / `/leaderboard` - View server player rankings
- `!champ_lb [stat] [role] [filters]` / `/champ_lb` - View champion rankings (all players combined)
- `!compare @user1 [@user2]` / `/compare` - Compare two players

### Filters
- Time: `last 3d`, `last 7d`, `last 14d`, `last 30d`, `since YYYY-MM-DD`, or `from YYYY-MM-DD to YYYY-MM-DD`
- Match: `map <name>`, `wins`, `losses`, `team1`, `team2`, `4-3`, `close`, `stomp`, `sweep`
- Players: `with <player>` or `against <player>`

Time filters use when the bot recorded the match. Older matches from before this update may not have a recorded timestamp. Example: `!lb wr support last 7d map jaguar falls`.

Incomplete match data from Hi-Rez/PaladinsAssistant is saved with `player_count` and `is_complete` metadata for audit/debugging. It still counts in normal stats and W/L calculations, but team-total stats can be less reliable when player rows are missing.

### For Admins
- `!ingest_text` - Manually add match data
- `!delete_match <id>` - Remove a match from the database
- `!add_alt @user <ign>` - Add alternate IGN for a player
- `!query <sql>` - Execute database queries
- And more...
