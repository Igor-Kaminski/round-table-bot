# run.py

import discord
from discord.ext import commands
import os
import dotenv
import re
from db import create_database

# Load environment variables
dotenv.load_dotenv()

# Bot configuration
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
bot = commands.Bot(command_prefix=["!"], intents=intents)
_startup_backfill_done = False

GUILD_ID = int(os.getenv("GUILD_ID"))


async def collect_match_registered_at_from_match_results():
    guild = bot.get_guild(GUILD_ID)
    if guild is None:
        print("Could not collect match timestamps: guild not found.")
        return {}

    channel = discord.utils.get(guild.text_channels, name="match-results")
    if channel is None:
        print("Could not collect match timestamps: #match-results channel not found.")
        return {}

    match_timestamps = {}
    pattern = re.compile(r">>\s*match_data\s+(\d{9,12})", re.IGNORECASE)
    scanned = 0

    async for message in channel.history(limit=None, oldest_first=True):
        scanned += 1
        for match in pattern.finditer(message.content or ""):
            match_id = int(match.group(1))
            match_timestamps.setdefault(match_id, int(message.created_at.timestamp()))

    print(
        "Collected match timestamps from #match-results: "
        f"scanned {scanned} messages, found {len(match_timestamps)} match_data commands."
    )
    return match_timestamps


@bot.event
async def on_ready():
    global _startup_backfill_done
    print(f"Logged in as {bot.user}")

    match_registered_at = {}
    if not _startup_backfill_done:
        _startup_backfill_done = True
        try:
            match_registered_at = await collect_match_registered_at_from_match_results()
        except Exception as e:
            print(f"Failed to collect match timestamps from #match-results: {e}")

    try:
        create_database(match_registered_at)
    except Exception as e:
        print(f"Failed to initialize database: {e}")
        return
    
    # Load all cogs
    cogs = ["cogs.admin", "cogs.general", "cogs.stats", "cogs.listeners"]
    for cog in cogs:
        try:
            await bot.load_extension(cog)
            print(f"✅ Loaded {cog}")
        except Exception as e:
            print(f"❌ Failed to load {cog}: {e}")
    
    # Sync slash commands
    try:
        guild = discord.Object(id=GUILD_ID)
        bot.tree.copy_global_to(guild=guild)
        synced = await bot.tree.sync(guild=guild)
        print(f"Synced {len(synced)} command(s) to guild")
    except Exception as e:
        print(f"Failed to sync commands: {e}")


# Run the bot
bot.run(os.getenv("BOT_TOKEN"))
