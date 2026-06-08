# run.py

import asyncio
import os
import re

import discord
import dotenv
from discord.ext import commands

from db import backfill_match_registered_at, create_database


dotenv.load_dotenv()

intents = discord.Intents.default()
intents.message_content = True
intents.members = True

bot = commands.Bot(command_prefix=["!"], intents=intents)
GUILD_ID = int(os.getenv("GUILD_ID"))

_startup_backfill_done = False
_startup_cogs_loaded = False


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


async def backfill_match_timestamps_task():
    try:
        match_registered_at = await collect_match_registered_at_from_match_results()
        updated = backfill_match_registered_at(match_registered_at)
        if updated:
            print(f"Backfilled registered_at for {updated} match rows.")
    except Exception as e:
        print(f"Failed to collect/backfill match timestamps from #match-results: {e}")


@bot.event
async def on_ready():
    global _startup_backfill_done, _startup_cogs_loaded
    print(f"Logged in as {bot.user}")

    try:
        create_database()
    except Exception as e:
        print(f"Failed to initialize database: {e}")
        return

    if not _startup_cogs_loaded:
        _startup_cogs_loaded = True
        cogs = ["cogs.admin", "cogs.general", "cogs.stats", "cogs.listeners"]
        for cog in cogs:
            try:
                await bot.load_extension(cog)
                print(f"Loaded {cog}")
            except Exception as e:
                print(f"Failed to load {cog}: {e}")

        try:
            guild = discord.Object(id=GUILD_ID)
            bot.tree.copy_global_to(guild=guild)
            synced = await bot.tree.sync(guild=guild)
            print(f"Synced {len(synced)} command(s) to guild")
        except Exception as e:
            print(f"Failed to sync commands: {e}")

    if not _startup_backfill_done:
        _startup_backfill_done = True
        asyncio.create_task(backfill_match_timestamps_task())


bot.run(os.getenv("BOT_TOKEN"))
