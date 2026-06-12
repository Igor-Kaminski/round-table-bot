# run.py

import asyncio
import os
import re
import sqlite3

import discord
import dotenv
from discord.ext import commands

from db import backfill_match_registered_at, create_database


dotenv.load_dotenv()

intents = discord.Intents.default()
intents.message_content = True
intents.members = True

bot = commands.Bot(command_prefix=["!"], intents=intents)
bot.remove_command("help")
GUILD_ID = int(os.getenv("GUILD_ID"))

_startup_backfill_done = False
_startup_cogs_loaded = False


def get_missing_registered_match_ids():
    conn = sqlite3.connect("match_data.db")
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT match_id FROM matches WHERE registered_at IS NULL;")
        return {int(row[0]) for row in cursor.fetchall()}
    finally:
        conn.close()


def extract_match_timestamps_from_message(message, command_pattern, scoreboard_pattern):
    parts = [message.content or ""]
    for embed in message.embeds:
        if embed.title:
            parts.append(embed.title)
        if embed.description:
            parts.append(embed.description)
    text = "\n".join(parts)

    match_timestamps = {}
    for pattern in (command_pattern, scoreboard_pattern):
        for match in pattern.finditer(text):
            match_id = int(match.group(1))
            match_timestamps.setdefault(match_id, int(message.created_at.timestamp()))
    return match_timestamps


async def collect_match_registered_at_from_match_results():
    guild = bot.get_guild(GUILD_ID)
    if guild is None:
        print("Could not collect match timestamps: guild not found.")
        return {}

    channel_names = ["match-results", "boss-matchresults", "admin"]
    channels = [channel for name in channel_names if (channel := discord.utils.get(guild.text_channels, name=name))]
    if not channels:
        print("Could not collect match timestamps: no match timestamp channels found.")
        return {}

    missing_match_ids = get_missing_registered_match_ids()
    if not missing_match_ids:
        print("No missing match timestamps to backfill.")
        return {}

    match_timestamps = {}
    command_pattern = re.compile(r">>\s*match_data\s+(\d{9,12})", re.IGNORECASE)
    scoreboard_pattern = re.compile(r"^\s*(\d{9,12})\s*,", re.MULTILINE)
    scanned_by_channel = {}

    for channel in channels:
        if channel.name == "admin" and not missing_match_ids:
            break

        scanned = 0
        async for message in channel.history(limit=None, oldest_first=True):
            scanned += 1
            for match_id, registered_at in extract_match_timestamps_from_message(
                message, command_pattern, scoreboard_pattern
            ).items():
                match_timestamps.setdefault(match_id, registered_at)
                missing_match_ids.discard(match_id)

            if channel.name == "admin" and not missing_match_ids:
                break

        scanned_by_channel[channel.name] = scanned

    print(
        "Collected match timestamps: "
        f"scanned {scanned_by_channel}, found {len(match_timestamps)} match timestamps."
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
