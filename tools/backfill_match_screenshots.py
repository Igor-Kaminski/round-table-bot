import argparse
import asyncio
import os
import sys
import tempfile

import discord
import dotenv


ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from cogs.listeners import Listeners
from core.constants import ALLOWED_CHANNELS
from db import create_database, get_match_screenshot, link_match_screenshot
from utils.match_screenshots import (
    attachment_is_supported,
    move_screenshot_file,
    remove_screenshot_file,
    resolve_screenshot_path,
    screenshot_extension,
)


async def _collect_candidate_messages(client, guild_id, channel_names, history_limit):
    guilds = [client.get_guild(guild_id)] if guild_id else list(client.guilds)
    allowed_names = {name.strip() for name in channel_names if name.strip()}
    candidates = []
    for guild in filter(None, guilds):
        for channel in guild.text_channels:
            if channel.name not in allowed_names:
                continue
            try:
                async for message in channel.history(limit=history_limit):
                    if any(attachment_is_supported(item) for item in message.attachments):
                        candidates.append(message)
            except discord.Forbidden:
                print(f"Skipping #{channel.name}: missing history permission.")
            except discord.HTTPException as exc:
                print(f"Skipping #{channel.name}: {exc}")

    candidates.sort(key=lambda message: message.created_at, reverse=True)
    return candidates


async def _backfill(client, args):
    create_database()
    ocr = Listeners(bot=None)
    history_limit = None if args.history_limit <= 0 else args.history_limit
    messages = await _collect_candidate_messages(
        client,
        args.guild_id,
        args.channels or ALLOWED_CHANNELS,
        history_limit,
    )

    linked = []
    skipped_existing = 0
    scanned_images = 0
    for message in messages:
        if len(linked) >= args.limit:
            break

        for attachment in message.attachments:
            if len(linked) >= args.limit:
                break
            if not attachment_is_supported(attachment):
                continue

            scanned_images += 1
            extension = screenshot_extension(attachment.filename)
            fd, temp_path = tempfile.mkstemp(suffix=extension)
            os.close(fd)
            new_path = None
            try:
                await attachment.save(temp_path)
                match_id = ocr.get_match_id(temp_path)
                if not match_id:
                    continue

                existing = get_match_screenshot(match_id)
                existing_path = resolve_screenshot_path(existing["file_path"]) if existing else None
                if existing_path and existing_path.exists() and not args.overwrite:
                    skipped_existing += 1
                    continue

                new_path = move_screenshot_file(temp_path, match_id, attachment.id, extension)
                temp_path = None
                if link_match_screenshot(
                    match_id,
                    new_path,
                    source_url=attachment.url,
                    message_id=message.id,
                    attachment_id=attachment.id,
                    channel_id=message.channel.id,
                    created_at=int(message.created_at.timestamp()),
                ):
                    if existing and existing["file_path"] != new_path:
                        remove_screenshot_file(existing["file_path"])
                    linked.append((match_id, new_path))
                else:
                    remove_screenshot_file(new_path)
            except (OSError, ValueError) as exc:
                if new_path:
                    remove_screenshot_file(new_path)
                print(f"Skipping attachment {attachment.id}: {exc}")
            finally:
                if temp_path and os.path.exists(temp_path):
                    os.remove(temp_path)

    print(f"Scanned image attachments: {scanned_images}")
    print(f"Linked screenshots: {len(linked)}")
    print(f"Already linked: {skipped_existing}")
    for match_id, path in linked:
        print(f"!match {match_id} -> {path}")


async def main():
    dotenv.load_dotenv(os.path.join(ROOT_DIR, ".env"))
    parser = argparse.ArgumentParser(description="Backfill locally saved OCR screenshots from Discord history.")
    parser.add_argument("--limit", type=int, default=10, help="Maximum screenshots to link.")
    parser.add_argument("--history-limit", type=int, default=500, help="Messages per channel; use 0 for all history.")
    parser.add_argument("--guild-id", type=int, default=int(os.getenv("GUILD_ID") or 0) or None)
    parser.add_argument("--channels", nargs="*", default=None, help="Channel names; defaults to result/admin channels.")
    parser.add_argument("--overwrite", action="store_true", help="Replace screenshots that are already stored.")
    args = parser.parse_args()

    if args.limit <= 0:
        raise ValueError("--limit must be greater than zero.")

    token = os.getenv("BOT_TOKEN")
    if not token:
        raise RuntimeError("BOT_TOKEN is required in .env or the environment.")

    intents = discord.Intents.default()
    intents.message_content = True
    client = discord.Client(intents=intents)

    @client.event
    async def on_ready():
        try:
            await _backfill(client, args)
        finally:
            await client.close()

    await client.start(token)


if __name__ == "__main__":
    asyncio.run(main())
