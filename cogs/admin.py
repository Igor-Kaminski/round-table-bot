# cogs/admin.py

import discord
from discord.ext import commands
import io
import re
from utils.checks import is_exec
from core.constants import ALLOWED_CHANNELS
from db import (
    update_discord_id,
    execute_select_query,
    insert_embed,
    add_alt_ign,
    get_alt_igns,
    delete_alt_ign,
    get_player_id,
    get_old_stats,
    match_exists,
    queue_exists,
    insert_scoreboard,
    delete_match,
)


class Admin(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @commands.command(name="link_disc", help="Update a player's Discord ID. Only for Executives.")
    @commands.check(is_exec)
    async def link_disc(self, ctx, old_id: str, new_id: str):
        try:
            update_discord_id(old_id, new_id)
            await ctx.send(f"Successfully updated Discord ID from {old_id} to {new_id}.")
        except Exception as e:
            print(f"Error in link_disc command: {e}")
            await ctx.send(f"An error occurred while updating the Discord ID: {e}")

    @commands.command(name="query", help="Execute a SELECT SQL query. Only for Executives.")
    @commands.check(is_exec)
    async def query(self, ctx, *, sql_query: str):
        try:
            results = execute_select_query(sql_query)
            if results:
                formatted_results = "\n".join([str(row) for row in results])
                if len(formatted_results) > 1900:
                    await ctx.send("Query Results:", file=discord.File(io.StringIO(formatted_results), "results.txt"))
                else:
                    await ctx.send(f"```\n{formatted_results}\n```")
            else:
                await ctx.send("No results found.")
        except Exception as e:
            print(f"Error in query command: {e}")
            await ctx.send(f"An error occurred while executing the query: {e}")

    @commands.command(name="fetch_embeds", help="Fetch messages and store embeds in the database.")
    @commands.check(is_exec)
    async def fetch_embeds(self, ctx):
        if ctx.channel.name not in ALLOWED_CHANNELS:
            await ctx.send(f"This command can only be used in the {', '.join(['#' + ch for ch in ALLOWED_CHANNELS])} channels.")
            return
        try:
            count = 0
            async for message in ctx.channel.history(limit=None):
                if (message.author.name == "NeatQueue" and message.author.discriminator == "0850" and message.embeds):
                    for embed in message.embeds:
                        queue_number = None
                        if embed.title and "Queue" in embed.title:
                            queue_number = embed.title.split("Queue")[-1].strip()
                        elif embed.description and "Queue" in embed.description:
                            queue_number = embed.description.split("Queue")[-1].strip()
                        if queue_number:
                            embed_data = embed.to_dict()
                            insert_embed(queue_number, embed_data)
                            count += 1
            await ctx.send(f"Successfully fetched and stored {count} embeds in the database.")
        except Exception as e:
            print(f"Error in fetch_embeds command: {e}")
            await ctx.send("An error occurred while fetching embeds.")

    @commands.command(name="add_alt", help="Add an alternate IGN for a player. Execs only.")
    @commands.check(is_exec)
    async def add_alt_ign_cmd(self, ctx, user: discord.Member, alt_ign: str):
        success = add_alt_ign(str(user.id), alt_ign)
        if success:
            await ctx.send(f"Added alt IGN `{alt_ign}` for {user.mention}.")
        else:
            await ctx.send(f"Failed to add alt IGN `{alt_ign}` for {user.mention}. It may already exist.")

    @commands.command(name="show_alts", help="Show all alternate IGNs for a player. Execs only.")
    @commands.check(is_exec)
    async def show_alt_igns_cmd(self, ctx, user: discord.Member):
        alt_igns = get_alt_igns(str(user.id))
        if alt_igns:
            await ctx.send(f"Alternate IGNs for {user.mention}: `{', '.join(alt_igns)}`")
        else:
            await ctx.send(f"No alternate IGNs found for {user.mention}.")

    @commands.command(name="delete_alt", help="Delete an alternate IGN for a player. Execs only.")
    @commands.check(is_exec)
    async def delete_alt_ign_cmd(self, ctx, user: discord.Member, alt_ign: str):
        success = delete_alt_ign(str(user.id), alt_ign)
        if success:
            await ctx.send(f"Deleted alt IGN `{alt_ign}` for {user.mention}.")
        else:
            await ctx.send(f"Failed to delete alt IGN `{alt_ign}` for {user.mention}. It may not exist.")

    @commands.command(name="player_id", help="Get player_id for a Discord ID. Execs only.")
    @commands.check(is_exec)
    async def player_id_cmd(self, ctx, user: discord.Member):
        pid = get_player_id(str(user.id))
        if pid:
            await ctx.send(f"player_id for {user.display_name}: `{pid}`")
        else:
            await ctx.send(f"No player found for {user.display_name}.")

    @commands.command(name="old_stats", help="[LEGACY] Get raw stats for a Discord ID. Execs only.")
    @commands.check(is_exec)
    async def old_stats_cmd(self, ctx, discord_id: str):
        match = re.search(r"\d{15,20}", discord_id)
        if match:
            discord_id = match.group(0)
        
        try:
            member = await self.bot.fetch_user(int(discord_id))
            name = member.display_name
        except (discord.NotFound, ValueError):
            name = f"ID: {discord_id}"

        player_id = get_player_id(discord_id)
        if not player_id:
            await ctx.send(f"No player found for {name}.")
            return

        stats = get_old_stats(player_id)
        if not stats:
            await ctx.send(f"No stats found for {name}.")
            return
        
        msg = (
            f"**Stats for {name} (player_id {player_id}):**\n"
            f"Kills/min: {stats['kills']}, Deaths/min: {stats['deaths']}, Assists/min: {stats['assists']}\n"
            f"Damage/min: {stats['damage']}, Obj/min: {stats['objective_time']}, Shield/min: {stats['shielding']}, Heal/min: {stats['healing']}\n"
            f"Games: {stats['games']}"
        )
        await ctx.send(msg)

    @commands.command(name="ingest_text", help="Parse and insert a scoreboard from text. Execs only.")
    @commands.check(is_exec)
    async def ingest_text_cmd(self, ctx, queue_num: str, *, scoreboard_text: str = None):
        try:
            # Import the parsing function from listeners
            from cogs.listeners import parse_match_textbox
            
            # --- Step 1: Gather the text from arguments or a reply ---
            text = scoreboard_text
            if not text and ctx.message.reference:
                ref_msg = await ctx.channel.fetch_message(ctx.message.reference.message_id)
                if ref_msg:
                    text = ref_msg.content or (ref_msg.embeds[0].description if ref_msg.embeds else None)
            
            # --- Step 2: Restore the "forgiving format" logic from your old code ---
            raw_queue_str = queue_num.strip()
            if "," in raw_queue_str or not raw_queue_str.isdigit():
                # This handles when the user pastes the header right after the command.
                # It merges the misplaced header back into the main text block.
                text = (raw_queue_str + (" " + text if text else "")).strip()
                # It clears the queue string so we can default to the match_id later.
                raw_queue_str = None
                
            # --- Step 3: Proceed with parsing ---
            if not text:
                await ctx.send("No scoreboard text provided. Paste it after the command or reply to a message containing it.")
                return

            cleaned_text = text.strip().strip("`")
            match_data = parse_match_textbox(cleaned_text)
            match_id = match_data["match_id"]

            # --- Step 4: Intelligently decide the queue number ---
            if raw_queue_str and raw_queue_str.isdigit():
                # Use the number the user provided if it was a valid, single number.
                queue_value = int(raw_queue_str)
            else:
                # Otherwise, default to using the Match ID as the queue number.
                queue_value = int(match_id)

            # --- Step 5: Run safety checks and insert the data ---
            if match_exists(match_id):
                await ctx.send(f"Match ID {match_id} already exists in the database.")
                return

            if queue_exists(queue_value):
                await ctx.send(f"Queue number {queue_value} already exists in the database.")
                return

            insert_scoreboard(match_data, queue_value)
            await ctx.send(f"Match {match_id} for queue {queue_value} successfully recorded.")
            
        except ValueError as ve:
            await ctx.send(f"Malformed match data: {ve}")
        except Exception as e:
            print(f"Error in ingest_text: {e}")
            await ctx.send(f"Error processing match data: {e}")

    @commands.command(name="delete_match", help="Permanently delete a match by its ID. Execs only.")
    @commands.check(is_exec)
    async def delete_match_cmd(self, ctx, match_id: int):
        try:
            # Call the database function
            deleted_rows_count = delete_match(match_id)

            if deleted_rows_count > 0:
                await ctx.send(f"✅ Successfully deleted Match ID `{match_id}` and its associated data. "
                             f"({deleted_rows_count} total records removed).")
            else:
                await ctx.send(f"⚠️ Match ID `{match_id}` could not be found in the database.")
                
        except ValueError:
            await ctx.send("❌ Invalid Match ID. Please provide a number.")
        except Exception as e:
            print(f"Error in delete_match command: {e}")
            await ctx.send(f"An unexpected error occurred while trying to delete Match ID `{match_id}`.")


async def setup(bot):
    await bot.add_cog(Admin(bot))

