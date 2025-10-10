# cogs/listeners.py

import discord
from discord.ext import commands
import re
import csv
import io
from core.constants import ALLOWED_CHANNELS
from db import (
    match_exists,
    insert_scoreboard,
    get_registered_igns,
    insert_embed,
)
import easyocr
import tempfile
import os

def parse_match_textbox(text):
    """Parses match scoreboard text into structured data."""
    print("\n--- [DEBUG] Starting to parse match textbox ---")
    lines = text.splitlines()
    
    # Filter out all empty/whitespace-only lines to get a clean list of data
    data_lines = [line.strip() for line in lines if line.strip()]
    if not data_lines:
        raise ValueError("No data lines found in match text!")

    # --- Header Parsing ---
    header_line = data_lines[0]
    match_info = header_line.strip().split(",")
    if len(match_info) < 6:
        print(f"--- [ERROR] Malformed match info line. Line: '{header_line}'")
        raise ValueError("Malformed match info line!")
    try:
        match_id, time, region, map_name, team1_score, team2_score = [s.strip() for s in match_info[:6]]
        print(f"[DEBUG] Parsed Header: MatchID={match_id}, Score={team1_score}-{team2_score}, Map='{map_name}'")
    except Exception as e:
        print(f"--- [ERROR] Could not unpack match info line. Line: '{header_line}'. Error: {e}")
        raise ValueError("Could not unpack match info line.")

    # --- Player Parsing ---
    players = []
    # All lines after the header are considered player lines
    player_lines = data_lines[1:] 

    print(f"[DEBUG] Found {len(player_lines)} total player lines to process.")

    # Team 1 is the first 5 players. Team 2 is everyone after. This is robust.
    for i, line in enumerate(player_lines):
        current_team = 1 if i < 5 else 2
        
        print(f"\n[DEBUG] Processing line for Team {current_team}: '{line}'")

        if not line.startswith("[") or not line.endswith("]"):
            print(f"--- [ERROR] Line is missing brackets.")
            raise ValueError(f"Malformed player line: Missing brackets. Line: ```{line}```")
        
        try:
            csv_content = line.strip("[]")
            reader = csv.reader(io.StringIO(csv_content), skipinitialspace=True, quotechar="'")
            parts = next(reader)
            
            if len(parts) != 12:
                print(f"--- [ERROR] Incorrect field count. Expected 12, got {len(parts)}.")
                raise ValueError(f"Malformed player line: Expected 12 fields, but got {len(parts)}. Line: ```{line}```")

            del parts[3]
            kda_parts = parts[4].split("/")
            if len(kda_parts) != 3:
                raise ValueError(f"Malformed KDA in line. KDA: `{parts[4]}`")

            player = {
                "name": parts[0], "champ": parts[1], "talent": parts[2],
                "credits": int(parts[3].replace(",", "")), "kills": int(kda_parts[0]),
                "deaths": int(kda_parts[1]), "assists": int(kda_parts[2]),
                "damage": int(parts[5].replace(",", "")), "taken": int(parts[6].replace(",", "")),
                "obj_time": int(parts[7]), "shielding": int(parts[8].replace(",", "")),
                "healing": int(parts[9].replace(",", "")), "self_healing": int(parts[10].replace(",", "")),
                "team": current_team
            }
            players.append(player)
            print("  - Successfully parsed player stats.")

        except Exception as e:
            print(f"--- [FATAL ERROR] An unexpected error occurred while parsing this line: {e}")
            raise ValueError(f"An unexpected error occurred on a player line. Please check its format. Error: {e}. Line: ```{line}```")

    print(f"\n--- [DEBUG] Finished parsing. Total players found: {len(players)} ---")
    return {
        "match_id": int(match_id), "time": int(time), "region": region,
        "map": map_name, "team1_score": int(team1_score),
        "team2_score": int(team2_score), "players": players,
    }


class Listeners(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.reader = easyocr.Reader(['en'])

    async def scoreboard_ingestion(self, message):
        # --- FULLY AUTOMATED SCOREBOARD INGESTION ---
        if message.author.name == "PaladinsAssistant" and message.author.discriminator == "2894":

            # Step 1: Safely combine message parts to get the full raw text
            raw_text = ""
            if message.embeds:
                embed = message.embeds[0]
                parts = []
                if embed.title: parts.append(embed.title)
                if embed.description: parts.append(embed.description)
                raw_text = "\n".join(parts)
            else:
                raw_text = message.content

            if not raw_text.strip():
                return  # Ignore empty messages

            # Step 2: Intelligently find the start of the scoreboard data
            lines = raw_text.strip().split('\n')
            start_index = -1
            for i, line in enumerate(lines):
                if re.match(r'^\s*\d{9,12}\s*,', line.strip()):
                    start_index = i
                    break

            if start_index == -1:
                return

            raw_scoreboard_text = "\n".join(lines[start_index:])
            cleaned_text = raw_scoreboard_text.strip().strip("`")

            # Step 3: Parse the cleaned text into structured data
            try:
                match_data = parse_match_textbox(cleaned_text)
                match_id = match_data["match_id"]
            except ValueError as e:
                await message.channel.send(f"⚠️ **Could not parse scoreboard.**\n**Reason:** {e}")
                return

            # Step 4: Perform Safety Checks
            # Check 1: Prevent duplicate matches
            if match_exists(match_id):
                await message.channel.send(f"⚠️ Match `{match_id}` has already been recorded.")
                return  # Stop processing to avoid duplicates

            # --- MODIFIED BEHAVIOR FOR UNLINKED PLAYERS ---
            # Check 2: Identify any unlinked players to issue a non-blocking warning.
            ign_list = [player["name"] for player in match_data["players"]]
            _, igns_not_registered = get_registered_igns(ign_list)

            # If players are not registered, send a warning but DO NOT stop the process.
            if igns_not_registered:
                unlinked_players = ", ".join(f"`{ign}`" for ign in igns_not_registered)
                warning_msg = (
                    f"⚠️ **Warning for Match `{match_id}`:** The following players' stats have been recorded "
                    f"but are not yet linked to a Discord account. They should use `!link <ign>` to claim their stats:\n"
                    f"▶️ {unlinked_players}"
                )
                await message.channel.send(warning_msg)
            # --- END OF MODIFIED BEHAVIOR ---

            # Step 5: Insert the data into the database regardless of link status
            insert_scoreboard(match_data, match_id)

            # Step 6: Send a public confirmation message
            await message.channel.send(f"✅ **Match `{match_id}` successfully recorded.**")
            print(f"Successfully ingested match {match_id}.")

        # --- This part saves NeatQueue embeds for player verification ---
        elif message.author.name == "NeatQueue" and message.author.discriminator == "0850" and message.embeds:
            for embed in message.embeds:
                queue_number_match = re.search(r"Queue #?(\d+)", embed.title or "") or re.search(r"Queue #?(\d+)",
                                                                                                 embed.description or "")
                if queue_number_match:
                    queue_number = queue_number_match.group(1)
                    insert_embed(queue_number, embed.to_dict())

    def get_match_id(self, img):
        # --- (HELPER) OCR IMAGE PROCESSING ---

        # Read and store text from the image
        results = self.reader.readtext(img)
        for _, text, prob in results:

            # Search for the match id ('ID ' and ten digits)
            found_id = re.search(r'ID\s\d{10}', text)
            if found_id:

                # Return the match id or None if not found
                match_id = int(text[3:13])
                return match_id

        return None

    async def match_results_id_ocr(self, message):
        # --- AUTOMATED MATCH ID PROCESSING ---
        if message.author == self.bot.user or message.channel.name != "match-results":
            return

        if message.attachments:
            for attachment in message.attachments:

                # Temporarily save attachments with the following file extensions
                if attachment.filename.lower().endswith(('.png', '.jpg', '.jpeg')):
                    extension = os.path.splitext(attachment.filename)[1]
                    img_path = f"temp_{attachment.id}{extension}"
                    try:
                        await attachment.save(img_path)

                        # Attempt to extract the match id from the image
                        match_id = self.get_match_id(img_path)

                        # Send the match id in the chat if it was successfully extracted
                        if match_id:
                            await message.channel.send(f"Match ID: {match_id}")
                        else:
                            await message.channel.send("Match ID not found.")
                    finally:
                        if os.path.exists(img_path):
                            os.remove(img_path)

    @commands.Cog.listener()
    async def on_message(self, message):
        # Ignore messages from itself and other bots we don't care about
        if message.author.bot and message.author.name not in ["PaladinsAssistant", "NeatQueue"]:
            return
        
        try:
            if isinstance(message.channel, discord.TextChannel) and message.channel.name in ALLOWED_CHANNELS:
                await self.scoreboard_ingestion(message)
                await self.match_results_id_ocr(message)

        except Exception as e:
            print(f"Error in on_message processing: {e}")

    @commands.Cog.listener()
    async def on_command_error(self, ctx, error):
        if isinstance(error, commands.CheckFailure):
            await ctx.send("You do not have the required permissions to run this command.")
        elif isinstance(error, commands.MissingRequiredArgument):
            await ctx.send(f"Missing argument: `{error.param.name}`. Use `!help {ctx.command.name}` for details.")
        elif isinstance(error, commands.BadArgument):
            await ctx.send(f"Invalid argument provided. {error}")
        else:
            print(f"An unhandled error occurred: {error}")
            await ctx.send("An unexpected error occurred. Please contact an administrator.")


async def setup(bot):
    await bot.add_cog(Listeners(bot))

