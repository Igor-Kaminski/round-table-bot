import discord
from discord.ext import commands
import os
import dotenv
import re
from discord.ui import View, Button, Modal, TextInput
from db import (
    create_database,
    insert_scoreboard,
    update_discord_id,
    execute_select_query,
    insert_embed,
    read_embeds,
    verify_registered_users,
    match_exists,
    queue_exists,
    get_registered_igns,
    link_ign,
    add_alt_ign,
    get_ign_link_info,
    get_ign_for_discord_id,
    get_alt_igns,
    delete_alt_ign,
    get_player_id,
    # NEW/UPDATED IMPORTS
    get_player_stats,
    get_top_champs,
    get_winrate_with_against,
    compare_players,
    get_match_history,
    get_leaderboard,
    get_old_stats,
)
import csv
import io

dotenv.load_dotenv()
create_database()

intents = discord.Intents.default()
intents.message_content = True
intents.members = True
bot = commands.Bot(command_prefix=["!"], intents=intents)

GUILD_ID = int(os.getenv("GUILD_ID"))
BOT_PERMISSION_ROLE_NAMES = ["Executive", "Bot Access"]
BOT_PERMISSION_USER_IDS = [163861584379248651]  # Nick
ALLOWED_CHANNels = ["match-results", "admin"]  # Define allowed channels


@bot.event
async def on_ready():
    print(f"Logged in as {bot.user}")
    try:
        synced = await bot.tree.sync(guild=discord.Object(id=GUILD_ID))
        print(f"Synced {len(synced)} command(s) to guild")
    except Exception as e:
        print(f"Failed to sync commands or load cog: {e}")


def is_exec(ctx_or_message):
    author = getattr(ctx_or_message, "author", None)
    if author is None:
        return False
    if hasattr(author, "roles"):
        if any(role.name in BOT_PERMISSION_ROLE_NAMES for role in author.roles):
            return True
    if author.id in BOT_PERMISSION_USER_IDS:
        return True
    return False


# NEW: Reusable converter for parsing user arguments (me, @mention, ID)
class PlayerConverter(commands.Converter):
    async def convert(self, ctx, argument):
        if argument.lower() == 'me':
            return ctx.author
        try:
            return await commands.MemberConverter().convert(ctx, argument)
        except commands.MemberNotFound:
            # Check if it's a numeric ID for a user not in the server
            if argument.isdigit():
                try:
                    return await bot.fetch_user(int(argument))
                except discord.NotFound:
                     raise commands.BadArgument(f"User with ID `{argument}` not found.")
            raise commands.BadArgument(f"User `{argument}` not found.")

# --- ADMIN COMMANDS ---

@bot.command(name="link_disc", help="Update a player's Discord ID. Only for Executives.")
@commands.check(is_exec)
async def link_disc(ctx, old_id: str, new_id: str):
    try:
        update_discord_id(old_id, new_id)
        await ctx.send(f"Successfully updated Discord ID from {old_id} to {new_id}.")
    except Exception as e:
        print(f"Error in link_disc command: {e}")
        await ctx.send(f"An error occurred while updating the Discord ID: {e}")

@bot.command(name="query", help="Execute a SELECT SQL query. Only for Executives.")
@commands.check(is_exec)
async def query(ctx, *, sql_query: str):
    try:
        results = execute_select_query(sql_query)
        if results:
            formatted_results = "\n".join([str(row) for row in results])
            # Handle long messages
            if len(formatted_results) > 1900:
                await ctx.send("Query Results:", file=discord.File(io.StringIO(formatted_results), "results.txt"))
            else:
                await ctx.send(f"```\n{formatted_results}\n```")
        else:
            await ctx.send("No results found.")
    except Exception as e:
        print(f"Error in query command: {e}")
        await ctx.send(f"An error occurred while executing the query: {e}")

@bot.command(name="fetch_embeds", help="Fetch messages and store embeds in the database.")
@commands.check(is_exec)
async def fetch_embeds(ctx):
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

@bot.command(name="add_alt", help="Add an alternate IGN for a player. Execs only.")
@commands.check(is_exec)
async def add_alt_ign_cmd(ctx, user: discord.Member, alt_ign: str):
    success = add_alt_ign(str(user.id), alt_ign)
    if success:
        await ctx.send(f"Added alt IGN `{alt_ign}` for {user.mention}.")
    else:
        await ctx.send(f"Failed to add alt IGN `{alt_ign}` for {user.mention}. It may already exist.")

@bot.command(name="show_alts", help="Show all alternate IGNs for a player. Execs only.")
@commands.check(is_exec)
async def show_alt_igns_cmd(ctx, user: discord.Member):
    alt_igns = get_alt_igns(str(user.id))
    if alt_igns:
        await ctx.send(f"Alternate IGNs for {user.mention}: `{', '.join(alt_igns)}`")
    else:
        await ctx.send(f"No alternate IGNs found for {user.mention}.")

@bot.command(name="delete_alt", help="Delete an alternate IGN for a player. Execs only.")
@commands.check(is_exec)
async def delete_alt_ign_cmd(ctx, user: discord.Member, alt_ign: str):
    success = delete_alt_ign(str(user.id), alt_ign)
    if success:
        await ctx.send(f"Deleted alt IGN `{alt_ign}` for {user.mention}.")
    else:
        await ctx.send(f"Failed to delete alt IGN `{alt_ign}` for {user.mention}. It may not exist.")

@bot.command(name="player_id", help="Get player_id for a Discord ID. Execs only.")
@commands.check(is_exec)
async def player_id_cmd(ctx, user: discord.Member):
    pid = get_player_id(str(user.id))
    if pid:
        await ctx.send(f"player_id for {user.display_name}: `{pid}`")
    else:
        await ctx.send(f"No player found for {user.display_name}.")

@bot.command(name="old_stats", help="[LEGACY] Get raw stats for a Discord ID. Execs only.")
@commands.check(is_exec)
async def old_stats_cmd(ctx, discord_id: str):
    match = re.search(r"\d{15,20}", discord_id)
    if match:
        discord_id = match.group(0)
    
    try:
        member = await bot.fetch_user(int(discord_id))
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
        f"Kills/sec: {stats['kills']}, Deaths/sec: {stats['deaths']}, Assists/sec: {stats['assists']}\n"
        f"Damage/sec: {stats['damage']}, Obj/sec: {stats['objective_time']}, Shield/sec: {stats['shielding']}, Heal/sec: {stats['healing']}\n"
        f"Games: {stats['games']}"
    )
    await ctx.send(msg)


@bot.command(name="ingest_text", help="Parse and insert a scoreboard from text. Execs only.")
@commands.check(is_exec)
async def ingest_text_cmd(ctx, queue_num: str, *, scoreboard_text: str = None):
    # This command remains largely the same, as its purpose is administrative.
    # Logic for parsing and inserting is kept as-is.
    try:
        text = scoreboard_text
        if not text and ctx.message.reference:
            ref_msg = await ctx.channel.fetch_message(ctx.message.reference.message_id)
            if ref_msg:
                text = ref_msg.content or (ref_msg.embeds[0].description if ref_msg.embeds else None)

        if not text:
            await ctx.send("No scoreboard text provided. Paste it after the command or reply to a message containing it.")
            return

        cleaned_text = text.strip().strip("`")
        match_data = parse_match_textbox(cleaned_text)
        match_id = match_data["match_id"]
        queue_value = int(queue_num)

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

# --- USER COMMANDS ---

class LinkConfirmView(View):
    def __init__(self, discord_id, ign):
        super().__init__(timeout=60)
        self.discord_id = discord_id
        self.ign = ign

    @discord.ui.button(label="Confirm (replace IGN)", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, button: Button):
        link_ign(self.ign, self.discord_id, force=True)
        await interaction.response.send_message(
            f"IGN `{self.ign}` has been linked to your account (previous link replaced).",
            ephemeral=True,
        )
        self.stop()

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: Button):
        await interaction.response.send_message("Linking cancelled.", ephemeral=True)
        self.stop()

@bot.command(name="link", help="Link your Discord account to an in-game name (IGN).")
async def link(ctx, ign: str):
    discord_id = str(ctx.author.id)
    try:
        existing_discord_id, ign_exists = get_ign_link_info(ign)
        discord_id_ign = get_ign_for_discord_id(discord_id)

        if ign_exists and existing_discord_id == discord_id:
            await ctx.send(f"IGN `{ign}` is already linked to your account.")
        elif discord_id_ign:
            view = LinkConfirmView(discord_id, ign)
            await ctx.send(
                f"⚠️ You already have an IGN (`{discord_id_ign}`) linked to your account.\n"
                "If this is an alternate account, ask an exec to use `!add_alt`.\n"
                "Otherwise, you can confirm to **replace** your primary IGN.",
                view=view,
            )
        elif not ign_exists:
            success = link_ign(ign, discord_id)
            if success:
                await ctx.send(f"✅ Successfully linked your Discord to IGN `{ign}`.")
            else:
                await ctx.send("❌ Failed to link your Discord to IGN.")
        else: # IGN exists but is linked to someone else
            await ctx.send(f"❌ IGN `{ign}` is already linked to another Discord account. Please contact an exec if this is an error.")
    except Exception as e:
        print(f"Error in --link command: {e}")
        await ctx.send("An error occurred while linking your account.")

@bot.command(name="stats", help="Get stats for a player. Can be filtered by champion.")
async def stats_cmd(ctx, user: PlayerConverter = None, *, champion: str = None):
    """
    TASK 1: Refactored !stats command.
    - Publicly accessible.
    - Optional user and champion arguments.
    - Uses PlayerConverter for user parsing.
    - Outputs a formatted discord.Embed.
    """
    target_user = user or ctx.author
    player_id = get_player_id(str(target_user.id))

    if not player_id:
        await ctx.send(f"No stats found for {target_user.display_name}. They may need to link their IGN using `!link <ign>`.")
        return

    stats = get_player_stats(player_id, champion)
    if not stats or stats["games"] == 0:
        await ctx.send(f"No stats found for {target_user.display_name}" + (f" on {champion.capitalize()}" if champion else "."))
        return

    title = f"Stats for {target_user.display_name}"
    if champion:
        title += f" as {champion.capitalize()}"

    embed = discord.Embed(title=title, color=discord.Color.blue())
    embed.set_thumbnail(url=target_user.display_avatar.url)
    embed.add_field(name="Games Played", value=stats["games"], inline=True)
    embed.add_field(name="Winrate", value=f"{stats['winrate']}%", inline=True)
    embed.add_field(name="K/D/A Ratio", value=stats['kda_ratio'], inline=True)
    embed.add_field(name="Damage/min", value=f"{stats['damage_dealt_pm']:,}", inline=True)
    embed.add_field(name="Healing/min", value=f"{stats['healing_pm']:,}", inline=True)
    embed.add_field(name="Objective Time/min", value=f"{stats['obj_time_pm']:,}", inline=True)
    embed.set_footer(text=f"Raw K/D/A: {stats['kda']}")

    await ctx.send(embed=embed)


@bot.command(name="history", help="Shows the last 5 matches for a player.")
async def history_cmd(ctx, user: PlayerConverter = None):
    """
    TASK 2: New !history command.
    - Publicly accessible.
    - Optional user argument.
    - Outputs a formatted discord.Embed listing the last 5 games.
    """
    target_user = user or ctx.author
    player_id = get_player_id(str(target_user.id))

    if not player_id:
        await ctx.send(f"No history found for {target_user.display_name}. They may need to link their IGN using `!link <ign>`.")
        return

    history = get_match_history(player_id)
    if not history:
        await ctx.send(f"No match history found for {target_user.display_name}.")
        return

    embed = discord.Embed(title=f"Match History for {target_user.display_name}", color=discord.Color.green())
    embed.set_thumbnail(url=target_user.display_avatar.url)

    description = []
    for match in history:
        map_name, champ, k, d, a, result = match
        emoji = "🏆" if result == "W" else "💔"
        description.append(f"{emoji} **[{result}]** {champ} - `{k}/{d}/{a}` on {map_name}")

    embed.description = "\n".join(description)
    await ctx.send(embed=embed)


@bot.command(name="leaderboard", aliases=['lb'], help="Shows the top players for a given stat.")
async def leaderboard_cmd(ctx, stat: str, limit: int = 10):
    """
    TASK 3: New !leaderboard command.
    - Publicly accessible.
    - Required stat argument from a whitelist.
    - Optional limit argument.
    - Outputs a formatted discord.Embed.
    """
    valid_stats = ["damage", "healing", "kda", "winrate", "obj_time"]
    if stat.lower() not in valid_stats:
        await ctx.send(f"Invalid stat. Please choose from: `{', '.join(valid_stats)}`.")
        return
    
    if not 1 <= limit <= 20:
        await ctx.send("Limit must be between 1 and 20.")
        return

    leaderboard_data = get_leaderboard(stat.lower(), limit)
    if not leaderboard_data:
        await ctx.send(f"Could not generate a leaderboard for `{stat}`. Not enough data may be available.")
        return

    # Create the embed
    stat_name_map = {
        "damage": "Damage/min", "healing": "Healing/min", "kda": "KDA Ratio",
        "winrate": "Winrate", "obj_time": "Objective Time/min"
    }
    embed_title = f"🏆 Top {len(leaderboard_data)} Players by {stat_name_map[stat.lower()]}"
    if stat.lower() in ["kda", "winrate"]:
        embed_title += " (min. 10 games)"
    embed = discord.Embed(title=embed_title, color=discord.Color.gold())

    description = []
    for i, (discord_id, value) in enumerate(leaderboard_data):
        member = ctx.guild.get_member(int(discord_id))
        name = member.display_name if member else f"ID: {discord_id}"
        
        # Formatting the value
        if stat.lower() == 'winrate':
            formatted_value = f"{value:.1f}%"
        elif stat.lower() == 'kda':
            formatted_value = f"{value:.2f}"
        else:
            formatted_value = f"{int(value):,}"

        description.append(f"`{i+1:2}.` **{name}** - {formatted_value}")
    
    embed.description = "\n".join(description)
    await ctx.send(embed=embed)


@bot.command(name="top_champs", help="Get top 5 champs for a player.")
async def top_champs_cmd(ctx, user: PlayerConverter = None):
    """
    TASK 4: Refactored !top_champs command.
    - Publicly accessible.
    - Outputs a formatted discord.Embed.
    """
    target_user = user or ctx.author
    player_id = get_player_id(str(target_user.id))

    if not player_id:
        await ctx.send(f"No stats found for {target_user.display_name}. They may need to `!link` their IGN.")
        return

    champs = get_top_champs(player_id)
    if not champs:
        await ctx.send(f"No champion stats found for {target_user.display_name}.")
        return

    embed = discord.Embed(title=f"Top 5 Champions for {target_user.display_name}", color=discord.Color.purple())
    embed.set_thumbnail(url=target_user.display_avatar.url)
    
    for champ in champs:
        name = f"**{champ['champ']}** ({champ['games']} games)"
        value = (
            f"**Winrate:** {champ['winrate']}% | **K/D/A:** `{champ['kda']}`\n"
            f"**Dmg/min:** {champ['damage']:,} | **Heal/min:** {champ['healing']:,}"
        )
        embed.add_field(name=name, value=value, inline=False)
        
    await ctx.send(embed=embed)


@bot.command(name="compare", help="Compare stats between two players.")
async def compare_cmd(ctx, user1: PlayerConverter, user2: PlayerConverter = None):
    """
    TASK 4: Refactored !compare command.
    - Publicly accessible.
    - Outputs a formatted discord.Embed.
    """
    target_user2 = user2 or ctx.author
    if user1 == target_user2:
        await ctx.send("You can't compare a player to themselves!")
        return
        
    result = compare_players(str(user1.id), str(target_user2.id))
    if not result:
        await ctx.send("Could not find stats for one or both players. Ensure they have linked their IGNs.")
        return

    p1_stats = result["player1"]
    p2_stats = result["player2"]

    embed = discord.Embed(title=f"Comparison: {user1.display_name} vs {target_user2.display_name}", color=0x2ECC71)
    
    # Player 1 Field
    p1_value = (
        f"**Games:** {p1_stats['games']} | **Winrate:** {p1_stats['winrate']}%\n"
        f"**KDA Ratio:** {p1_stats['kda_ratio']} | **Dmg/min:** {p1_stats['damage_dealt_pm']:,}\n"
    )
    p1_value += "**Top Champion:**\n"
    if result['top_champs1']:
        top_champ = result['top_champs1'][0]
        p1_value += f"• {top_champ['champ']} ({top_champ['winrate']}% WR in {top_champ['games']} games)"
    else:
        p1_value += "N/A"
    embed.add_field(name=user1.display_name, value=p1_value, inline=True)

    # Player 2 Field
    p2_value = (
        f"**Games:** {p2_stats['games']} | **Winrate:** {p2_stats['winrate']}%\n"
        f"**KDA Ratio:** {p2_stats['kda_ratio']} | **Dmg/min:** {p2_stats['damage_dealt_pm']:,}\n"
    )
    p2_value += "**Top Champion:**\n"
    if result['top_champs2']:
        top_champ = result['top_champs2'][0]
        p2_value += f"• {top_champ['champ']} ({top_champ['winrate']}% WR in {top_champ['games']} games)"
    else:
        p2_value += "N/A"
    embed.add_field(name=target_user2.display_name, value=p2_value, inline=True)
    
    # Synergy Field
    synergy_value = (
        f"**Together:** {result['with_winrate']}% WR ({result['with_games']} games)\n"
        f"**Against:** {result['against_winrate']}% WR ({result['against_games']} games for {user1.display_name})"
    )
    embed.add_field(name="Synergy", value=synergy_value, inline=False)
    
    await ctx.send(embed=embed)


# --- DATA INGESTION LOGIC ---

def parse_match_textbox(text):
    # This function's logic remains the same as it correctly parses the data format.
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    if not lines: raise ValueError("No lines found in match text!")
    match_info = lines[0].split(",")
    if len(match_info) < 6: raise ValueError("Malformed match info line!")
    try:
        match_id, time, region, map_name, team1_score, team2_score = [s.strip() for s in match_info]
    except Exception:
        raise ValueError("Could not unpack match info line.")

    players = []
    for idx, line in enumerate(lines[1:], start=2):
        if not line.startswith("["): raise ValueError(f"Malformed player line {idx}: {line}")
        reader = csv.reader(io.StringIO(line.strip("[]")), skipinitialspace=True, quotechar="'")
        parts = next(reader)
        if len(parts) != 12: raise ValueError(f"Malformed player line {idx}: {len(parts)} fields")
        
        del parts[3]  # Remove rank/placement column
        kda_parts = parts[4].split("/")
        if len(kda_parts) != 3: raise ValueError(f"Malformed KDA in line {idx}")

        try:
            player = {
                "name": parts[0], "champ": parts[1], "talent": parts[2],
                "credits": int(parts[3].replace(",", "")), "kills": int(kda_parts[0]),
                "deaths": int(kda_parts[1]), "assists": int(kda_parts[2]),
                "damage": int(parts[5].replace(",", "")), "taken": int(parts[6].replace(",", "")),
                "obj_time": int(parts[7]), "shielding": int(parts[8].replace(",", "")),
                "healing": int(parts[9].replace(",", "")), "self_healing": int(parts[10].replace(",", "")),
            }
            players.append(player)
        except Exception as e:
            raise ValueError(f"Error parsing player line {idx}: {e}")

    return {
        "match_id": int(match_id), "time": int(time), "region": region,
        "map": map_name, "team1_score": int(team1_score),
        "team2_score": int(team2_score), "players": players,
    }


class QueueNumModal(Modal):
    def __init__(self, match_data_text, author_id):
        super().__init__(title="Enter Queue Number")
        self.queue_num_input = TextInput(label="Queue Number", required=True)
        self.add_item(self.queue_num_input)
        self.match_data_text = match_data_text
        self.author_id = author_id

    async def on_submit(self, interaction):
        queue_num = self.queue_num_input.value.strip()
        try:
            cleaned_text = self.match_data_text.strip().strip("`")
            match_data = parse_match_textbox(cleaned_text)
            match_id = match_data["match_id"]

            if match_exists(match_id):
                await interaction.response.send_message(f"Match ID {match_id} already exists.", ephemeral=True)
                return
            if queue_exists(queue_num):
                await interaction.response.send_message(f"Queue number {queue_num} already exists.", ephemeral=True)
                return

            # Simplified registration check to allow ingestion even with unregistered players
            # Admins can link them later
            insert_scoreboard(match_data, int(queue_num))
            await interaction.response.send_message(f"Match {match_id} for queue {queue_num} successfully recorded.", ephemeral=True)
        except ValueError as ve:
            await interaction.response.send_message(f"Malformed match data: {ve}", ephemeral=True)
        except Exception as e:
            await interaction.response.send_message(f"Error processing match data: {e}", ephemeral=True)


class QueueNumView(View):
    def __init__(self, match_data_text, author_id):
        super().__init__(timeout=300)
        self.match_data_text = match_data_text
        self.author_id = author_id

    @discord.ui.button(label="Enter Queue Number", style=discord.ButtonStyle.primary)
    async def enter_queue(self, interaction: discord.Interaction, button: Button):
        if not is_exec(interaction):
            await interaction.response.send_message("You don't have permission to do this.", ephemeral=True)
            return
        await interaction.response.send_modal(QueueNumModal(self.match_data_text, self.author_id))


@bot.event
async def on_message(message):
    if message.author.bot:
        # Process commands first
        await bot.process_commands(message)
        # Then handle automated ingestion logic
        try:
            if isinstance(message.channel, discord.TextChannel) and message.channel.name in ALLOWED_CHANNELS:
                # Ingestion from NeatQueue (for player IDs)
                if message.author.name == "NeatQueue" and message.author.discriminator == "0850" and message.embeds:
                    for embed in message.embeds:
                        queue_number_match = re.search(r"Queue #?(\d+)", embed.title or "") or re.search(r"Queue #?(\d+)", embed.description or "")
                        if queue_number_match:
                            queue_number = queue_number_match.group(1)
                            insert_embed(queue_number, embed.to_dict())
                
                # Ingestion from PaladinsAssistant (for match results)
                elif message.author.name == "PaladinsAssistant" and message.author.discriminator == "2894":
                    match_text = message.content or (message.embeds[0].description if message.embeds else None)
                    if match_text:
                        view = QueueNumView(match_text, message.author.id)
                        await message.reply("Admins: Click to enter the queue number for this match:", view=view)
        except Exception as e:
            print(f"Error in on_message processing: {e}")
        return

    await bot.process_commands(message)

# Catch-all for command errors
@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.CheckFailure):
        await ctx.send("You do not have the required permissions to run this command.")
    elif isinstance(error, commands.MissingRequiredArgument):
        await ctx.send(f"Missing argument: `{error.param.name}`. Use `!help {ctx.command.name}` for details.")
    elif isinstance(error, commands.BadArgument):
        await ctx.send(f"Invalid argument provided. {error}")
    else:
        print(f"An unhandled error occurred: {error}")
        await ctx.send("An unexpected error occurred. Please contact an administrator.")


bot.run(os.getenv("BOT_TOKEN"))