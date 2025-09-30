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
    get_stats,
    get_top_champs,
    get_winrate_with_against,
    compare_players
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
ALLOWED_CHANNELS = ["match-results", "admin"]  # Define allowed channels

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

@bot.command(name="link_disc", help="Update a player's Discord ID. Only for Executives.")
async def link_disc(ctx, old_id: str, new_id: str):
    if not is_exec(ctx):
        await ctx.send("You need the 'Executive' role to use this command!")
        return
    try:
        update_discord_id(old_id, new_id)
        await ctx.send(f"Successfully updated Discord ID from `{old_id}` to `{new_id}`.")
    except Exception as e:
        print(f"Error in link_disc command: {e}")
        await ctx.send(f"An error occurred while updating the Discord ID: {e}")

@bot.command(name="query", help="Execute a SELECT SQL query. Only for Executives.")
async def query(ctx, *, sql_query: str):
    if not is_exec(ctx):
        await ctx.send("You need the 'Executive' role to use this command!")
        return
    try:
        results = execute_select_query(sql_query)
        if results:
            formatted_results = "\n".join([str(row) for row in results])
            await ctx.send(f"Query Results:\n\n{formatted_results}\n")
        else:
            await ctx.send("No results found.")
    except Exception as e:
        print(f"Error in query command: {e}")
        await ctx.send(f"An error occurred while executing the query: {e}")

@bot.command(name="fetch_embeds", help="Fetch messages and store embeds in the database.")
async def fetch_embeds(ctx):
    if not is_exec(ctx):
        await ctx.send("You need the 'Executive' role to use this command!")
        return
    if ctx.channel.name not in ALLOWED_CHANNELS:
        await ctx.send(f"This command can only be used in the {', '.join(['#' + ch for ch in ALLOWED_CHANNELS])} channels.")
        return
    try:
        async for message in ctx.channel.history(limit=None):
            if (
                message.author.name == "NeatQueue"
                and message.author.discriminator == "0850"
                and message.embeds
            ):
                for embed in message.embeds:
                    queue_number = None
                    if embed.title and "Queue" in embed.title:
                        queue_number = embed.title.split("Queue")[-1].strip()
                    elif embed.description and "Queue" in embed.description:
                        queue_number = embed.description.split(
                            "Queue")[-1].strip()
                    if queue_number:
                        embed_data = embed.to_dict()
                        insert_embed(queue_number, embed_data)
        await ctx.send("Successfully fetched and stored embeds in the database.")
    except Exception as e:
        print(f"Error in fetch_embeds command: {e}")
        await ctx.send("An error occurred while fetching embeds.")

class LinkConfirmView(View):
    def __init__(self, discord_id, ign):
        super().__init__(timeout=60)
        self.discord_id = discord_id
        self.ign = ign

    @discord.ui.button(label="Confirm (replace IGN)", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, button: Button):
        link_ign(self.ign, self.discord_id, force=True)
        await interaction.response.send_message(
            f"IGN `{self.ign}` has been linked to Discord ID `{self.discord_id}` (previous link replaced).", ephemeral=True
        )
        self.stop()

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: Button):
        await interaction.response.send_message("Linking cancelled.", ephemeral=True)
        self.stop()

@bot.command(name="link", help="Link a Discord ID to an in-game name (IGN).")
async def link(ctx, discord_id: str, ign: str):
    try:
        is_exec_user = is_exec(ctx)
        if discord_id.lower() == "me":
            discord_id = str(ctx.author.id)
        elif not is_exec_user:
            await ctx.send("You need the 'Executive' role to link others. Use 'me'.")
            return
        else:
            discord_id = re.sub(r"[<@!>]", "", discord_id)
            if not discord_id.isdigit():
                await ctx.send("Error: Please provide a valid Discord ID or use 'me'.")
                return

        existing_discord_id, ign_exists = get_ign_link_info(ign)
        discord_id_ign = get_ign_for_discord_id(discord_id)

        if ign_exists and existing_discord_id == discord_id:
            await ctx.send(f"IGN `{ign}` is already linked to your Discord ID `{discord_id}`.")
        elif discord_id_ign and (not ign_exists or existing_discord_id != discord_id):
            view = LinkConfirmView(discord_id, ign)
            await ctx.send(
                f"Warning: You already have an IGN (`{discord_id_ign}`) linked to your Discord ID `{discord_id}`.\n"
                "If this is an alternate account, contact an exec to set it using `add_alt_ign`.\n"
                "Otherwise, you can confirm to replace your IGN or cancel.",
                view=view
            )
        elif not ign_exists:
            success = link_ign(ign, discord_id)
            if success:
                await ctx.send(f"Linked Discord ID {discord_id} to IGN {ign}.")
            else:
                await ctx.send(f"Failed to link Discord ID {discord_id} to IGN {ign}.")
        else:
            view = LinkConfirmView(discord_id, ign)
            await ctx.send(
                f"IGN `{ign}` is already linked to another Discord ID `{existing_discord_id}`.\n"
                "Is this an alternate account? If so, contact an exec to set it using `add_alt_ign`.\n"
                "Otherwise, you can confirm to replace the IGN or cancel.",
                view=view
            )
    except Exception as e:
        print(f"Error in --link command: {e}")
        await ctx.send("An error occurred while linking the Discord ID to the IGN.")

@bot.command(name="add_alt", help="Add an alternate IGN for a player. Execs only.")
async def add_alt_ign_cmd(ctx, discord_id: str, alt_ign: str):
    if not is_exec(ctx):
        await ctx.send("You need the 'Executive' role to use this command!")
        return
    match = re.search(r"\d{15,20}", discord_id)
    if match:
        discord_id = match.group(0)
    else:
        await ctx.send("Error: Please provide a valid Discord ID, mention, or use 'me'.")
        return
    success = add_alt_ign(discord_id, alt_ign)
    if success:
        await ctx.send(f"Added alt IGN `{alt_ign}` for Discord ID `{discord_id}`.")
    else:
        await ctx.send(f"Failed to add alt IGN `{alt_ign}` for Discord ID `{discord_id}`.")

def parse_match_textbox(text):
    print("Raw match text received:")
    print(text)
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    print(f"Total lines after stripping: {len(lines)}")
    if not lines:
        print("No lines found in match text!")
        raise ValueError("No lines found in match text!")

    match_info = lines[0].split(",")
    print(f"Match info line: {match_info}")
    if len(match_info) < 6:
        print("Malformed match info line!")
        raise ValueError("Malformed match info line!")

    try:
        match_id = int(match_info[0])
        time = int(match_info[1])
        region = match_info[2].strip()
        map_name = match_info[3].strip()
        team1_score = int(match_info[4])
        team2_score = int(match_info[5])
    except Exception as e:
        print(f"Error parsing match info: {e}")
        raise ValueError(f"Error parsing match info: {e}")

    players = []
    for idx, line in enumerate(lines[1:], start=2):
        if not line.startswith("["):
            error_msg = f"Malformed player line {idx}: {line} (does not start with [)"
            print(error_msg)
            raise ValueError(error_msg)
        reader = csv.reader(io.StringIO(line.strip("[]")),
                            skipinitialspace=True, quotechar="'")
        parts = next(reader)
        if len(parts) != 12:
            error_msg = f"Malformed player line {idx}: {parts} (found {len(parts)} fields, expected 12)"
            print(error_msg)
            raise ValueError(error_msg)
        del parts[3]
        kda_parts = parts[4].split("/")
        if len(kda_parts) != 3:
            error_msg = f"Malformed KDA field in line {idx}: {parts[4]}"
            print(error_msg)
            raise ValueError(error_msg)
        try:
            kills = int(kda_parts[0])
            deaths = int(kda_parts[1])
            assists = int(kda_parts[2])
            player = {
                "name": parts[0],
                "champ": parts[1],
                "talent": parts[2],
                "credits": int(parts[3].replace(",", "")),
                "kills": kills,
                "deaths": deaths,
                "assists": assists,
                "damage": int(parts[5].replace(",", "")),
                "taken": int(parts[6].replace(",", "")),
                "obj_time": int(parts[7]),
                "shielding": int(parts[8].replace(",", "")),
                "healing": int(parts[9].replace(",", "")),
                "self_healing": int(parts[10].replace(",", "")),
            }
            print(f"Parsed player: {player}")
            players.append(player)
        except Exception as e:
            error_msg = f"Error parsing player line {idx}: {e}"
            print(error_msg)
            raise ValueError(error_msg)

    print(f"Total parsed players: {len(players)}")
    return {
        "match_id": match_id,
        "time": time,
        "region": region,
        "map": map_name,
        "team1_score": team1_score,
        "team2_score": team2_score,
        "players": players,
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
            cleaned_text = self.match_data_text.strip()
            if cleaned_text.startswith("```"):
                cleaned_text = re.sub(r"^```[a-zA-Z]*\n?", "", cleaned_text)
                cleaned_text = cleaned_text.rstrip("`").rstrip()
            match_data = parse_match_textbox(cleaned_text)
            match_id = match_data["match_id"]

            if match_exists(match_id):
                await interaction.response.send_message(f"Match ID `{match_id}` already exists in the database.", ephemeral=True)
                return
            if queue_exists(queue_num):
                await interaction.response.send_message(f"Queue number `{queue_num}` already exists in the database.", ephemeral=True)
                return

            discord_ids = read_embeds(int(queue_num))
            if not discord_ids:
                await interaction.response.send_message(f"No Discord IDs found for queue `{queue_num}`.", ephemeral=True)
                return

            ign_list = [player["name"] for player in match_data["players"]]
            registered_igns, igns_not_registered = get_registered_igns(
                ign_list)
            _, unregistered_discords = verify_registered_users(discord_ids)

            warning_msgs = []
            if unregistered_discords:
                warning_msgs.append(
                    "Not registered: " + ", ".join([f"<@{d}>" for d in unregistered_discords]))
            if igns_not_registered:
                warning_msgs.append(
                    "IGNs not registered: " + ", ".join(igns_not_registered))
            if warning_msgs:
                await interaction.response.send_message("\n".join(warning_msgs) + "\nNo data written to DB.", ephemeral=True)
                return

            insert_scoreboard(match_data, int(queue_num))
            await interaction.response.send_message(f"Match `{match_id}` for queue `{queue_num}` successfully recorded.", ephemeral=True)
        except ValueError as ve:
            print(f"Malformed match data: {ve}")
            await interaction.response.send_message(f"Malformed match data: {ve}", ephemeral=True)
        except Exception as e:
            print(f"Error in QueueNumModal: {e}")
            await interaction.response.send_message(f"Error processing match data: {e}", ephemeral=True)

class QueueNumView(View):
    def __init__(self, match_data_text, author_id):
        super().__init__(timeout=300)
        self.match_data_text = match_data_text
        self.author_id = author_id

    @discord.ui.button(label="Enter Queue Number", style=discord.ButtonStyle.primary)
    async def enter_queue(self, interaction: discord.Interaction, button: Button):
        await interaction.response.send_modal(QueueNumModal(self.match_data_text, self.author_id))

@bot.event
async def on_message(message):
    try:
        if isinstance(message.channel, discord.TextChannel):
            if (
                message.channel.name in ALLOWED_CHANNELS
                and message.author.name == "NeatQueue"
                and message.author.discriminator == "0850"
            ):
                if message.embeds:
                    for embed in message.embeds:
                        queue_number = None
                        if embed.title and "Queue" in embed.title:
                            queue_number = embed.title.split(
                                "Queue")[-1].strip()
                        elif embed.description and "Queue" in embed.description:
                            queue_number = embed.description.split(
                                "Queue")[-1].strip()
                        if queue_number:
                            embed_data = embed.to_dict()
                            insert_embed(queue_number, embed_data)
                    print("New embed processed and stored in the database.")

            if (
                message.channel.name in ALLOWED_CHANNELS
                and message.author.name == "PaladinsAssistant"
                and message.author.discriminator == "2894"
            ):
                match_text = None
                if message.content:
                    match_text = message.content
                elif message.embeds:
                    for embed in message.embeds:
                        if embed.description:
                            match_text = embed.description
                            break
                if match_text:
                    view = QueueNumView(match_text, message.author.id)
                    await message.reply(
                        "Please enter the corresponding queue number for this match:",
                        view=view
                    )
                    return

    except Exception as e:
        print(f"Error processing embeds: {e}")

    await bot.process_commands(message)

@bot.command(name="show_alts", help="Show all alternate IGNs for a player. Execs only.")
async def show_alt_igns_cmd(ctx, discord_id: str):
    if not is_exec(ctx):
        await ctx.send("You need the 'Executive' role to use this command!")
        return
    match = re.search(r"\d{15,20}", discord_id)
    if match:
        discord_id = match.group(0)
    else:
        await ctx.send("Error: Please provide a valid Discord ID, mention, or use 'me'.")
        return
    alt_igns = get_alt_igns(discord_id)
    if alt_igns:
        await ctx.send(f"Alternate IGNs for Discord ID `{discord_id}`: {', '.join(alt_igns)}")
    else:
        await ctx.send(f"No alternate IGNs found for Discord ID `{discord_id}`.")

@bot.command(name="delete_alt", help="Delete an alternate IGN for a player. Execs only.")
async def delete_alt_ign_cmd(ctx, discord_id: str, alt_ign: str):
    if not is_exec(ctx):
        await ctx.send("You need the 'Executive' role to use this command!")
        return
    match = re.search(r"\d{15,20}", discord_id)
    if match:
        discord_id = match.group(0)
    else:
        await ctx.send("Error: Please provide a valid Discord ID, mention, or use 'me'.")
        return
    success = delete_alt_ign(discord_id, alt_ign)
    if success:
        await ctx.send(f"Deleted alt IGN `{alt_ign}` for Discord ID `{discord_id}`.")
    else:
        await ctx.send(f"Failed to delete alt IGN `{alt_ign}` for Discord ID `{discord_id}`.")

@bot.command(name="compare", help="Compare two players by Discord ID or 'me'.")
async def compare_cmd(ctx, id1: str, id2: str):
    def parse_id(val):
        if val.lower() == "me":
            return str(ctx.author.id)
        match = re.search(r"\d{15,20}", val)
        return match.group(0) if match else val

    disc_id1 = parse_id(id1)
    disc_id2 = parse_id(id2)

    member1 = await get_member(ctx, disc_id1)
    member2 = await get_member(ctx, disc_id2)
    name1 = member1.display_name if member1 else disc_id1
    name2 = member2.display_name if member2 else disc_id2

    result = compare_players(disc_id1, disc_id2)
    if not result:
        await ctx.send("Could not find stats for one or both players.")
        return

    p1 = result["player1"]
    p2 = result["player2"]
    champs1 = result["top_champs1"]
    champs2 = result["top_champs2"]

    msg = f"**Player 1 ({name1})**\n"
    msg += f"Kills/min: {p1['kills']}, Deaths/min: {p1['deaths']}, Assists/min: {p1['assists']}\n"
    msg += f"Damage/min: {p1['damage']}, Obj/min: {p1['objective_time']}, Shield/min: {p1['shielding']}, Heal/min: {p1['healing']}\n"
    msg += f"Games: {p1['games']}\n"
    msg += "**Top 5 Champs:**\n"
    for champ in champs1:
        msg += f"{champ['champ']}: Winrate {champ['winrate']}%, KDA {champ['kda']}, Dmg {champ['damage']}/min, Obj {champ['objective_time']}/min, Shield {champ['shielding']}/min, Heal {champ['healing']}/min\n"

    msg += f"\n**Player 2 ({name2})**\n"
    msg += f"Kills/min: {p2['kills']}, Deaths/min: {p2['deaths']}, Assists/min: {p2['assists']}\n"
    msg += f"Damage/min: {p2['damage']}, Obj/min: {p2['objective_time']}, Shield/min: {p2['shielding']}, Heal/min: {p2['healing']}\n"
    msg += f"Games: {p2['games']}\n"
    msg += "**Top 5 Champs:**\n"
    for champ in champs2:
        msg += f"{champ['champ']}: Winrate {champ['winrate']}%, KDA {champ['kda']}, Dmg {champ['damage']}/min, Obj {champ['objective_time']}/min, Shield {champ['shielding']}/min, Heal {champ['healing']}/min\n"

    msg += f"\n**Winrate with each other:** {result['with_winrate']}% over {result['with_games']} games\n"
    msg += f"**Winrate against each other:** {result['against_winrate']}% over {result['against_games']} games"

    await ctx.send(msg)

@bot.command(name="player_id", help="Get player_id for a Discord ID. Execs only.")
async def player_id_cmd(ctx, discord_id: str):
    if not is_exec(ctx):
        await ctx.send("You need the 'Executive' role to use this command!")
        return
    match = re.search(r"\d{15,20}", discord_id)
    if match:
        discord_id = match.group(0)
    member = await get_member(ctx, discord_id)
    name = member.display_name if member else discord_id
    pid = get_player_id(discord_id)
    if pid:
        await ctx.send(f"player_id for {name}: `{pid}`")
    else:
        await ctx.send(f"No player found for {name}.")

@bot.command(name="stats", help="Get normalized stats for a Discord ID or mention. Execs only.")
async def stats_cmd(ctx, discord_id: str):
    if not is_exec(ctx):
        await ctx.send("You need the 'Executive' role to use this command!")
        return
    match = re.search(r"\d{15,20}", discord_id)
    if match:
        discord_id = match.group(0)
    member = await get_member(ctx, discord_id)
    name = member.display_name if member else discord_id
    player_id = get_player_id(discord_id)
    if not player_id:
        await ctx.send(f"No player found for {name}.")
        return
    stats = get_stats(player_id)
    if not stats:
        await ctx.send(f"No stats found for {name}.")
        return
    msg = (
        f"**Stats for {name} (player_id `{player_id}`):**\n"
        f"Kills/min: {stats['kills']}, Deaths/min: {stats['deaths']}, Assists/min: {stats['assists']}\n"
        f"Damage/min: {stats['damage']}, Obj/min: {stats['objective_time']}, Shield/min: {stats['shielding']}, Heal/min: {stats['healing']}\n"
        f"Games: {stats['games']}"
    )
    await ctx.send(msg)

@bot.command(name="top_champs", help="Get top 5 champs for a Discord ID or mention. Execs only.")
async def top_champs_cmd(ctx, discord_id: str):
    if not is_exec(ctx):
        await ctx.send("You need the 'Executive' role to use this command!")
        return
    match = re.search(r"\d{15,20}", discord_id)
    if match:
        discord_id = match.group(0)
    member = await get_member(ctx, discord_id)
    name = member.display_name if member else discord_id
    player_id = get_player_id(discord_id)
    if not player_id:
        await ctx.send(f"No player found for {name}.")
        return
    champs = get_top_champs(player_id)
    if not champs:
        await ctx.send(f"No champion stats found for {name}.")
        return
    msg = f"**Top 5 Champs for {name} (player_id `{player_id}`):**\n"
    for champ in champs:
        msg += (
            f"**{champ['champ']}**: "
            f"Winrate: {champ['winrate']}%, "
            f"KDA: {champ['kda']}, "
            f"Damage/min: {champ['damage']}, "
            f"Obj/min: {champ['objective_time']}, "
            f"Shield/min: {champ['shielding']}, "
            f"Heal/min: {champ['healing']}\n"
        )
    await ctx.send(msg)

@bot.command(name="winrate_with_against", help="Get winrate with/against two Discord IDs or mentions. Execs only.")
async def winrate_with_against_cmd(ctx, discord_id1: str, discord_id2: str):
    if not is_exec(ctx):
        await ctx.send("You need the 'Executive' role to use this command!")
        return
    match1 = re.search(r"\d{15,20}", discord_id1)
    match2 = re.search(r"\d{15,20}", discord_id2)
    if match1:
        discord_id1 = match1.group(0)
    if match2:
        discord_id2 = match2.group(0)
    member1 = await get_member(ctx, discord_id1)
    member2 = await get_member(ctx, discord_id2)
    name1 = member1.display_name if member1 else discord_id1
    name2 = member2.display_name if member2 else discord_id2
    player_id1 = get_player_id(discord_id1)
    player_id2 = get_player_id(discord_id2)
    if not player_id1 or not player_id2:
        await ctx.send(f"Could not find player_id for one or both users ({name1}, {name2}).")
        return
    with_winrate, with_games, against_winrate, against_games = get_winrate_with_against(
        player_id1, player_id2)
    await ctx.send(
        f"Winrate WITH ({name1} & {name2}): {with_winrate}% over {with_games} games\n"
        f"Winrate AGAINST ({name1} vs {name2}): {against_winrate}% over {against_games} games"
    )

@bot.command(name="ingest_text", help="Parse and insert a scoreboard from pasted text or a replied message. Execs only.")
async def ingest_text_cmd(ctx, queue_num: str, *, scoreboard_text: str = None):
    if not is_exec(ctx):
        await ctx.send("You need the 'Executive' role to use this command!")
        return
    try:
        # Try to gather scoreboard text from explicit arg or reply
        text = scoreboard_text
        if not text:
            ref = getattr(ctx.message, "reference", None)
            ref_msg = None
            if ref and ref.resolved:
                ref_msg = ref.resolved
            elif ref and ref.message_id:
                try:
                    ref_msg = await ctx.channel.fetch_message(ref.message_id)
                except Exception:
                    ref_msg = None
            if ref_msg:
                if ref_msg.content:
                    text = ref_msg.content
                elif ref_msg.embeds:
                    for embed in ref_msg.embeds:
                        if embed.description:
                            text = embed.description
                            break

        # Handle forgiving formats
        raw_queue_str = queue_num.strip()
        queue_is_digits = raw_queue_str.isdigit()
        if "," in raw_queue_str or not queue_is_digits:
            # User pasted header immediately after command; ensure header starts with match_id
            # Prepend the token back to the text so parser sees full header
            text = (raw_queue_str + (" " + text if text else "")).strip()
            # Use match_id for queue unless an explicit numeric was supplied (it wasn't in this branch)
            raw_queue_str = None

        if not text:
            await ctx.send("No scoreboard text provided. Paste it after the command or reply to a message containing it.")
            return

        cleaned_text = text.strip()
        if cleaned_text.startswith("```"):
            cleaned_text = re.sub(r"^```[a-zA-Z]*\n?", "", cleaned_text)
            cleaned_text = cleaned_text.rstrip("`").rstrip()

        match_data = parse_match_textbox(cleaned_text)
        match_id = match_data["match_id"]

        # Decide queue number: prefer explicit numeric arg; otherwise default to match_id
        if raw_queue_str and raw_queue_str.isdigit():
            queue_value = int(raw_queue_str)
        else:
            queue_value = int(match_id)

        # Duplicate checks similar to modal flow
        if match_exists(match_id):
            await ctx.send(f"Match ID `{match_id}` already exists in the database.")
            return
        if queue_exists(queue_value):
            await ctx.send(f"Queue number `{queue_value}` already exists in the database.")
            return

        insert_scoreboard(match_data, int(queue_value))
        await ctx.send(f"Match `{match_id}` for queue `{queue_value}` successfully recorded.")
    except ValueError as ve:
        await ctx.send(f"Malformed match data: {ve}")
    except Exception as e:
        print(f"Error in ingest_text: {e}")
        await ctx.send(f"Error processing match data: {e}")

async def get_member(ctx, discord_id):
    guild = ctx.guild
    if guild is None:
        guild = bot.get_guild(GUILD_ID)
    member = guild.get_member(int(discord_id))
    if member:
        return member
    # fallback: fetch from API if not cached
    try:
        member = await guild.fetch_member(int(discord_id))
        return member
    except Exception:
        return None

bot.run(os.getenv("BOT_TOKEN"))
