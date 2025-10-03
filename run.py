import discord
from discord.ext import commands
import os
import dotenv
import re
import time
from discord.ui import View, Button, Modal, TextInput, Select
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
    get_player_stats,
    get_top_champs,
    get_winrate_with_against,
    compare_players,
    get_match_history,
    get_leaderboard,
    get_old_stats,
    get_discord_id_for_ign,
    get_champion_name,
    get_all_champion_stats,
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

CHAMPION_ROLES = {
    # Damage
    "Bomb King": "Damage", "Cassie": "Damage", "Dredge": "Damage", "Drogoz": "Damage",
    "Imani": "Damage", "Kinessa": "Damage", "Lian": "Damage", "Octavia": "Damage",
    "Saati": "Damage", "Sha Lin": "Damage", "Strix": "Damage", "Tiberius": "Damage",
    "Tyra": "Damage", "Viktor": "Damage", "Willo": "Damage", "Betty la Bomba": "Damage",
    # Flank
    "Androxus": "Flank", "Buck": "Flank", "Caspian": "Flank", "Evie": "Flank",
    "Koga": "Flank", "Lex": "Flank", "Maeve": "Flank", "Moji": "Flank",
    "Skye": "Flank", "Talus": "Flank", "Vatu": "Flank", "Vora": "Flank",
    "VII": "Flank", "Zhin": "Flank",
    # Tank
    "Ash": "Tank", "Atlas": "Tank", "Azaan": "Tank", "Barik": "Tank", "Fernando": "Tank",
    "Inara": "Tank", "Khan": "Tank", "Makoa": "Tank", "Raum": "Tank", "Ruckus": "Tank",
    "Terminus": "Tank", "Torvald": "Tank", "Yagorath": "Tank", "Nyx": "Tank", "Omen": "Tank",
    # Support
    "Corvus": "Support", "Furia": "Support", "Ghrok": "Support", "Grover": "Support",
    "Io": "Support", "Jenos": "Support", "Lillith": "Support", "Mal'Damba": "Support",
    "Pip": "Support", "Rei": "Support", "Seris": "Support", "Ying": "Support",
}


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



def get_champion_icon_path(champion_name):
    """Formats a champion name into a valid file path for its icon."""
    # Converts "Champion Name" into "champion_name.png"
    formatted_name = champion_name.lower().replace(" ", "_").replace("'", "")
    # This assumes your icon files are named like 'androxus.png', 'sha_lin.png', etc.
    # and are located in 'icons/champ_icons/'
    return os.path.join("icons", "champ_icons", f"{formatted_name}.png")




# FINAL VERSION: This converter now handles all cases:
# 1. 'me' keyword
# 2. Mentions and cached member IDs
# 3. Uncached user IDs (users not in the server)
# 4. Searches for members in the server by name/nickname
# 5. Searches the database for a matching In-Game Name (IGN)
class PlayerConverter(commands.Converter):
    async def convert(self, ctx, argument):
        # 1. Handle 'me'
        if argument.lower() == 'me':
            return ctx.author

        # 2. Try standard member converter (mentions, cached IDs)
        try:
            return await commands.MemberConverter().convert(ctx, argument)
        except commands.MemberNotFound:
            # 3. Try fetching user by raw ID
            if argument.isdigit():
                try:
                    return await bot.fetch_user(int(argument))
                except discord.NotFound:
                    pass  # Not a valid user ID, proceed to name search

            # 4. Try searching members in the current server by name
            lower_arg = argument.lower()
            for member in ctx.guild.members:
                if member.display_name.lower() == lower_arg or member.name.lower() == lower_arg:
                    return member
            for member in ctx.guild.members:
                if member.display_name.lower().startswith(lower_arg) or member.name.lower().startswith(lower_arg):
                    return member

            # 5. Final fallback: search the database for a matching IGN
            found_id = get_discord_id_for_ign(argument)
            if found_id:
                try:
                    return await bot.fetch_user(int(found_id))
                except discord.NotFound:
                    # The user associated with the IGN might have deleted their account
                    pass

            # If all attempts fail, raise the error
            raise commands.BadArgument(f'User or IGN "{argument}" not found.')


# --- ADMIN COMMANDS --- (Unchanged)

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
        f"Kills/min: {stats['kills']}, Deaths/min: {stats['deaths']}, Assists/min: {stats['assists']}\n"
        f"Damage/min: {stats['damage']}, Obj/min: {stats['objective_time']}, Shield/min: {stats['shielding']}, Heal/min: {stats['healing']}\n"
        f"Games: {stats['games']}"
    )
    await ctx.send(msg)

@bot.command(name="ingest_text", help="Parse and insert a scoreboard from text. Execs only.")
@commands.check(is_exec)
async def ingest_text_cmd(ctx, queue_num: str, *, scoreboard_text: str = None):
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

# --- USER COMMANDS --- (Unchanged from here down)

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
        else:
            await ctx.send(f"❌ IGN `{ign}` is already linked to another Discord account. Please contact an exec if this is an error.")
    except Exception as e:
        print(f"Error in --link command: {e}")
        await ctx.send("An error occurred while linking your account.")


@bot.command(name="stats", help="Get detailed stats for a player, with an optional champion filter.")
async def stats_cmd(ctx, user: PlayerConverter = None, *, champion: str = None):
    start_time = time.monotonic()
    target_user = user or ctx.author
    
    player_id = get_player_id(str(target_user.id))
    if not player_id:
        await ctx.send(f"No stats found for {target_user.display_name}. They may need to link their IGN using `!link <ign>`.")
        return

    icon_file = None
    embed = discord.Embed(color=discord.Color.blue())

    if champion:
        # --- CHAMPION-SPECIFIC STATS LAYOUT ---
        full_champion_name = get_champion_name(player_id, champion)
        if not full_champion_name:
            await ctx.send(f"No stats found for {target_user.display_name} on a champion matching '{champion}'.")
            return
        
        champ_stats = get_player_stats(player_id, full_champion_name)
        if not champ_stats or champ_stats["games"] == 0:
            await ctx.send(f"No stats found for {target_user.display_name} on {full_champion_name}.")
            return

        global_stats = get_player_stats(player_id)

        embed.set_author(name=f"{target_user.display_name}'s Stats", icon_url=target_user.display_avatar.url)
        icon_path = get_champion_icon_path(full_champion_name)
        if os.path.exists(icon_path):
            icon_file = discord.File(icon_path, filename="icon.png")
            embed.set_thumbnail(url="attachment://icon.png")
        
        # FIXED: Corrected all variables to use 'champ_stats' instead of 'stats'
        champ_data = {
            f"--- Champion: {full_champion_name} ---": "",
            "Winrate": f"{champ_stats['winrate']:.2f}% ({champ_stats['wins']}-{champ_stats['losses']})",
            "KDA": f"{champ_stats['kda_ratio']:.2f} ({champ_stats['raw_k']}/{champ_stats['raw_d']}/{champ_stats['raw_a']})",
            "Damage/Min": f"{int(champ_stats['damage_dealt_pm']):,}",
            "Damage Taken/Min": f"{int(champ_stats['damage_taken_pm']):,}",
            "Healing/Min": f"{int(champ_stats['healing_pm']):,}",
            "Self Healing/Min": f"{int(champ_stats['self_healing_pm']):,}",
            "Credits/Min": f"{int(champ_stats['credits_pm']):,}",
            "AVG Damage Dealt": f"{int(champ_stats['avg_damage_dealt']):,}",
            "AVG Damage Taken": f"{int(champ_stats['avg_damage_taken']):,}",
            "AVG Damage Delta": f"{int(champ_stats['damage_delta']):,}",
            "AVG Healing": f"{int(champ_stats['avg_healing']):,}",
            "AVG Self Healing": f"{int(champ_stats['avg_self_healing']):,}",
            "AVG Shielding": f"{int(champ_stats['avg_shielding']):,}",
            "AVG Credits": f"{int(champ_stats['avg_credits']):,}",
            "AVG Objective Time": f"{int(champ_stats['obj_time']):,}",
        }
        global_data = {
            "--- Global Stats ---": "",
            "Global Winrate": f"{global_stats['winrate']:.2f}% ({global_stats['wins']}-{global_stats['losses']})",
            "Global KDA": f"{global_stats['kda_ratio']:.2f}",
        }
        max_label_len = max(len(label) for label in list(champ_data.keys()) + list(global_data.keys()))
        
        champ_lines = [f"{label + ':':<{max_label_len + 2}} {value}" if value else label for label, value in champ_data.items()]
        global_lines = [f"{label + ':':<{max_label_len + 2}} {value}" if value else label for label, value in global_data.items()]
        
        embed.description = "```\n" + "\n".join(champ_lines) + "\n\n" + "\n".join(global_lines) + "\n```"

    else:
        # --- GENERAL STATS LAYOUT ---
        stats = get_player_stats(player_id)
        if not stats or stats["games"] == 0:
            await ctx.send(f"No stats found for {target_user.display_name}.")
            return
            
        embed.title = f"Stats for {target_user.display_name}"
        embed.set_thumbnail(url=target_user.display_avatar.url)

        data = {
            "Winrate": f"{stats['winrate']:.2f}% ({stats['wins']}-{stats['losses']})",
            "KDA": f"{stats['kda_ratio']:.2f} ({stats['raw_k']}/{stats['raw_d']}/{stats['raw_a']})",
            "--- Per Minute ---": "",
            "Damage/Min": f"{int(stats['damage_dealt_pm']):,}",
            "Damage Taken/Min": f"{int(stats['damage_taken_pm']):,}",
            "Healing/Min": f"{int(stats['healing_pm']):,}",
            "Self Healing/Min": f"{int(stats['self_healing_pm']):,}",
            "Credits/Min": f"{int(stats['credits_pm']):,}",
            "--- Per Match ---": "",
            "AVG Damage Dealt": f"{int(stats['avg_damage_dealt']):,}",
            "AVG Damage Taken": f"{int(stats['avg_damage_taken']):,}",
            "AVG Damage Delta": f"{int(stats['damage_delta']):,}",
            "AVG Healing": f"{int(stats['avg_healing']):,}",
            "AVG Self Healing": f"{int(stats['avg_self_healing']):,}",
            "AVG Shielding": f"{int(stats['avg_shielding']):,}",
            "AVG Credits": f"{int(stats['avg_credits']):,}",
            "AVG Objective Time": f"{int(stats['obj_time']):,}",
        }
        max_label_len = max(len(label) for label in data.keys())
        stat_lines = [f"{label + ':':<{max_label_len + 2}} {value}" if value else label for label, value in data.items()]
        embed.description = "```\n" + "\n".join(stat_lines) + "\n```"
    
    # Set the footer
    fetch_time = (time.monotonic() - start_time) * 1000
    footer_text = f"Fetched in {fetch_time:.0f}ms"
    if not champion:
        footer_text = f"Player ID: {target_user.id}  •  {footer_text}"
    embed.set_footer(text=footer_text, icon_url=ctx.guild.icon.url if ctx.guild.icon else None)

    await ctx.send(embed=embed, file=icon_file)

# --- NEW INTERACTIVE VIEW FOR !TOP COMMAND ---
class TopChampsView(View):
    def __init__(self, author_id, all_champ_data, target_user_name):
        super().__init__(timeout=90)
        self.author_id = author_id
        self.all_champ_data = all_champ_data
        self.target_user_name = target_user_name
        self.current_sort_key = "games"  # Default sort
        self.current_role_filter = None  # Default no filter

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        # Only allow the original command user to interact
        return interaction.user.id == self.author_id

    
    async def on_timeout(self) -> None:
        # The message is being deleted by `delete_after`, so we don't need to do anything here.
        # This prevents the bot from trying to edit a message that no longer exists.
        pass

    def _generate_description(self) -> str:
        """Generates the formatted text block based on current sort/filter."""
        
        filtered_data = self.all_champ_data
        if self.current_role_filter:
            filtered_data = [
                champ for champ in self.all_champ_data
                if CHAMPION_ROLES.get(champ["champ"]) == self.current_role_filter
            ]

        if not filtered_data:
            return f"```\nNo champions played in the '{self.current_role_filter}' role.\n```"

        sorted_data = sorted(filtered_data, key=lambda x: x[self.current_sort_key], reverse=True)

        lines = []
        # CHANGED: All columns are now left-aligned (<) with new widths to create spacing.
        header = f"{'Champion':<16}{'KDA':<8}{'WR':<9}{'Matches':<10}{'Time'}"
        separator = "-" * len(header)
        
        roles_to_display = [self.current_role_filter] if self.current_role_filter else ["Damage", "Flank", "Tank", "Support"]

        for role in roles_to_display:
            champs_in_role = [c for c in sorted_data if CHAMPION_ROLES.get(c["champ"]) == role]
            if not champs_in_role:
                continue
            
            lines.append(header)
            lines.append(separator)
            lines.append(f"#   {role}")
            
            for i, champ in enumerate(champs_in_role, 1):
                name = champ['champ']
                if len(name) > 12:
                    name = name[:11] + "…"
                kda, wr, matches, time_played = f"{champ['kda_ratio']:.2f}", f"{champ['winrate']:.1f}%", str(champ['games']), champ['time_played']
                
                # CHANGED: Data rows now match the header's left-alignment and spacing.
                lines.append(f"{str(i)+'.':<4}{name:<12}{kda:<8}{wr:<9}{matches:<10}{time_played}")
            lines.append("")
        
        return "```\n" + "\n".join(lines) + "\n```"





    @discord.ui.select(
        placeholder="Sort by Matches",
        options=[
            discord.SelectOption(label="Sort by Matches", value="games", description="Default sorting, most played first."),
            discord.SelectOption(label="Sort by KDA", value="kda_ratio", description="Highest KDA ratio first."),
            discord.SelectOption(label="Sort by Winrate", value="winrate", description="Highest winrate first."),
        ]
    )


    async def sort_select(self, interaction: discord.Interaction, select: Select):
        self.current_sort_key = select.values[0]
        select.placeholder = f"Sort by {select.values[0].replace('_', ' ').capitalize()}"
        
        new_description = self._generate_description()
        await interaction.response.edit_message(content=None, embed=discord.Embed(
            title=f"Top Champions for {self.target_user_name}",
            description=new_description,
            color=discord.Color.blue()
        ), view=self)

    @discord.ui.button(label="All Roles", style=discord.ButtonStyle.primary, row=2)
    async def all_roles_button(self, interaction: discord.Interaction, button: Button):
        self.current_role_filter = None
        new_description = self._generate_description()
        await interaction.response.edit_message(content=None, embed=discord.Embed(
            title=f"Top Champions for {self.target_user_name}",
            description=new_description,
            color=discord.Color.blue()
        ), view=self)

    @discord.ui.button(label="Damage", style=discord.ButtonStyle.secondary, row=2)
    async def damage_button(self, interaction: discord.Interaction, button: Button):
        self.current_role_filter = "Damage"
        new_description = self._generate_description()
        await interaction.response.edit_message(content=None, embed=discord.Embed(
            title=f"Top Champions for {self.target_user_name}",
            description=new_description,
            color=discord.Color.red()
        ), view=self)

    @discord.ui.button(label="Flank", style=discord.ButtonStyle.secondary, row=2)
    async def flank_button(self, interaction: discord.Interaction, button: Button):
        self.current_role_filter = "Flank"
        new_description = self._generate_description()
        await interaction.response.edit_message(content=None, embed=discord.Embed(
            title=f"Top Champions for {self.target_user_name}",
            description=new_description,
            color=discord.Color.purple()
        ), view=self)

    @discord.ui.button(label="Tank", style=discord.ButtonStyle.secondary, row=3)
    async def tank_button(self, interaction: discord.Interaction, button: Button):
        self.current_role_filter = "Tank"
        new_description = self._generate_description()
        await interaction.response.edit_message(content=None, embed=discord.Embed(
            title=f"Top Champions for {self.target_user_name}",
            description=new_description,
            color=discord.Color.orange()
        ), view=self)

    @discord.ui.button(label="Support", style=discord.ButtonStyle.secondary, row=3)
    async def support_button(self, interaction: discord.Interaction, button: Button):
        self.current_role_filter = "Support"
        new_description = self._generate_description()
        await interaction.response.edit_message(content=None, embed=discord.Embed(
            title=f"Top Champions for {self.target_user_name}",
            description=new_description,
            color=discord.Color.green()
        ), view=self)


# --- USER COMMANDS ---

# NEW: The !top command with interactive UI
@bot.command(name="top", help="Shows an interactive breakdown of a player's champions.")
async def top_cmd(ctx, user: PlayerConverter = None):
    target_user = user or ctx.author
    player_id = get_player_id(str(target_user.id))

    if not player_id:
        await ctx.send(f"No stats found for {target_user.display_name}. They may need to `!link` their IGN.")
        return
        
    all_champ_data = get_all_champion_stats(player_id)
    if not all_champ_data:
        await ctx.send(f"No champion stats found for {target_user.display_name}.")
        return

    # Create and send the initial view
    view = TopChampsView(ctx.author.id, all_champ_data, target_user.display_name)
    initial_description = view._generate_description()
    
    embed = discord.Embed(
        title=f"Top Champions for {target_user.display_name}",
        description=initial_description,
        color=discord.Color.blue()
    )
    
    # Send the message and store it on the view for later editing
    view.message = await ctx.send(
        "This message will self-destruct in 90 seconds.\nSelect an option:",
        embed=embed,
        view=view,
        delete_after=90
    )



@bot.command(name="history", help="Shows recent matches. Ex: !history 10, !history @user 5")
async def history_cmd(ctx, *args):
    target_user = ctx.author
    limit = 30
    user_input_parts = []

    if args:
        if args[-1].isdigit():
            limit = int(args[-1])
            user_input_parts = args[:-1]
        else:
            user_input_parts = args

    if user_input_parts:
        try:
            target_user = await PlayerConverter().convert(ctx, " ".join(user_input_parts))
        except commands.BadArgument as e:
            await ctx.send(e)
            return

    limit = max(1, min(limit, 50))

    player_id = get_player_id(str(target_user.id))
    if not player_id:
        await ctx.send(
            f"No history found for {target_user.display_name}. They may need to link their IGN using `!link <ign>`."
        )
        return

    history = get_match_history(player_id, limit)
    if not history:
        await ctx.send(f"No match history found for {target_user.display_name}.")
        return

    # Header - Changed "Result" to "W/L" and adjusted padding
    header = f"{'W/L':<5} {'Champion':<16} {'Time':<6} {'Match ID':<10} {'KDA':<6} {'Raw KDA':<11} {'Map':<20}"
    lines = [header]

    for match in history:
        map_name, champ, k, d, a, result, match_id, match_time = match

        symbol = "🏆" if result == "W" else "💔"
        kda_ratio = f"{(k + a) / max(1, d):.2f}"
        time_str = f"{match_time}:00"
        raw_kda_str = f"({k}/{d}/{a})"

        # Truncate long names
        champ_str = champ if len(champ) <= 16 else champ[:15] + "…"
        map_str = map_name if len(map_name) <= 20 else map_name[:19] + "…"

        # Data Row - Adjusted padding to match the new header
        line = f"{symbol:<4} {champ_str:<16} {time_str:<6} {match_id:<10} {kda_ratio:<6} {raw_kda_str:<11} {map_str:<20}"
        lines.append(line)

    # Final message
    output = f"Last {len(history)} Matches for {target_user.display_name}\n\n" + "\n".join(lines)
    await ctx.send(f"```diff\n{output}\n```")


# --- DETAILED HELP MESSAGE FOR LEADERBOARD ---
LEADERBOARD_HELP = """
Shows player rankings for various stats.

**Usage:** `!leaderboard [stat] [limit] [-b]`

**Arguments:**
- `[stat]`: The statistic to rank by. Defaults to `winrate`.
- `[limit]`: The number of players to show. Defaults to `20`.
- `[-b]`: Optional flag to show the bottom of the leaderboard instead of the top.

**Available Stats:**
- `winrate` (or `wr`): Overall Winrate
- `kda`: Kill/Death/Assist Ratio
- `dmg` (or `dpm`): Damage per Minute
- `taken_pm`: Damage Taken per Minute
- `heal_pm`: Healing per Minute
- `self_heal_pm`: Self Healing per Minute
- `creds_pm`: Credits per Minute
- `avg_dmg`: Average Damage per Match
- `avg_taken`: Average Damage Taken per Match
- `delta`: Average Damage Delta (Dealt - Taken)
- `avg_heal`: Average Healing per Match
- `avg_self_heal`: Average Self Healing per Match
- `avg_shield`: Average Shielding per Match
- `avg_creds`: Average Credits per Match
- `obj_time`: Average Objective Time per Match

**Examples:**
- `!lb`: Shows the top 20 players by Winrate.
- `!lb kda 10`: Shows the top 10 players by KDA.
- `!lb dmg`: Shows the top 20 players by Damage/Min.
- `!lb obj_time 5 -b`: Shows the bottom 5 players for Objective Time.
"""

@bot.command(name="leaderboard", aliases=["lb"], help=LEADERBOARD_HELP)
async def leaderboard_cmd(ctx, *args):
    # --- Stat Mapping (defined inside the function as requested) ---
    # Maps user aliases to: (Display Name, data_key_for_db_function, formatting_function)
    stat_map = {
        "winrate": ("Winrate", "winrate", lambda v, s: f"{v:.2f}% ({s['wins']}-{s['losses']})"),
        "kda": ("KDA Ratio", "kda", lambda v, s: f"{v:.2f} ({s['k']}/{s['d']}/{s['a']})"),
        "dmg_pm": ("Damage/Min", "damage_dealt_pm", lambda v, s: f"{int(v):,}"),
        "taken_pm": ("Damage Taken/Min", "damage_taken_pm", lambda v, s: f"{int(v):,}"),
        "heal_pm": ("Healing/Min", "healing_pm", lambda v, s: f"{int(v):,}"),
        "self_heal_pm": ("Self Healing/Min", "self_healing_pm", lambda v, s: f"{int(v):,}"),
        "creds_pm": ("Credits/Min", "credits_pm", lambda v, s: f"{int(v):,}"),
        "avg_dmg": ("AVG Damage Dealt", "avg_damage_dealt", lambda v, s: f"{int(v):,}"),
        "avg_taken": ("AVG Damage Taken", "avg_damage_taken", lambda v, s: f"{int(v):,}"),
        "delta": ("AVG Damage Delta", "damage_delta", lambda v, s: f"{int(v):,}"),
        "avg_heal": ("AVG Healing", "avg_healing", lambda v, s: f"{int(v):,}"),
        "avg_self_heal": ("AVG Self Healing", "avg_self_healing", lambda v, s: f"{int(v):,}"),
        "avg_shield": ("AVG Shielding", "avg_shielding", lambda v, s: f"{int(v):,}"),
        "avg_creds": ("AVG Credits", "avg_credits", lambda v, s: f"{int(v):,}"),
        "obj_time": ("AVG Objective Time", "obj_time", lambda v, s: f"{int(v):,}s"),
        # Convenience aliases
        "dmg": ("Damage/Min", "damage_dealt_pm", lambda v, s: f"{int(v):,}"),
        "dpm": ("Damage/Min", "damage_dealt_pm", lambda v, s: f"{int(v):,}"),
        "wr": ("Winrate", "winrate", lambda v, s: f"{v:.2f}% ({s['wins']}-{s['losses']})"),
    }

    # --- 1. Argument Parsing ---
    stat_alias = "winrate"
    limit = 20
    show_bottom = False
    args = list(args)

    if "-b" in args:
        show_bottom = True
        args.remove("-b")

    if args:
        stat_alias = args.pop(0).lower()
    if args and args[0].isdigit():
        limit = int(args.pop(0))

    limit = max(1, min(limit, 50))
    
    # --- 2. Validate Stat ---
    if stat_alias not in stat_map:
        valid_stats = ", ".join(f"`{s}`" for s in sorted(stat_map.keys()) if len(s) > 2)
        await ctx.send(f"Invalid stat. Please choose from: {valid_stats}")
        return

    display_name, data_key, formatter = stat_map[stat_alias]

    # --- 3. Fetch Data ---
    leaderboard_data = get_leaderboard(data_key, limit, show_bottom)
    if not leaderboard_data:
        await ctx.send(f"Could not generate a leaderboard for `{display_name}`. Not enough qualified player data may be available.")
        return

    # --- 4. Build Embed ---
    embed_title = f"🏆 {'Bottom' if show_bottom else 'Top'} {len(leaderboard_data)} Players by {display_name}"
    embed_color = 0xE74C3C if show_bottom else 0x2ECC71
    embed = discord.Embed(title=embed_title, color=embed_color)
    embed.set_footer(text="Players must have at least 20 games to qualify for most leaderboards.")

    description = []
    for i, data_row in enumerate(leaderboard_data):
        discord_id = data_row['discord_id']
        value = data_row['value']

        member = ctx.guild.get_member(int(discord_id))
        # --- THIS IS THE CORRECTED LINE ---
        name = member.display_name if member else data_row['player_ign']
        
        rank = (data_row['total_players'] - i) if show_bottom else (i + 1)
        formatted_value = formatter(value, data_row)

        description.append(f"`{rank:2}.` **{name}** - {formatted_value}")
    
    embed.description = "\n".join(description)
    await ctx.send(embed=embed)

# RENAMED: The old !top_champs command is now legacy
@bot.command(name="legacy_top_champs", help="[LEGACY] Get top 5 champs for a player (non-interactive).", hidden=True)
async def legacy_top_champs_cmd(ctx, user: PlayerConverter = None):
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
            f"**Dmg/min:** {int(champ['damage']):,} | **Heal/min:** {int(champ['healing']):,}"
        )
        embed.add_field(name=name, value=value, inline=False)
        
    await ctx.send(embed=embed)

@bot.command(name="compare", help="Compare stats between two players.")
async def compare_cmd(ctx, user1: PlayerConverter, user2: PlayerConverter = None):
    # If user2 is not provided, default to the command author
    user2 = user2 or ctx.author

    if user1 == user2:
        await ctx.send("You can't compare a player to themselves!")
        return
        
    result = compare_players(str(user1.id), str(user2.id))
    if not result:
        await ctx.send("Could not find stats for one or both players. Ensure they have linked their IGNs.")
        return

    p1_stats = result["player1"]
    p2_stats = result["player2"]

    # --- Create the Embed ---
    embed = discord.Embed(
        title=f"Head-to-Head: {user1.name} vs {user2.name}",
        description="Here's how their stats stack up.",
        color=0x3498DB
    )
    # CORRECTED LINE: Use .display_avatar.url instead of .avatar_url
    embed.set_author(name=user1.display_name, icon_url=user1.display_avatar.url)
    
    # CORRECTED LINE: Use .display_avatar.url instead of .avatar_url
    embed.set_footer(text=f"Compared with {user2.display_name}", icon_url=user2.display_avatar.url)

    # --- Helper logic for adding winner emojis ---
    def get_emoji(stat1, stat2):
        if stat1 > stat2:
            return "👑", ""
        elif stat2 > stat1:
            return "", "👑"
        else:
            return "🤝", "🤝"

    wr_e1, wr_e2 = get_emoji(p1_stats['winrate'], p2_stats['winrate'])
    kda_e1, kda_e2 = get_emoji(p1_stats['kda_ratio'], p2_stats['kda_ratio'])
    dmg_e1, dmg_e2 = get_emoji(p1_stats['damage_dealt_pm'], p2_stats['damage_dealt_pm'])

    # --- Stat-by-Stat Comparison Fields ---
    embed.add_field(
        name="📊 Winrate & Games Played",
        value=(
            f"{wr_e1} `{user1.display_name}`: **{p1_stats['winrate']:.2f}%** ({p1_stats['games']} games)\n"
            f"{wr_e2} `{user2.display_name}`: **{p2_stats['winrate']:.2f}%** ({p2_stats['games']} games)"
        ),
        inline=False
    )
    embed.add_field(
        name="⚔️ KDA Ratio",
        value=(
            f"{kda_e1} `{user1.display_name}`: **{p1_stats['kda_ratio']:.2f}**\n"
            f"{kda_e2} `{user2.display_name}`: **{p2_stats['kda_ratio']:.2f}**"
        ),
        inline=True
    )
    embed.add_field(
        name="💥 Damage per Minute",
        value=(
            f"{dmg_e1} `{user1.display_name}`: **{int(p1_stats['damage_dealt_pm']):,}**\n"
            f"{dmg_e2} `{user2.display_name}`: **{int(p2_stats['damage_dealt_pm']):,}**"
        ),
        inline=True
    )
    
    # --- Top Champions ---
    p1_top_champ_str = "N/A"
    if result['top_champs1']:
        top_champ = result['top_champs1'][0]
        p1_top_champ_str = f"**{top_champ['champ']}** ({top_champ['winrate']:.1f}% WR over {top_champ['games']} games)"

    p2_top_champ_str = "N/A"
    if result['top_champs2']:
        top_champ = result['top_champs2'][0]
        p2_top_champ_str = f"**{top_champ['champ']}** ({top_champ['winrate']:.1f}% WR over {top_champ['games']} games)"

    embed.add_field(
        name="🏆 Top Champion",
        value=(
            f"`{user1.display_name}`: {p1_top_champ_str}\n"
            f"`{user2.display_name}`: {p2_top_champ_str}"
        ),
        inline=False
    )

    # --- Synergy Section (with clearer explanation) ---
    embed.add_field(
        name="🤝 Synergy & Rivalry",
        value=(
            f"**Playing Together:** `{result['with_games']}` games with a **{result['with_winrate']:.1f}%** winrate.\n"
            f"**Playing Against:** When matched up, `{user1.display_name}` wins **{result['against_winrate']:.1f}%** of the time across `{result['against_games']}` games."
        ),
        inline=False
    )
    
    await ctx.send(embed=embed)

# --- DATA INGESTION LOGIC & EVENT HANDLERS ---

def parse_match_textbox(text):
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
        
        del parts[3]
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
        await bot.process_commands(message)
        try:
            if isinstance(message.channel, discord.TextChannel) and message.channel.name in ALLOWED_CHANNELS:
                if message.author.name == "NeatQueue" and message.author.discriminator == "0850" and message.embeds:
                    for embed in message.embeds:
                        queue_number_match = re.search(r"Queue #?(\d+)", embed.title or "") or re.search(r"Queue #?(\d+)", embed.description or "")
                        if queue_number_match:
                            queue_number = queue_number_match.group(1)
                            insert_embed(queue_number, embed.to_dict())
                
                elif message.author.name == "PaladinsAssistant" and message.author.discriminator == "2894":
                    match_text = message.content or (message.embeds[0].description if message.embeds else None)
                    if match_text:
                        view = QueueNumView(match_text, message.author.id)
                        await message.reply("Admins: Click to enter the queue number for this match:", view=view)
        except Exception as e:
            print(f"Error in on_message processing: {e}")
        return

    await bot.process_commands(message)

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





