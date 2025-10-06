# cogs/stats.py

import discord
from discord.ext import commands
import os
import time
from utils.converters import PlayerConverter
from utils.views import TopChampsView
from core.constants import CHAMPION_ROLES, ROLE_ALIASES
from db import (
    get_player_id,
    get_player_stats,
    get_champion_name,
    get_all_champion_stats,
    get_match_history,
    get_leaderboard,
    compare_players,
    get_top_champs,
)


def get_champion_icon_path(champion_name):
    """Formats a champion name into a valid file path for its icon."""
    formatted_name = champion_name.lower().replace(" ", "_").replace("'", "")
    return os.path.join("icons", "champ_icons", f"{formatted_name}.png")


class Stats(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @commands.command(name="stats", help="Get stats for a player, with an optional champion or role filter.")
    async def stats_cmd(self, ctx, user: PlayerConverter = None, *, filter_str: str = None):
        start_time = time.monotonic()
        target_user = user or ctx.author
        
        player_id = get_player_id(str(target_user.id))
        if not player_id:
            await ctx.send(f"No stats found for {target_user.display_name}. They may need to link their IGN using `!link <ign>`.")
            return

        icon_file = None
        embed = discord.Embed(color=discord.Color.blue())
        
        # --- Filtered Stats Logic (Champion or Role) ---
        if filter_str:
            filter_lower = filter_str.lower()
            
            # --- ROLE-BASED STATS ---
            if filter_lower in ROLE_ALIASES:
                role_name = ROLE_ALIASES[filter_lower]
                champs_in_role = [champ for champ, r_name in CHAMPION_ROLES.items() if r_name == role_name]
                
                if not champs_in_role:
                    await ctx.send("Internal error: Could not find champions for that role.")
                    return

                role_stats = get_player_stats(player_id, champions=champs_in_role)

                if not role_stats or role_stats["games"] == 0:
                    await ctx.send(f"No stats found for {target_user.display_name} playing the '{role_name}' role.")
                    return

                embed.set_author(name=f"{target_user.display_name}'s Stats", icon_url=target_user.display_avatar.url)
                
                data = {
                    f"--- Role: {role_name} ({role_stats['games']} games) ---": "",
                    "Winrate": f"{role_stats['winrate']:.2f}% ({role_stats['wins']}-{role_stats['losses']})",
                    "KDA": f"{role_stats['kda_ratio']:.2f} ({role_stats['raw_k']}/{role_stats['raw_d']}/{role_stats['raw_a']})",
                    "--- Per Minute ---": "",
                    "Kills/Min": f"{role_stats['kills_pm']:.2f}",
                    "Deaths/Min": f"{role_stats['deaths_pm']:.2f}",
                    "Damage/Min": f"{int(role_stats['damage_dealt_pm']):,}",
                    "Damage Taken/Min": f"{int(role_stats['damage_taken_pm']):,}",
                    "Healing/Min": f"{int(role_stats['healing_pm']):,}",
                    "Self Healing/Min": f"{int(role_stats['self_healing_pm']):,}",
                    "Credits/Min": f"{int(role_stats['credits_pm']):,}",
                    "--- Per Match ---": "",
                    "AVG Kills": f"{role_stats['avg_kills']:.2f}",
                    "AVG Deaths": f"{role_stats['avg_deaths']:.2f}",
                    "AVG Damage Dealt": f"{int(role_stats['avg_damage_dealt']):,}",
                    "AVG Damage Taken": f"{int(role_stats['avg_damage_taken']):,}",
                    "AVG Damage Delta": f"{int(role_stats['damage_delta']):,}",
                    "AVG Healing": f"{int(role_stats['avg_healing']):,}",
                    "AVG Self Healing": f"{int(role_stats['avg_self_healing']):,}",
                    "AVG Shielding": f"{int(role_stats['avg_shielding']):,}",
                    "AVG Credits": f"{int(role_stats['avg_credits']):,}",
                    "AVG Objective Time": f"{int(role_stats['obj_time']):,}",
                }
                max_label_len = max(len(label) for label in data.keys())
                stat_lines = [f"{label + ':':<{max_label_len + 2}} {value}" if value else label for label, value in data.items()]
                embed.description = "```\n" + "\n".join(stat_lines) + "\n```"

            # --- CHAMPION-BASED STATS ---
            else:
                full_champion_name = get_champion_name(player_id, filter_str)
                if not full_champion_name:
                    await ctx.send(f"No stats found for {target_user.display_name} on a champion or role matching '{filter_str}'.")
                    return
                
                champ_stats = get_player_stats(player_id, champions=[full_champion_name])
                if not champ_stats or champ_stats["games"] == 0:
                    await ctx.send(f"No stats found for {target_user.display_name} on {full_champion_name}.")
                    return

                global_stats = get_player_stats(player_id)

                embed.set_author(name=f"{target_user.display_name}'s Stats", icon_url=target_user.display_avatar.url)
                icon_path = get_champion_icon_path(full_champion_name)
                if os.path.exists(icon_path):
                    icon_file = discord.File(icon_path, filename="icon.png")
                    embed.set_thumbnail(url="attachment://icon.png")
                
                champ_data = {
                    f"--- Champion: {full_champion_name} ---": "",
                    "Winrate": f"{champ_stats['winrate']:.2f}% ({champ_stats['wins']}-{champ_stats['losses']})",
                    "KDA": f"{champ_stats['kda_ratio']:.2f} ({champ_stats['raw_k']}/{champ_stats['raw_d']}/{champ_stats['raw_a']})",
                    "Kills/Min": f"{champ_stats['kills_pm']:.2f}",
                    "Deaths/Min": f"{champ_stats['deaths_pm']:.2f}",
                    "Damage/Min": f"{int(champ_stats['damage_dealt_pm']):,}",
                    "Damage Taken/Min": f"{int(champ_stats['damage_taken_pm']):,}",
                    "Healing/Min": f"{int(champ_stats['healing_pm']):,}",
                    "Self Healing/Min": f"{int(champ_stats['self_healing_pm']):,}",
                    "Credits/Min": f"{int(champ_stats['credits_pm']):,}",
                    "AVG Kills": f"{champ_stats['avg_kills']:.2f}",
                    "AVG Deaths": f"{champ_stats['avg_deaths']:.2f}",
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

        # --- GENERAL STATS (No Filter) ---
        else:
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
                "Kills/Min": f"{stats['kills_pm']:.2f}",
                "Deaths/Min": f"{stats['deaths_pm']:.2f}",
                "Damage/Min": f"{int(stats['damage_dealt_pm']):,}",
                "Damage Taken/Min": f"{int(stats['damage_taken_pm']):,}",
                "Healing/Min": f"{int(stats['healing_pm']):,}",
                "Self Healing/Min": f"{int(stats['self_healing_pm']):,}",
                "Credits/Min": f"{int(stats['credits_pm']):,}",
                "--- Per Match ---": "",
                "AVG Kills": f"{stats['avg_kills']:.2f}",
                "AVG Deaths": f"{stats['avg_deaths']:.2f}",
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
        if not filter_str:
            footer_text = f"Player ID: {target_user.id}    •   {footer_text}"
        embed.set_footer(text=footer_text, icon_url=ctx.guild.icon.url if ctx.guild.icon else None)

        await ctx.send(embed=embed, file=icon_file)

    @commands.command(name="top", help="Shows an interactive breakdown of a player's champions.")
    async def top_cmd(self, ctx, user: PlayerConverter = None):
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

    @commands.command(name="history", help="Shows recent matches. Ex: !history 10, !history @user 5 | Max 20")
    async def history_cmd(self, ctx, *args):
        target_user = ctx.author
        limit = 20  # Default to 20
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

        # MODIFIED: The maximum number of matches is now capped at 20.
        limit = max(1, min(limit, 20))

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

        header = f"{'W/L':<5} {'Champion':<16} {'Time':<6} {'Match ID':<10} {'KDA':<6} {'Raw KDA':<11} {'Map':<20}"
        lines = [header]

        for match in history:
            map_name, champ, k, d, a, result, match_id, match_time = match
            symbol = "🏆" if result == "W" else "💔"
            kda_ratio = f"{(k + a) / max(1, d):.2f}"
            time_str = f"{match_time}:00"
            raw_kda_str = f"({k}/{d}/{a})"
            champ_str = champ if len(champ) <= 16 else champ[:15] + "…"
            map_str = map_name if len(map_name) <= 20 else map_name[:19] + "…"
            line = f"{symbol:<4} {champ_str:<16} {time_str:<6} {match_id:<10} {kda_ratio:<6} {raw_kda_str:<11} {map_str:<20}"
            lines.append(line)

        output = f"Last {len(history)} Matches for {target_user.display_name}\n\n" + "\n".join(lines)
        await ctx.send(f"```diff\n{output}\n```")

    LEADERBOARD_HELP = """
Shows player rankings, with optional filters for champions or roles.

**Usage:** `!leaderboard [stat] [champion/role] [limit] [-b] [-m <games>]`

**Arguments:**
- `[stat]`: The statistic to rank by. Defaults to `winrate`.
- `[champion/role]`: Filter by a champion name (e.g., `nando`) or a role (`tank`, `support`).
- `[limit]`: The number of players to show. Defaults to `20`.
- `[-b]`: Optional flag to show the bottom of the leaderboard.
- `[-m <games>]`: Optional flag to set a minimum number of games played to qualify. Defaults to 1 (all players).

**Available Stats:**
- `winrate` (or `wr`): Overall Winrate
- `kda`: Kill/Death/Assist Ratio
- `kpm`: Kills per Minute
- `deaths_pm`: Deaths per Minute
- `dmg` (or `dpm`): Damage per Minute
- `taken_pm`: Damage Taken per Minute
- `heal_pm`: Healing per Minute (Defaults to Supports)
- `self_heal_pm`: Self Healing per Minute
- `creds_pm`: Credits per Minute
- `avg_kills`: Average Kills per Match
- `avg_deaths`: Average Deaths per Match
- `avg_dmg`: Average Damage per Match
- `avg_taken`: Average Damage Taken per Match
- `delta`: Average Damage Delta (Dealt - Taken)
- `avg_heal`: Average Healing per Match (Defaults to Supports)
- `avg_self_heal`: Average Self Healing per Match
- `avg_shield`: Average Shielding per Match
- `avg_creds`: Average Credits per Match
- `obj_time`: Average Objective Time per Match

**Examples:**
- `!lb heal_pm`: Top 20 healers on Support champions.
- `!lb heal_pm tank`: Top 20 healers on Tank champions.
"""

    @commands.command(name="leaderboard", aliases=["lb"], help=LEADERBOARD_HELP)
    async def leaderboard_cmd(self, ctx, *args):
        # --- Stat Mapping (Complete with all stats) ---
        stat_map = {
            "winrate": ("Winrate", "winrate", lambda v, s: f"{v:.2f}% ({s['wins']}-{s['losses']})"),
            "kda": ("KDA Ratio", "kda", lambda v, s: f"{v:.2f} ({s['k']}/{s['d']}/{s['a']})"),
            "kpm": ("Kills/Min", "kills_pm", lambda v, s: f"{v:.2f}"),
            "deaths_pm": ("Deaths/Min", "deaths_pm", lambda v, s: f"{v:.2f}"),
            "dmg_pm": ("Damage/Min", "damage_dealt_pm", lambda v, s: f"{int(v):,}"),
            "taken_pm": ("Damage Taken/Min", "damage_taken_pm", lambda v, s: f"{int(v):,}"),
            "heal_pm": ("Healing/Min", "healing_pm", lambda v, s: f"{int(v):,}"),
            "self_heal_pm": ("Self Healing/Min", "self_healing_pm", lambda v, s: f"{int(v):,}"),
            "creds_pm": ("Credits/Min", "credits_pm", lambda v, s: f"{int(v):,}"),
            "avg_kills": ("AVG Kills", "avg_kills", lambda v, s: f"{v:.2f}"),
            "avg_deaths": ("AVG Deaths", "avg_deaths", lambda v, s: f"{v:.2f}"),
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
            "hpm": ("Healing/Min", "healing_pm", lambda v, s: f"{int(v):,}"),
        }
        
        # --- 1. Argument Parsing ---
        stat_alias = "winrate"
        limit = 20
        show_bottom = False
        champion_filter = None
        role_filter = None
        min_games = None
        
        valid_roles = {role.lower() for role in CHAMPION_ROLES.values()}
        role_aliases = {'dmg': 'damage'}
        
        unprocessed_args = []
        args = list(args)

        i = 0
        while i < len(args):
            arg = args[i]
            
            if arg.lower() == '-m':
                if i + 1 < len(args) and args[i+1].isdigit():
                    min_games = max(1, int(args[i+1]))
                    i += 2
                    continue
                i += 1
                continue

            if arg.lower() == "-b":
                show_bottom = True
            elif arg.lower() in stat_map:
                stat_alias = arg.lower()
            elif arg.isdigit():
                limit = int(arg)
            else:
                unprocessed_args.append(arg)
            i += 1
        
        if unprocessed_args:
            full_filter_str = " ".join(unprocessed_args).lower()
            
            matched_role = None
            if full_filter_str in role_aliases:
                matched_role = role_aliases[full_filter_str]
            else:
                matched_role = next((role for role in valid_roles if role.startswith(full_filter_str)), None)

            if matched_role:
                role_filter = matched_role.capitalize()
            else:
                champion_filter = full_filter_str

        limit = max(1, min(limit, 50))
        
        if min_games is None:
            min_games = 1
        
        # --- 2. Fetch Data ---
        display_name, data_key, formatter = stat_map[stat_alias]
        leaderboard_data = get_leaderboard(
            data_key, limit, show_bottom,
            champion=champion_filter, role=role_filter, min_games=min_games
        )
        if not leaderboard_data:
            filter_name = champion_filter.title() if champion_filter else role_filter if role_filter else ""
            # Add a note if it's a healing stat and no filter was applied
            if not filter_name and data_key in ["healing_pm", "avg_healing"]:
                 filter_name = "Supports"
            filter_msg = f" as {filter_name}" if filter_name else ""
            await ctx.send(f"Could not generate a leaderboard for `{display_name}`{filter_msg}. No qualified player data found.")
            return

        # --- 3. Build Embed ---
        filter_text = ""
        if champion_filter:
            full_champ_name = next((name for name in CHAMPION_ROLES if champion_filter.lower() in name.lower()), champion_filter)
            filter_text = f" on {full_champ_name.title()}"
        elif role_filter:
            filter_text = f" as {role_filter}"

        embed_title = f"🏆 {'Bottom' if show_bottom else 'Top'} {len(leaderboard_data)} Players by {display_name}{filter_text}"
        embed_color = 0xE74C3C if show_bottom else 0x2ECC71
        embed = discord.Embed(title=embed_title, color=embed_color)
        
        if min_games > 1:
            embed.set_footer(text=f"Players must have at least {min_games} games with the specified filter to qualify.")

        description = []
        for i, data_row in enumerate(leaderboard_data):
            discord_id = data_row['discord_id']
            value = data_row['value']
            member = ctx.guild.get_member(int(discord_id))
            name = member.display_name if member else data_row['player_ign']
            
            rank = (data_row['total_players'] - i) if show_bottom else (i + 1)
            formatted_value = formatter(value, data_row)

            description.append(f"`{rank:2}.` **{name}** - {formatted_value}")
        
        embed.description = "\n".join(description)
        await ctx.send(embed=embed)

    @commands.command(name="compare", help="Compare stats between two players.")
    async def compare_cmd(self, ctx, user1: PlayerConverter, user2: PlayerConverter = None):
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
        embed.set_author(name=user1.display_name, icon_url=user1.display_avatar.url)
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


async def setup(bot):
    await bot.add_cog(Stats(bot))

