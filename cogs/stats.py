# cogs/stats.py

import discord
from discord.ext import commands
import os
import re
import time
from utils.converters import PlayerConverter, resolve_player_id
from utils.views import TopChampsView
from core.constants import CHAMPION_ROLES, get_champions_for_role, resolve_champion_name, resolve_role_name
from db import (
    get_player_stats,
    get_champion_name,
    get_all_champion_stats,
    get_player_champion_stats,
    get_match_history,
    get_player_map_winrates,
    get_champion_map_winrates,
    get_champion_overall_stats,
    get_leaderboard,
    get_champion_leaderboard,
    compare_by_player_ids,
    get_top_champs,
    resolve_map_name,
)


def _is_unlinked(target_user):
    return getattr(target_user, "is_unlinked", False)


def _avatar_url(target_user):
    avatar = getattr(target_user, "display_avatar", None)
    return getattr(avatar, "url", None) if avatar else None


def get_champion_icon_path(champion_name):
    """Formats a champion name into a valid file path for its icon."""
    formatted_name = champion_name.lower().replace(" ", "_").replace("'", "")
    return os.path.join("icons", "champ_icons", f"{formatted_name}.png")


RESULT_FILTER_ALIASES = {
    "win": "wins", "wins": "wins", "won": "wins", "wonly": "wins",
    "winonly": "wins", "winsonly": "wins",
    "loss": "losses", "losses": "losses", "lost": "losses", "lonly": "losses",
    "lossonly": "losses", "lossesonly": "losses",
}

TEAM_FILTER_ALIASES = {
    "team1": 1, "t1": 1, "first": 1, "fp": 1, "firstpick": 1,
    "team2": 2, "t2": 2, "second": 2, "lastpick": 2, "lp": 2,
}

SCORE_FILTER_ALIASES = {
    "close": "close", "closegame": "close",
    "stomp": "stomp", "stomps": "stomp",
    "sweep": "sweep", "sweeps": "sweep",
}

FILTER_KEYWORDS = {
    "map", "with", "against",
    *RESULT_FILTER_ALIASES.keys(),
    *TEAM_FILTER_ALIASES.keys(),
    *SCORE_FILTER_ALIASES.keys(),
}


def _compact_arg(arg):
    return str(arg).lower().replace("_", "").replace("-", "").replace("'", "")


def _parse_scoreline(arg):
    if "-" not in arg:
        return None
    left, right = arg.split("-", 1)
    if left.isdigit() and right.isdigit():
        return int(left), int(right)
    return None


def _filter_summary(filters):
    if not filters:
        return []

    labels = []
    if filters.get("map"):
        labels.append(f"Map: {filters['map']}")
    if filters.get("result") == "wins":
        labels.append("Wins Only")
    elif filters.get("result") == "losses":
        labels.append("Losses Only")
    if filters.get("team") == 1:
        labels.append("Team 1")
    elif filters.get("team") == 2:
        labels.append("Team 2")
    if filters.get("scoreline"):
        labels.append(f"Scoreline {filters['scoreline'][0]}-{filters['scoreline'][1]}")
    elif filters.get("score_category") == "close":
        labels.append("Close Games")
    elif filters.get("score_category") == "stomp":
        labels.append("Stomps")
    elif filters.get("score_category") == "sweep":
        labels.append("Sweeps")
    if filters.get("with_player_name"):
        labels.append(f"With {filters['with_player_name']}")
    if filters.get("against_player_name"):
        labels.append(f"Against {filters['against_player_name']}")
    return labels


def _title_filter_suffix(filters):
    parts = []
    if filters.get("map"):
        parts.append(f"on {filters['map']}")
    if filters.get("result") == "wins":
        parts.append("in Wins")
    elif filters.get("result") == "losses":
        parts.append("in Losses")
    if filters.get("team") == 1:
        parts.append("on Team 1")
    elif filters.get("team") == 2:
        parts.append("on Team 2")
    if filters.get("scoreline"):
        parts.append(f"at {filters['scoreline'][0]}-{filters['scoreline'][1]}")
    elif filters.get("score_category") == "close":
        parts.append("in Close Games")
    elif filters.get("score_category") == "stomp":
        parts.append("in Stomps")
    elif filters.get("score_category") == "sweep":
        parts.append("in Sweeps")
    if filters.get("with_player_name"):
        parts.append(f"with {filters['with_player_name']}")
    if filters.get("against_player_name"):
        parts.append(f"against {filters['against_player_name']}")
    return " " + " ".join(parts) if parts else ""


async def _extract_match_filters(ctx, args):
    args = list(args)
    filters = {}
    remaining = []
    i = 0

    while i < len(args):
        arg = str(args[i])
        raw_key = arg.lower()
        key = _compact_arg(arg)

        if key == "map":
            resolved_map = None
            consumed_until = i + 1
            for end in range(len(args), i + 1, -1):
                candidate = " ".join(str(part) for part in args[i + 1:end])
                resolved_map = resolve_map_name(candidate)
                if resolved_map:
                    consumed_until = end
                    break
            if not resolved_map:
                return remaining, filters, f"Could not find a map matching `{arg if i + 1 >= len(args) else args[i + 1]}`."
            filters["map"] = resolved_map
            i = consumed_until
            continue

        if key == "only":
            i += 1
            continue

        if key in RESULT_FILTER_ALIASES:
            filters["result"] = RESULT_FILTER_ALIASES[key]
            i += 1
            continue

        if key == "team" and i + 1 < len(args) and _compact_arg(args[i + 1]) in {"1", "2"}:
            filters["team"] = int(_compact_arg(args[i + 1]))
            i += 2
            continue

        if key in {"first", "last"} and i + 1 < len(args) and _compact_arg(args[i + 1]) == "pick":
            filters["team"] = 1 if key == "first" else 2
            i += 2
            continue

        if key in TEAM_FILTER_ALIASES:
            filters["team"] = TEAM_FILTER_ALIASES[key]
            i += 1
            continue

        if key in SCORE_FILTER_ALIASES:
            filters["score_category"] = SCORE_FILTER_ALIASES[key]
            filters.pop("scoreline", None)
            i += 1
            continue

        scoreline = _parse_scoreline(raw_key)
        if scoreline:
            filters["scoreline"] = scoreline
            filters.pop("score_category", None)
            i += 1
            continue

        if key in {"with", "against"}:
            if i + 1 >= len(args):
                return remaining, filters, f"`{arg}` needs a player after it."
            player_arg = str(args[i + 1])
            try:
                player = await PlayerConverter().convert(ctx, player_arg)
            except commands.BadArgument as exc:
                return remaining, filters, str(exc)

            player_id = resolve_player_id(player)
            if not player_id:
                return remaining, filters, f"No stats found for `{player_arg}`."

            filters[f"{key}_player_id"] = player_id
            filters[f"{key}_player_name"] = player.display_name
            i += 2
            continue

        remaining.append(arg)
        i += 1

    return remaining, filters, None


def _resolve_leading_map(args):
    args = list(args)
    for end in range(len(args), 0, -1):
        candidate = " ".join(str(part) for part in args[:end])
        resolved_map = resolve_map_name(candidate)
        if resolved_map:
            return resolved_map, args[end:]
    return None, args


def _format_stat_block(data):
    label_width = 19
    lines = []
    for label, value in data.items():
        if value:
            lines.append(f"{label + ':':<{label_width}} {value}")
        else:
            lines.append(label)
    return lines


def _split_champion_pair(args):
    for split_at in range(1, len(args)):
        first = resolve_champion_name(" ".join(args[:split_at]))
        second = resolve_champion_name(" ".join(args[split_at:]))
        if first and second:
            return first, second
    return None, None


class Stats(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    def _examples_embed(self, topic=None):
        topic_key = _compact_arg(topic or "overview")
        topic_titles = {
            "overview": "Command Examples",
            "stats": "Stats Examples",
            "top": "Top Examples",
            "lb": "Leaderboard Examples",
            "leaderboard": "Leaderboard Examples",
            "clb": "Champion Leaderboard Examples",
            "champlb": "Champion Leaderboard Examples",
            "championleaderboard": "Champion Leaderboard Examples",
            "map": "Map Examples",
            "mapwr": "Map Winrate Examples",
            "champmapwr": "Champion Map Winrate Examples",
            "champcompare": "Champion Compare Examples",
            "filters": "Filter Examples",
            "filter": "Filter Examples",
            "aliases": "Alias Examples",
            "alias": "Alias Examples",
        }
        examples_by_topic = {
            "overview": [
                "`!examples stats` - Player stat examples.",
                "`!examples top` - Personal champion table examples.",
                "`!examples mapwr` - Player map winrate examples.",
                "`!examples champmapwr` - Champion map winrate examples.",
                "`!examples champcompare` - Champion comparison examples.",
                "`!examples lb` - Player leaderboard examples.",
                "`!examples clb` - Champion leaderboard examples.",
                "`!examples map` - Map shortcut examples.",
                "`!examples filters` - Every filter style with examples.",
                "`!stats me` - Your overall stats.",
                "`!lb kp moji` - Moji kill participation leaderboard.",
                "`!map jaguar falls wr ying` - Ying WR on Jaguar Falls.",
                "`!clb wr support team1 map stone keep night` - Support champion WR on Team 1 for Stone Keep Night.",
            ],
            "stats": [
                "`!stats me` - Your overall stats.",
                "`!stats me support` - Your support stats.",
                "`!stats me point tank` - Your point tank stats.",
                "`!stats me off tank` - Your off tank stats.",
                "`!stats @user moji` - A user's Moji stats.",
                "`!stats pjamo damba wins` - Mal'Damba stats in wins only.",
                "`!stats me ying map jaguar falls` - Ying stats on Jaguar Falls.",
                "`!stats me fernando team2` - Fernando stats on Team 2.",
                "`!stats me moji 4-3` - Moji stats in games ending 4-3.",
                "`!stats me barik with lulub against nozy` - Barik stats with lulub against nozy.",
            ],
            "top": [
                "`!top` - Your champion table.",
                "`!top me -wr -kp -dhpm support` - Support table with WR, KP, and DPM+HPM.",
                "`!top me point tank` - Your point tank champion table.",
                "`!top me off tank` - Your off tank champion table.",
                "`!top me bk` - Your Bomb King table.",
                "`!top @user -dmg -heal_pm ying` - A user's Ying damage and healing/min.",
                "`!top me barik map jaguar falls` - Barik on Jaguar Falls.",
                "`!top me nyx team1` - Nyx on Team 1.",
                "`!top me ash losses` - Ash in losses only.",
                "`!top me fernando 4-3 with pjamo` - Fernando in 4-3 games with pjamo.",
            ],
            "leaderboard": [
                "`!lb` - Player winrate leaderboard.",
                "`!lb wr ying` - Ying winrate leaderboard.",
                "`!lb kp moji` - Moji KP leaderboard.",
                "`!lb dhpm ying` - Ying damage+healing/min leaderboard.",
                "`!lb wr barik team2` - Barik WR on Team 2.",
                "`!lb wr inara 4-3` - Inara WR in 4-3 games.",
                "`!lb wr ash against nozy` - Ash WR against nozy.",
                "`!lb dmg bk wins` - Bomb King damage/min in wins.",
                "`!lb wr point tank map jaguar falls` - Point tank WR on Jaguar Falls.",
                "`!lb wr off tank losses with lulub` - Off tank WR in losses with lulub.",
            ],
            "clb": [
                "`!clb` - Champion winrate leaderboard.",
                "`!clb wr support` - Support champion WR.",
                "`!clb wr point tank` - Point tank champion WR.",
                "`!clb wr off tank` - Off tank champion WR.",
                "`!clb dhpm support` - Support champion damage+healing/min.",
                "`!clb kp close` - Champion KP in close games.",
                "`!clb wr flank losses` - Flank champion WR in losses.",
                "`!clb dmg damage team2 stomp` - Damage champion damage/min on Team 2 in stomps.",
                "`!clb wr support against nozy` - Support champion WR against nozy.",
                "`!clb wr point tank map stone keep night` - Point tank WR on Stone Keep Night.",
            ],
            "map": [
                "`!map jaguar falls` - Winrate leaderboard on Jaguar Falls.",
                "`!map brightmarsh wr ying` - Ying WR on Brightmarsh.",
                "`!map stone keep night kp moji` - Moji KP on Stone Keep Night.",
                "`!map ascension peak dmg bk 10` - Top 10 Bomb King damage/min on Ascension Peak.",
                "`!map serpent beach wr barik team2` - Barik WR on Team 2 on Serpent Beach.",
                "`!map jaguar falls wr inara 4-3` - Inara WR in 4-3 games on Jaguar Falls.",
                "`!map brightmarsh dhpm support` - Support damage+healing/min on Brightmarsh.",
                "`!map frog isle wr point tank` - Point tank WR on Frog Isle.",
                "`!map ice mines wr off tank losses` - Off tank WR in losses on Ice Mines.",
                "`!map splitstone quarry wr ash against nozy` - Ash WR against nozy on Splitstone Quarry.",
            ],
            "mapwr": [
                "`!mapwr` - Your winrate on every map.",
                "`!mapwr me` - Same thing, explicit self lookup.",
                "`!mapwr Eagle` - Eagle's map winrates.",
                "`!mapwr Eagle ying` - Eagle's Ying map winrates.",
                "`!mapwr Eagle support` - Eagle's support map winrates.",
                "`!mapwr Eagle point tank` - Eagle's point tank map winrates.",
                "`!mapwr Eagle off tank` - Eagle's off tank map winrates.",
                "`!mapwr me barik team2` - Your Barik map WR on Team 2.",
                "`!mapwr Eagle moji losses` - Eagle's Moji map records in losses only.",
                "`!mapwr Eagle inara 4-3` - Eagle's Inara map WR in 4-3 games.",
                "`!mapwr Eagle -wr` - Eagle's map WR sorted by winrate.",
            ],
            "champmapwr": [
                "`!champmapwr atlas` - Atlas winrate on every map.",
                "`!champmapwr khan` - Khan winrate on every map.",
                "`!champmapwr bk` - Bomb King winrate on every map.",
                "`!champmapwr damba` - Mal'Damba winrate on every map.",
                "`!champmapwr atlas team2` - Atlas map WR on Team 2.",
                "`!champmapwr khan 4-3` - Khan map WR in 4-3 games.",
                "`!champmapwr atlas close` - Atlas map WR in close games.",
                "`!champmapwr khan against nozy` - Khan map WR against nozy.",
                "`!champmapwr atlas -m 5` - Atlas map WR, maps with 5+ games only.",
                "`!cmapwr ying losses` - Ying map records in losses only.",
                "`!champmapwr khan -wr` - Khan map WR sorted by winrate.",
            ],
            "champcompare": [
                "`!champcompare atlas khan` - Compare Atlas and Khan overall plus map records.",
                "`!champcompare pip damba` - Compare Pip and Mal'Damba support stats.",
                "`!champcompare bk willo` - Compare Bomb King and Willo.",
                "`!champcompare atlas khan team2` - Compare both champs on Team 2 only.",
                "`!champcompare atlas khan 4-3` - Compare both champs in 4-3 games.",
                "`!champcompare ying lilith against nozy` - Compare both champs against nozy.",
                "`!champcompare barik inara map jaguar falls` - Compare map-filtered point tanks.",
                "`!cc andy evie close` - Short alias, close games only.",
            ],
            "filters": [
                "`team1` / `team2` - Filter by draft side, e.g. `!lb wr barik team2`.",
                "`4-3` - Exact scoreline, e.g. `!lb wr inara 4-3`.",
                "`close` - Any one-point game, e.g. `!lb kp moji close`.",
                "`stomp` - Big margin games, e.g. `!clb dmg damage stomp`.",
                "`sweep` - 4-0 games, e.g. `!lb wr bk sweep`.",
                "`wins` - Wins only, e.g. `!stats me damba wins`.",
                "`losses` - Losses only, e.g. `!lb kp moji losses`.",
                "`map <name>` - Map filter, e.g. `!lb wr ying map jaguar falls`.",
                "`with <player>` - Same team, e.g. `!lb wr barik with pjamo`.",
                "`against <player>` - Enemy team, e.g. `!lb wr ash against nozy`.",
            ],
            "aliases": [
                "`bk` = Bomb King, e.g. `!lb dmg bk`.",
                "`damba` = Mal'Damba, e.g. `!stats me damba`.",
                "`andy` = Androxus, e.g. `!lb wr andy`.",
                "`ruk` = Ruckus, e.g. `!top me ruk`.",
                "`dmg` = Damage role, e.g. `!clb wr dmg`.",
                "`sup` / `supp` = Support role, e.g. `!lb dhpm sup`.",
                "`tank` / `frontline` = all tanks, e.g. `!lb wr tank`.",
                "`point tank` / `pt` = Barik, Fernando, Inara, Nyx, Terminus.",
                "`off tank` / `ot` = every other frontline.",
                "`dhpm` = damage+healing per minute, e.g. `!lb dhpm ying`.",
            ],
        }
        aliases = {
            "lb": "leaderboard",
            "champ": "clb",
            "champlb": "clb",
            "champleaderboard": "clb",
            "championleaderboard": "clb",
            "mapwinrate": "mapwr",
            "mapwinrates": "mapwr",
            "mapstats": "mapwr",
            "maps": "mapwr",
            "champmaps": "champmapwr",
            "champmap": "champmapwr",
            "championmapwr": "champmapwr",
            "cmapwr": "champmapwr",
            "cc": "champcompare",
            "ccompare": "champcompare",
            "champcmp": "champcompare",
            "filter": "filters",
            "alias": "aliases",
        }
        topic_key = aliases.get(topic_key, topic_key)
        if topic_key not in examples_by_topic:
            topic_key = "overview"

        embed = discord.Embed(
            title=topic_titles.get(topic_key, "Command Examples"),
            description="Use `!examples stats`, `!examples lb`, `!examples mapwr`, `!examples champcompare`, or `!examples filters` for focused examples.",
            color=discord.Color.green(),
        )
        embed.add_field(name="Examples", value="\n".join(examples_by_topic[topic_key]), inline=False)
        return embed

    @commands.command(
        name="examples",
        aliases=["example"],
        help=(
            "Show useful command examples. Usage: `!examples [stats|top|lb|clb|map|mapwr|champmapwr|filters|aliases]`.\n"
            "Examples: `!examples`, `!examples lb`, `!examples filters`, `!examples mapwr`, `!examples champmapwr`."
        ),
    )
    async def examples_cmd(self, ctx, *, topic: str = None):
        await ctx.send(embed=self._examples_embed(topic))

    @commands.command(
        name="filters",
        aliases=["filter"],
        help="Show available match filters. Examples: `!filters`, `!examples filters`, `!help filters`.",
    )
    async def filters_cmd(self, ctx):
        await ctx.send(embed=self._examples_embed("filters"))

    @commands.command(
        name="stats",
        help=(
            "Get stats for a player, with optional champion, role, and match filters.\n"
            "Usage: `!stats [user|ign] [champion|role] [filters]`\n"
            "The user argument accepts a mention, Discord ID, `me`, a username, "
            "a main IGN, an alt IGN, or even an unlinked IGN (match history only).\n"
            "Roles: `damage`, `flank`, `support`, `tank`, `point tank`, `off tank`.\n"
            "Filters: `map <name>`, `wins`, `losses`, `team1/team2`, `4-3`, `close`, "
            "`stomp`, `sweep`, `with <player>`, `against <player>`.\n"
            "Examples:\n"
            "- `!stats me`\n"
            "- `!stats me support`\n"
            "- `!stats @user moji`\n"
            "- `!stats pjamo damba wins`\n"
            "- `!stats me tank map jaguar falls`\n"
            "- `!stats me support team1`\n"
            "- `!stats me moji losses 4-3`\n"
            "- `!stats me tank with pjamo against nozy`"
        ),
    )
    async def stats_cmd(self, ctx, user: PlayerConverter = None, *, filter_str: str = None):
        start_time = time.monotonic()
        target_user = user or ctx.author
        match_filters = {}

        player_id = resolve_player_id(target_user)
        if not player_id:
            await ctx.send(f"No stats found for {target_user.display_name}. They may need to link their IGN using `!link <ign>`.")
            return

        icon_file = None
        embed = discord.Embed(color=discord.Color.blue())

        if filter_str:
            filter_args, match_filters, filter_error = await _extract_match_filters(ctx, filter_str.split())
            if filter_error:
                await ctx.send(filter_error)
                return
            filter_str = " ".join(filter_args).strip() or None
        
        # --- Filtered Stats Logic (Champion or Role) ---
        if filter_str:
            filter_lower = filter_str.lower()
            
            # --- ROLE-BASED STATS ---
            role_name = resolve_role_name(filter_lower)
            if role_name:
                champs_in_role = get_champions_for_role(role_name)
                
                if not champs_in_role:
                    await ctx.send("Internal error: Could not find champions for that role.")
                    return

                role_stats = get_player_stats(player_id, champions=champs_in_role, filters=match_filters)

                if not role_stats or role_stats["games"] == 0:
                    await ctx.send(f"No stats found for {target_user.display_name} playing the '{role_name}' role.")
                    return

                author_icon = _avatar_url(target_user)
                if author_icon:
                    embed.set_author(name=f"{target_user.display_name}'s Stats", icon_url=author_icon)
                else:
                    embed.set_author(name=f"{target_user.display_name}'s Stats")
                
                data = {
                    f"--- Role: {role_name} ({role_stats['games']} games) ---": "",
                    "Winrate": f"{role_stats['winrate']:.2f}% ({role_stats['wins']}-{role_stats['losses']})",
                    "KDA": f"{role_stats['kda_ratio']:.2f} ({role_stats['raw_k']}/{role_stats['raw_d']}/{role_stats['raw_a']})",
                    "Kill Participation": f"{role_stats['kill_share']:.2f}%",
                    "Damage Share": f"{role_stats['damage_share']:.2f}%",
                    "Damage Healed": f"{role_stats['damage_healed_pct']:.2f}%",
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
                stat_lines = _format_stat_block(data)
                embed.description = "```\n" + "\n".join(stat_lines) + "\n```"

            # --- CHAMPION-BASED STATS ---
            else:
                full_champion_name = get_champion_name(player_id, filter_str)
                if not full_champion_name:
                    await ctx.send(f"No stats found for {target_user.display_name} on a champion or role matching '{filter_str}'.")
                    return
                
                champ_stats = get_player_stats(player_id, champions=[full_champion_name], filters=match_filters)
                if not champ_stats or champ_stats["games"] == 0:
                    await ctx.send(f"No stats found for {target_user.display_name} on {full_champion_name}.")
                    return

                global_stats = get_player_stats(player_id, filters=match_filters)

                author_icon = _avatar_url(target_user)
                if author_icon:
                    embed.set_author(name=f"{target_user.display_name}'s Stats", icon_url=author_icon)
                else:
                    embed.set_author(name=f"{target_user.display_name}'s Stats")
                icon_path = get_champion_icon_path(full_champion_name)
                if os.path.exists(icon_path):
                    icon_file = discord.File(icon_path, filename="icon.png")
                    embed.set_thumbnail(url="attachment://icon.png")
                
                champ_data = {
                    f"--- Champion: {full_champion_name} ---": "",
                    "Winrate": f"{champ_stats['winrate']:.2f}% ({champ_stats['wins']}-{champ_stats['losses']})",
                    "KDA": f"{champ_stats['kda_ratio']:.2f} ({champ_stats['raw_k']}/{champ_stats['raw_d']}/{champ_stats['raw_a']})",
                    "Kill Participation": f"{champ_stats['kill_share']:.2f}%",
                    "Damage Share": f"{champ_stats['damage_share']:.2f}%",
                    "Damage Healed": f"{champ_stats['damage_healed_pct']:.2f}%",
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

                champ_lines = _format_stat_block(champ_data)
                global_lines = _format_stat_block(global_data)
                embed.description = "```\n" + "\n".join(champ_lines) + "\n\n" + "\n".join(global_lines) + "\n```"

        # --- GENERAL STATS (No Filter) ---
        else:
            stats = get_player_stats(player_id, filters=match_filters)
            if not stats or stats["games"] == 0:
                await ctx.send(f"No stats found for {target_user.display_name}.")
                return
                
            embed.title = f"Stats for {target_user.display_name}"
            thumbnail_url = _avatar_url(target_user)
            if thumbnail_url:
                embed.set_thumbnail(url=thumbnail_url)

            data = {
                "Winrate": f"{stats['winrate']:.2f}% ({stats['wins']}-{stats['losses']})",
                "KDA": f"{stats['kda_ratio']:.2f} ({stats['raw_k']}/{stats['raw_d']}/{stats['raw_a']})",
                "Kill Participation": f"{stats['kill_share']:.2f}%",
                "Damage Share": f"{stats['damage_share']:.2f}%",
                "Damage Healed": f"{stats['damage_healed_pct']:.2f}%",
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
            stat_lines = _format_stat_block(data)
            embed.description = "```\n" + "\n".join(stat_lines) + "\n```"
        
        # Set the footer
        fetch_time = (time.monotonic() - start_time) * 1000
        footer_text = f"Fetched in {fetch_time:.0f}ms"
        active_filters = _filter_summary(match_filters)
        if active_filters:
            footer_text = f"{footer_text}    •   {'; '.join(active_filters)}"
        if _is_unlinked(target_user):
            footer_text = f"Unlinked IGN: {target_user.display_name}    •   {footer_text}"
        elif not filter_str:
            footer_text = f"Player ID: {target_user.id}    •   {footer_text}"
        embed.set_footer(text=footer_text, icon_url=ctx.guild.icon.url if ctx.guild and ctx.guild.icon else None)

        await ctx.send(embed=embed, file=icon_file)

    TOP_HELP = """
Shows champion statistics breakdown for a player.

**Usage:** `!top [@user] [-stat1] [-stat2] ... [role/champion] [-m <games>] [filters]`

**Arguments:**
- `[@user]`: Target player (defaults to yourself)
- `[-stat]`: Stats to display (e.g., `-kpm -dmg_share -kda`)
- `[role/champion]`: Filter by role (`damage`, `flank`, `support`, `tank`, `point tank`, `off tank`) or champion name
- `[-m <games>]`: Minimum games filter (default: 1)
- `[filters]`: `map <name>`, `wins`, `losses`, `team1/team2`, `4-3`, `close`, `stomp`, `sweep`, `with <player>`, `against <player>`

**Available Stats:**
- `-wr` or `-winrate`: Winrate percentage
- `-kda`: KDA ratio
- `-kpm`: Kills per minute
- `-dpm` or `-deaths_pm`: Deaths per minute  
- `-dmg` or `-damage_pm`: Damage per minute
- `-taken_pm`: Damage taken per minute
- `-heal_pm`: Healing per minute
- `-dhpm` or `-dmg_heal_pm`: Damage + healing per minute
- `-self_heal_pm`: Self healing per minute
- `-creds_pm`: Credits per minute
- `-kp`: Kill participation %
- `-dmg_share`: Damage share %
- `-avg_kills`: Average kills per match
- `-avg_deaths`: Average deaths per match
- `-avg_dmg`: Average damage per match
- `-avg_taken`: Average damage taken per match
- `-delta`: Average damage delta
- `-avg_heal`: Average healing per match
- `-avg_self_heal`: Average self healing per match
- `-avg_shield`: Average shielding per match
- `-avg_creds`: Average credits per match
- `-obj_time`: Average objective time

**Examples:**
- `!top` - Shows default stats (games, winrate, KDA, time)
- `!top me` - Shows your champion table.
- `!top -kpm -dmg_share` - Shows kills/min and damage share
- `!top tank -m 5` - Shows tanks with 5+ games
- `!top me bk` - Shows your Bomb King stats.
- `!top me point tank` - Shows your point tanks.
- `!top me off tank` - Shows your off tanks.
- `!top @user -wr -kp -dmg support` - Shows support stats with custom columns
- `!top me -wr -dhpm ying` - Shows Ying winrate and damage+healing/min
- `!top me -wr -kp support with pjamo` - Shows support stats while teamed with pjamo
- `!top me damba wins map brightmarsh` - Shows Mal'Damba on Brightmarsh wins
- `!top me ash team2 close against nozy` - Shows Ash on Team 2 in close games against nozy
"""

    @commands.command(name="top", help=TOP_HELP)
    async def top_cmd(self, ctx, *args):
        args, match_filters, filter_error = await _extract_match_filters(ctx, args)
        if filter_error:
            await ctx.send(filter_error)
            return

        # Parse arguments
        target_user = ctx.author
        stat_flags = []
        role_filter = None
        champion_filter = None
        min_games = 1
        unprocessed_args = []
        
        # Define stat aliases
        stat_aliases = {
            '-wr': '-winrate',
            '-dmg': '-damage_pm', 
            '-dpm': '-deaths_pm',
            '-avg_dmg': '-avg_damage_dealt',
            '-avg_taken': '-avg_damage_taken',
            '-avg_heal': '-avg_healing',
            '-avg_self_heal': '-avg_self_healing',
            '-avg_shield': '-avg_shielding',
            '-avg_creds': '-avg_credits',
            '-delta': '-damage_delta',
            '-kpm': '-kills_pm',
            '-heal_pm': '-healing_pm',
            '-self_heal_pm': '-self_healing_pm',
            '-creds_pm': '-credits_pm',
            '-dhpm': '-damage_healing_pm',
            '-dmg_heal_pm': '-damage_healing_pm',
            '-dmg_healing_pm': '-damage_healing_pm',
            '-damage_heal_pm': '-damage_healing_pm',
        }
        
        # Valid stat keys
        valid_stats = {
            '-winrate', '-kda', '-kda_ratio', '-kills_pm', '-deaths_pm', 
            '-damage_pm', '-damage_dealt_pm', '-damage_taken_pm', '-healing_pm',
            '-damage_healing_pm',
            '-self_healing_pm', '-credits_pm', '-kp', '-dmg_share',
            '-avg_kills', '-avg_deaths', '-avg_damage_dealt', '-avg_damage_taken',
            '-damage_delta', '-avg_healing', '-avg_self_healing', '-avg_shielding',
            '-avg_credits', '-obj_time'
        }
        
        # Process arguments
        i = 0
        while i < len(args):
            arg = args[i]
            
            # Check for -m flag
            if arg.lower() == '-m':
                if i + 1 < len(args) and args[i+1].isdigit():
                    min_games = max(1, int(args[i+1]))
                    i += 2
                    continue
                i += 1
                continue
            
            # Check for stat flags
            if arg.lower().startswith('-'):
                normalized = stat_aliases.get(arg.lower(), arg.lower())
                if normalized in valid_stats:
                    # Remove the dash and store the actual key
                    stat_key = normalized[1:].replace('_pm', '_pm').replace('damage_pm', 'damage_dealt_pm')
                    if stat_key not in stat_flags:
                        stat_flags.append(stat_key)
                else:
                    await ctx.send(f"Unknown stat flag: `{arg}`. Use `!help top` to see available stats.")
                    return
            else:
                unprocessed_args.append(arg)
            i += 1
        
        # Process non-flag arguments
        if unprocessed_args:
            # Check if first arg is a user
            try:
                target_user = await PlayerConverter().convert(ctx, unprocessed_args[0])
                unprocessed_args = unprocessed_args[1:]
            except:
                pass  # Not a user, continue processing
            
            # Check for role/champion filter
            if unprocessed_args:
                filter_str = " ".join(unprocessed_args).lower()
                
                matched_role = resolve_role_name(filter_str)
                
                if matched_role:
                    role_filter = matched_role
                else:
                    champion_filter = resolve_champion_name(filter_str) or filter_str
        
        # Get player ID
        player_id = resolve_player_id(target_user)
        if not player_id:
            await ctx.send(f"No stats found for {target_user.display_name}. They may need to `!link` their IGN.")
            return
        
        # Default stats if none specified
        if not stat_flags:
            stat_flags = ['winrate', 'kda_ratio', 'games', 'time_played']
        
        # Get champion stats
        champ_data = get_player_champion_stats(
            player_id, role_filter=role_filter, min_games=min_games, filters=match_filters
        )
        
        # Filter by champion if specified
        if champion_filter and champ_data:
            # Find champions that match the filter
            filtered_data = []
            for champ in champ_data:
                if champion_filter in champ['champ'].lower():
                    filtered_data.append(champ)
            
            if not filtered_data:
                await ctx.send(f"No stats found for champion matching '{champion_filter}'.")
                return
            champ_data = filtered_data
        
        if not champ_data:
            filter_msg = ""
            if role_filter:
                filter_msg = f" for role '{role_filter}'"
            elif champion_filter:
                filter_msg = f" for champion '{champion_filter}'"
            if min_games > 1:
                filter_msg += f" with at least {min_games} games"
            await ctx.send(f"No champion stats found for {target_user.display_name}{filter_msg}.")
            return
        
        # Sort by first stat flag (or games if using defaults)
        sort_key = stat_flags[0] if stat_flags[0] not in ['time_played'] else 'games'
        champ_data.sort(key=lambda x: x.get(sort_key, 0), reverse=True)
        
        # Build the display
        embed = discord.Embed(
            title=f"Champion Stats for {target_user.display_name}",
            color=discord.Color.blue()
        )
        
        # Add filters to description if any
        filters = []
        if role_filter:
            filters.append(f"Role: {role_filter}")
        if champion_filter:
            filters.append(f"Champion: {champion_filter}")
        if min_games > 1:
            filters.append(f"Min games: {min_games}")
        filters.extend(_filter_summary(match_filters))
        if filters:
            embed.description = f"*Filters: {', '.join(filters)}*"
        
        # Define stat formatters
        stat_formatters = {
            'winrate': lambda v: f"{v:.1f}%",
            'kda_ratio': lambda v: f"{v:.2f}",
            'kda': lambda v: v,
            'kills_pm': lambda v: f"{v:.2f}",
            'deaths_pm': lambda v: f"{v:.2f}",
            'damage_dealt_pm': lambda v: f"{int(v):,}",
            'damage_taken_pm': lambda v: f"{int(v):,}",
            'healing_pm': lambda v: f"{int(v):,}",
            'damage_healing_pm': lambda v: f"{int(v):,}",
            'self_healing_pm': lambda v: f"{int(v):,}",
            'credits_pm': lambda v: f"{int(v):,}",
            'kp': lambda v: f"{v:.1f}%",
            'dmg_share': lambda v: f"{v:.1f}%",
            'avg_kills': lambda v: f"{v:.1f}",
            'avg_deaths': lambda v: f"{v:.1f}",
            'avg_damage_dealt': lambda v: f"{int(v):,}",
            'avg_damage_taken': lambda v: f"{int(v):,}",
            'damage_delta': lambda v: f"{int(v):,}",
            'avg_healing': lambda v: f"{int(v):,}",
            'avg_self_healing': lambda v: f"{int(v):,}",
            'avg_shielding': lambda v: f"{int(v):,}",
            'avg_credits': lambda v: f"{int(v):,}",
            'obj_time': lambda v: f"{int(v)}s",
            'games': lambda v: str(v),
            'time_played': lambda v: v
        }
        
        # Define display names for stats
        stat_display_names = {
            'winrate': 'WR%',
            'kda_ratio': 'KDA',
            'kda': 'K/D/A',
            'kills_pm': 'K/min',
            'deaths_pm': 'D/min',
            'damage_dealt_pm': 'DMG/min',
            'damage_taken_pm': 'Taken/min',
            'healing_pm': 'Heal/min',
            'damage_healing_pm': 'D+H/min',
            'self_healing_pm': 'SHeal/min',
            'credits_pm': 'Creds/min',
            'kp': 'KP%',
            'dmg_share': 'DMG%',
            'avg_kills': 'AvgK',
            'avg_deaths': 'AvgD',
            'avg_damage_dealt': 'AvgDMG',
            'avg_damage_taken': 'AvgTaken',
            'damage_delta': 'Delta',
            'avg_healing': 'AvgHeal',
            'avg_self_healing': 'AvgSHeal',
            'avg_shielding': 'AvgShield',
            'avg_credits': 'AvgCreds',
            'obj_time': 'ObjTime',
            'games': 'Games',
            'time_played': 'Time'
        }
        
        # Build the table
        lines = []
        
        # Determine column widths
        col_widths = {'champ': 16}
        for stat in stat_flags:
            display_name = stat_display_names.get(stat, stat)
            col_widths[stat] = max(len(display_name) + 2, 10)
        
        # Build header
        header_parts = [f"{'Champion':<16}"]
        for stat in stat_flags:
            display_name = stat_display_names.get(stat, stat)
            header_parts.append(f"{display_name:<{col_widths[stat]}}")
        header = "".join(header_parts)
        separator = "-" * len(header)
        
        # Add data rows grouped by role
        if not role_filter and not champion_filter:
            # Group by role
            for role in ["Damage", "Flank", "Point Tank", "Off Tank", "Support"]:
                role_champ_names = set(get_champions_for_role(role))
                role_champs = [c for c in champ_data if c["champ"] in role_champ_names]
                if role_champs:
                    lines.append(header)
                    lines.append(separator)
                    lines.append(f"# {role}")
                    
                    for i, champ in enumerate(role_champs[:10], 1):  # Limit to top 10 per role
                        row_parts = [f"{i}. {champ['champ'][:14]:<14}"]
                        for stat in stat_flags:
                            value = champ.get(stat, 0)
                            formatter = stat_formatters.get(stat, str)
                            formatted = formatter(value)
                            row_parts.append(f"{formatted:<{col_widths[stat]}}")
                        lines.append("".join(row_parts))
                    lines.append("")
        else:
            # No role grouping
            lines.append(header)
            lines.append(separator)
            
            for i, champ in enumerate(champ_data[:30], 1):  # Limit to top 30
                row_parts = [f"{i}. {champ['champ'][:14]:<14}"]
                for stat in stat_flags:
                    value = champ.get(stat, 0)
                    formatter = stat_formatters.get(stat, str)
                    formatted = formatter(value)
                    row_parts.append(f"{formatted:<{col_widths[stat]}}")
                lines.append("".join(row_parts))
        
        # Add to embed
        result_text = "```\n" + "\n".join(lines) + "\n```"
        
        # Check if the result is too long
        if len(result_text) > 1900:
            # Truncate and add note
            truncated_lines = []
            current_length = 0
            for line in lines:
                if current_length + len(line) + 10 > 1900:  # Leave room for closing
                    truncated_lines.append("... (truncated)")
                    break
                truncated_lines.append(line)
                current_length += len(line) + 1
            result_text = "```\n" + "\n".join(truncated_lines) + "\n```"
        
        # Split into multiple fields if needed
        if len(result_text) <= 1024:
            embed.add_field(name="Statistics", value=result_text, inline=False)
        else:
            # Split the content
            chunks = []
            current_chunk = []
            current_length = 0
            
            for line in lines:
                if current_length + len(line) + 10 > 1000:
                    chunks.append("```\n" + "\n".join(current_chunk) + "\n```")
                    current_chunk = [line]
                    current_length = len(line)
                else:
                    current_chunk.append(line)
                    current_length += len(line) + 1
            
            if current_chunk:
                chunks.append("```\n" + "\n".join(current_chunk) + "\n```")
            
            for i, chunk in enumerate(chunks[:3]):  # Max 3 fields
                field_name = "Statistics" if i == 0 else "​"  # Zero-width space for continuation
                embed.add_field(name=field_name, value=chunk, inline=False)
        
        # Add footer with info
        footer_parts = []
        if stat_flags != ['winrate', 'kda_ratio', 'games', 'time_played']:
            footer_parts.append(f"Stats shown: {', '.join(stat_display_names.get(s, s) for s in stat_flags)}")
        footer_parts.append(f"Use !help top for more options")
        embed.set_footer(text=" • ".join(footer_parts))
        
        await ctx.send(embed=embed)

    @commands.command(
        name="history",
        help=(
            "Show recent matches for a player (max 20).\n"
            "Usage: `!history [user|ign] [count]`\n"
            "Examples:\n"
            "- `!history`\n"
            "- `!history 10`\n"
            "- `!history @user 5`\n"
            "- `!history pjamo 8`\n"
            "- `!history Fúriä 20`"
        ),
    )
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

        player_id = resolve_player_id(target_user)
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

    MAP_WINRATES_HELP = """
Show a player's winrate on every map, with optional role/champion filters.

**Usage:** `!mapwr [user|ign] [champion|role] [-m <games>] [-wr] [filters]`

**Roles:** `damage`, `flank`, `support`, `tank`, `point tank`, `off tank`
**Filters:** `wins`, `losses`, `team1/team2`, `4-3`, `close`, `stomp`, `sweep`, `with <player>`, `against <player>`

**Examples:**
- `!mapwr` - Your map winrates.
- `!mapwr Eagle` - Eagle's map winrates.
- `!mapwr Eagle ying` - Eagle's Ying map winrates.
- `!mapwr Eagle support` - Eagle's support map winrates.
- `!mapwr Eagle point tank` - Eagle's point tank map winrates.
- `!mapwr me barik team2` - Your Barik map winrates on Team 2.
- `!mapwr Eagle inara 4-3` - Eagle's Inara map winrates in 4-3 games.
- `!mapwr Eagle -wr` - Sort by winrate instead of alphabetically.
"""

    @commands.command(name="mapwr", aliases=["map_wr", "maps", "mapstats"], help=MAP_WINRATES_HELP)
    async def map_winrates_cmd(self, ctx, *args):
        args, match_filters, filter_error = await _extract_match_filters(ctx, args)
        if filter_error:
            await ctx.send(filter_error)
            return

        target_user = ctx.author
        filter_args = list(args)
        min_games = 1
        sort_by_winrate = False

        i = 0
        cleaned_args = []
        while i < len(filter_args):
            arg = str(filter_args[i])
            if arg.lower() in {"-wr", "--wr", "-winrate", "--winrate"}:
                sort_by_winrate = True
                i += 1
                continue
            if arg.lower() == "-m":
                if i + 1 < len(filter_args) and str(filter_args[i + 1]).isdigit():
                    min_games = max(1, int(filter_args[i + 1]))
                    i += 2
                    continue
                await ctx.send("`-m` needs a number after it, like `!mapwr Eagle -m 3`.")
                return
            cleaned_args.append(arg)
            i += 1
        filter_args = cleaned_args

        if filter_args:
            for end in range(len(filter_args), 0, -1):
                candidate = " ".join(filter_args[:end])
                try:
                    target_user = await PlayerConverter().convert(ctx, candidate)
                    filter_args = filter_args[end:]
                    break
                except commands.BadArgument:
                    continue

        player_id = resolve_player_id(target_user)
        if not player_id:
            await ctx.send(f"No stats found for {target_user.display_name}. They may need to link their IGN using `!link <ign>`.")
            return

        filter_name = None
        champions = None
        if filter_args:
            filter_str = " ".join(filter_args).lower()
            role_name = resolve_role_name(filter_str)
            if role_name:
                champions = get_champions_for_role(role_name)
                filter_name = role_name
            else:
                champion_name = resolve_champion_name(filter_str) or get_champion_name(player_id, filter_str)
                if not champion_name:
                    await ctx.send(f"No champion or role found matching `{filter_str}`.")
                    return
                champions = [champion_name]
                filter_name = champion_name

        rows = get_player_map_winrates(
            player_id,
            champions=champions,
            filters=match_filters,
            min_games=min_games,
            include_all_maps=(min_games <= 1),
            sort_by_winrate=sort_by_winrate,
        )
        if not rows:
            detail = f" for {filter_name}" if filter_name else ""
            await ctx.send(f"No map winrate data found for {target_user.display_name}{detail}.")
            return

        title = f"Map Winrates for {target_user.display_name}"
        if filter_name:
            title += f" on {filter_name}"
        title += _title_filter_suffix(match_filters)

        embed = discord.Embed(title=title, color=discord.Color.blue())
        icon_file = None
        if champions and len(champions) == 1:
            champ_icon = get_champion_icon_path(champions[0])
            if os.path.exists(champ_icon):
                icon_file = discord.File(champ_icon, filename="champ_icon.png")
                embed.set_thumbnail(url="attachment://champ_icon.png")
        rows = rows[:35]
        name_width = min(24, max(len(row["map"]) for row in rows))
        header = f"{'Map':<{name_width}}  {'Record':<7} {'WR':>7}"
        lines = [header, "-" * len(header)]
        for row in rows:
            map_name = row["map"]
            if len(map_name) > name_width:
                map_name = map_name[:name_width - 1] + "…"
            record = f"{row['wins']}-{row['losses']}"
            lines.append(f"{map_name:<{name_width}}  {record:<7} {row['winrate']:>6.2f}%")

        embed.description = "```\n" + "\n".join(lines) + "\n```"
        footer_parts = []
        if min_games > 1:
            footer_parts.append(f"Maps must have at least {min_games} games.")
        if sort_by_winrate:
            footer_parts.append("Sorted by winrate.")
        else:
            footer_parts.append("Sorted alphabetically.")
        active_filters = _filter_summary(match_filters)
        if active_filters:
            footer_parts.append("Filters: " + "; ".join(active_filters))
        if footer_parts:
            embed.set_footer(text=" • ".join(footer_parts))
        await ctx.send(embed=embed, file=icon_file)

    CHAMPION_COMPARE_HELP = """
Compare two champions overall and by map.

**Usage:** `!champcompare <champion1> <champion2> [filters]`

**Filters:** `wins`, `losses`, `team1/team2`, `4-3`, `close`, `stomp`, `sweep`, `map <name>`, `with <player>`, `against <player>`

**Examples:**
- `!champcompare atlas khan`
- `!champcompare pip damba`
- `!champcompare bk willo team2`
- `!champcompare atlas khan 4-3`
- `!champcompare ying lilith against nozy`
"""

    @commands.command(
        name="champcompare",
        aliases=["ccompare", "champcmp", "cc", "comparechamps"],
        help=CHAMPION_COMPARE_HELP,
    )
    async def champion_compare_cmd(self, ctx, *args):
        args, match_filters, filter_error = await _extract_match_filters(ctx, args)
        if filter_error:
            await ctx.send(filter_error)
            return

        if len(args) < 2:
            await ctx.send("Usage: `!champcompare <champion1> <champion2> [filters]`, like `!champcompare atlas khan`.")
            return

        first_champ, second_champ = _split_champion_pair(list(args))
        if not first_champ or not second_champ:
            await ctx.send("I couldn't find two champions there. Try aliases like `bk`, `damba`, `andy`, or full names like `bomb king`.")
            return
        if first_champ == second_champ:
            await ctx.send("Pick two different champions to compare.")
            return

        first_stats = get_champion_overall_stats(first_champ, filters=match_filters)
        second_stats = get_champion_overall_stats(second_champ, filters=match_filters)
        if not first_stats and not second_stats:
            await ctx.send(f"No comparison data found for {first_champ} or {second_champ}.")
            return

        def stat_value(stats, key, formatter, fallback="--"):
            if not stats:
                return fallback
            value = stats.get(key)
            if value is None:
                return fallback
            return formatter(value)

        def record(stats):
            if not stats:
                return "--"
            return f"{stats['wins']}-{stats['losses']}"

        first_label = first_champ[:12]
        second_label = second_champ[:12]
        rows = [
            ("Games", stat_value(first_stats, "games", lambda v: f"{v:,}"), stat_value(second_stats, "games", lambda v: f"{v:,}")),
            ("Record", record(first_stats), record(second_stats)),
            ("Winrate", stat_value(first_stats, "winrate", lambda v: f"{v:.2f}%"), stat_value(second_stats, "winrate", lambda v: f"{v:.2f}%")),
            ("KDA", stat_value(first_stats, "kda", lambda v: f"{v:.2f}"), stat_value(second_stats, "kda", lambda v: f"{v:.2f}")),
            ("DPM", stat_value(first_stats, "dpm", lambda v: f"{round(v):,}"), stat_value(second_stats, "dpm", lambda v: f"{round(v):,}")),
            ("HPM", stat_value(first_stats, "hpm", lambda v: f"{round(v):,}"), stat_value(second_stats, "hpm", lambda v: f"{round(v):,}")),
            ("Dmg Healed", stat_value(first_stats, "damage_healed_pct", lambda v: f"{v:.2f}%"), stat_value(second_stats, "damage_healed_pct", lambda v: f"{v:.2f}%")),
            ("KP", stat_value(first_stats, "kp", lambda v: f"{v:.2f}%"), stat_value(second_stats, "kp", lambda v: f"{v:.2f}%")),
            ("Dmg Share", stat_value(first_stats, "dmg_share", lambda v: f"{v:.2f}%"), stat_value(second_stats, "dmg_share", lambda v: f"{v:.2f}%")),
            ("Taken/min", stat_value(first_stats, "taken_pm", lambda v: f"{round(v):,}"), stat_value(second_stats, "taken_pm", lambda v: f"{round(v):,}")),
            ("Credits/min", stat_value(first_stats, "credits_pm", lambda v: f"{round(v):,}"), stat_value(second_stats, "credits_pm", lambda v: f"{round(v):,}")),
        ]

        header = f"{'Stat':<12} {first_label:>12} {second_label:>12}"
        stat_lines = [header, "-" * len(header)]
        stat_lines.extend(f"{name:<12} {left:>12} {right:>12}" for name, left, right in rows)

        first_maps = {row["map"]: row for row in get_champion_map_winrates(first_champ, filters=match_filters)}
        second_maps = {row["map"]: row for row in get_champion_map_winrates(second_champ, filters=match_filters)}
        map_names = sorted(set(first_maps) | set(second_maps), key=str.lower)

        def map_cell(row):
            if not row or not row.get("games"):
                return "--"
            return f"{row['wins']}-{row['losses']} {row['winrate']:.0f}%"

        map_lines = []
        if map_names:
            map_header = f"{'Map':<18} {first_label:>11} {second_label:>11}"
            map_lines = [map_header, "-" * len(map_header)]
            for map_name in map_names[:20]:
                shown_map = map_name if len(map_name) <= 18 else map_name[:17] + "."
                map_lines.append(f"{shown_map:<18} {map_cell(first_maps.get(map_name)):>11} {map_cell(second_maps.get(map_name)):>11}")

        embed = discord.Embed(
            title=f"Champion Compare: {first_champ} vs {second_champ}{_title_filter_suffix(match_filters)}",
            color=discord.Color.purple(),
        )
        embed.add_field(name="Overall", value="```\n" + "\n".join(stat_lines) + "\n```", inline=False)
        if map_lines:
            embed.add_field(name="By Map", value="```\n" + "\n".join(map_lines) + "\n```", inline=False)

        footer_parts = []
        active_filters = _filter_summary(match_filters)
        if active_filters:
            footer_parts.append("Filters: " + "; ".join(active_filters))
        if len(map_names) > 20:
            footer_parts.append(f"Showing first 20 of {len(map_names)} maps.")
        if footer_parts:
            embed.set_footer(text=" â€¢ ".join(footer_parts))
        await ctx.send(embed=embed)

    CHAMPION_MAP_WINRATES_HELP = """
Show one champion's winrate on every map.

**Usage:** `!champmapwr <champion> [-m <games>] [-wr] [filters]`

**Filters:** `wins`, `losses`, `team1/team2`, `4-3`, `close`, `stomp`, `sweep`, `with <player>`, `against <player>`

**Examples:**
- `!champmapwr atlas` - Atlas map winrates.
- `!champmapwr khan` - Khan map winrates.
- `!champmapwr bk` - Bomb King map winrates.
- `!champmapwr atlas team2` - Atlas map winrates on Team 2.
- `!champmapwr khan 4-3` - Khan map winrates in 4-3 games.
- `!champmapwr atlas -m 5` - Atlas maps with at least 5 games.
- `!champmapwr atlas -wr` - Sort by winrate instead of alphabetically.
"""

    @commands.command(
        name="champmapwr",
        aliases=["cmapwr", "champmaps", "champmap", "champ_mapwr"],
        help=CHAMPION_MAP_WINRATES_HELP,
    )
    async def champion_map_winrates_cmd(self, ctx, *args):
        args, match_filters, filter_error = await _extract_match_filters(ctx, args)
        if filter_error:
            await ctx.send(filter_error)
            return

        min_games = 1
        sort_by_winrate = False
        cleaned_args = []
        i = 0
        while i < len(args):
            arg = str(args[i])
            if arg.lower() in {"-wr", "--wr", "-winrate", "--winrate"}:
                sort_by_winrate = True
                i += 1
                continue
            if arg.lower() == "-m":
                if i + 1 < len(args) and str(args[i + 1]).isdigit():
                    min_games = max(1, int(args[i + 1]))
                    i += 2
                    continue
                await ctx.send("`-m` needs a number after it, like `!champmapwr atlas -m 5`.")
                return
            cleaned_args.append(arg)
            i += 1

        if not cleaned_args:
            await ctx.send("Usage: `!champmapwr <champion> [filters]`")
            return

        champion_input = " ".join(cleaned_args)
        champion_name = resolve_champion_name(champion_input) or champion_input.title()
        if champion_name not in CHAMPION_ROLES:
            await ctx.send(f"No champion found matching `{champion_input}`.")
            return

        rows = get_champion_map_winrates(
            champion_name,
            filters=match_filters,
            min_games=min_games,
            include_all_maps=(min_games <= 1),
            sort_by_winrate=sort_by_winrate,
        )
        if not rows:
            await ctx.send(f"No map winrate data found for {champion_name}.")
            return

        title = f"Map Winrates for {champion_name}{_title_filter_suffix(match_filters)}"
        embed = discord.Embed(title=title, color=discord.Color.blue())
        icon_file = None
        champ_icon = get_champion_icon_path(champion_name)
        if os.path.exists(champ_icon):
            icon_file = discord.File(champ_icon, filename="champ_icon.png")
            embed.set_thumbnail(url="attachment://champ_icon.png")

        rows = rows[:35]
        name_width = min(24, max(len(row["map"]) for row in rows))
        header = f"{'Map':<{name_width}}  {'Record':<7} {'WR':>7}"
        lines = [header, "-" * len(header)]
        for row in rows:
            map_name = row["map"]
            if len(map_name) > name_width:
                map_name = map_name[:name_width - 1] + "…"
            record = f"{row['wins']}-{row['losses']}"
            lines.append(f"{map_name:<{name_width}}  {record:<7} {row['winrate']:>6.2f}%")

        embed.description = "```\n" + "\n".join(lines) + "\n```"
        footer_parts = []
        if min_games > 1:
            footer_parts.append(f"Maps must have at least {min_games} games.")
        if sort_by_winrate:
            footer_parts.append("Sorted by winrate.")
        else:
            footer_parts.append("Sorted alphabetically.")
        active_filters = _filter_summary(match_filters)
        if active_filters:
            footer_parts.append("Filters: " + "; ".join(active_filters))
        if footer_parts:
            embed.set_footer(text=" • ".join(footer_parts))
        await ctx.send(embed=embed, file=icon_file)

    LEADERBOARD_HELP = """
Shows player rankings, with optional filters for champions or roles.

**Usage:** `!leaderboard [stat] [champion/role] [limit] [-b] [-m <games>] [filters]`

**Arguments:**
- `[stat]`: The statistic to rank by. Defaults to `winrate`.
- `[champion/role]`: Filter by a champion name (e.g., `nando`) or a role (`tank`, `support`, `point tank`, `off tank`).
- `[limit]`: The number of players to show. Defaults to `20`.
- `[-b]`: Optional flag to show the bottom of the leaderboard.
- `[-m <games>]`: Optional flag to set a minimum number of games played to qualify. Defaults to 1 (all players).
- `[filters]`: `map <name>`, `wins`, `losses`, `team1/team2`, `4-3`, `close`, `stomp`, `sweep`, `with <player>`, `against <player>`

**Available Stats:**
- `winrate` (or `wr`): Overall Winrate
- `kda`: Kill/Death/Assist Ratio
- `kp`: Kill Participation (% of team kills + assists)
- `dmg_share`: Damage Share (% of team damage)
- `kpm`: Kills per Minute
- `deaths_pm`: Deaths per Minute
- `dmg` (or `dpm`): Damage per Minute
- `taken_pm`: Damage Taken per Minute
- `heal_pm`: Healing per Minute (Defaults to Supports)
- `dhpm`: Damage + Healing per Minute (Defaults to Supports)
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
- `!lb kp tank`: Top 20 tanks by kill participation.
- `!lb dmg_share dmg`: Top 20 damage dealers by damage share.
- `!lb wr barik team2`: Barik winrate on Team 2.
- `!lb kp moji losses`: Moji KP in losses only.
- `!lb wr support map jaguar falls`: Support winrate on Jaguar Falls.
- `!lb wr inara 4-3`: Inara winrate in games ending 4-3.
- `!lb wr ash team2 close against pjamo`: Ash winrate on Team 2 in close games against pjamo.
- `!lb wr bk wins`: Bomb King winrate in wins only.
- `!lb dhpm ying`: Ying damage + healing per minute.
- `!lb wr point tank map jaguar falls`: Point tank winrate on Jaguar Falls.
- `!map jaguar falls wr support`: Shortcut for support winrate on Jaguar Falls.
"""

    @commands.command(name="leaderboard", aliases=["lb"], help=LEADERBOARD_HELP)
    async def leaderboard_cmd(self, ctx, *args):
        # --- Stat Mapping (Complete with all stats) ---
        stat_map = {
            "winrate": ("Winrate", "winrate", lambda v, s: f"{v:.2f}% ({s['wins']}-{s['losses']})"),
            "kda": ("KDA Ratio", "kda", lambda v, s: f"{v:.2f} ({s['k']}/{s['d']}/{s['a']})"),
            "kp": ("Kill Participation", "kp", lambda v, s: f"{v:.2f}%"),
            "dmg_share": ("Damage Share", "dmg_share", lambda v, s: f"{v:.2f}%"),
            "kpm": ("Kills/Min", "kills_pm", lambda v, s: f"{v:.2f}"),
            "deaths_pm": ("Deaths/Min", "deaths_pm", lambda v, s: f"{v:.2f}"),
            "dmg_pm": ("Damage/Min", "damage_dealt_pm", lambda v, s: f"{int(v):,}"),
            "taken_pm": ("Damage Taken/Min", "damage_taken_pm", lambda v, s: f"{int(v):,}"),
            "heal_pm": ("Healing/Min", "healing_pm", lambda v, s: f"{int(v):,}"),
            "dhpm": ("Damage + Healing/Min", "damage_healing_pm", lambda v, s: f"{int(v):,}"),
            "dmg_heal_pm": ("Damage + Healing/Min", "damage_healing_pm", lambda v, s: f"{int(v):,}"),
            "dmg_healing_pm": ("Damage + Healing/Min", "damage_healing_pm", lambda v, s: f"{int(v):,}"),
            "damage_healing_pm": ("Damage + Healing/Min", "damage_healing_pm", lambda v, s: f"{int(v):,}"),
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

        args, match_filters, filter_error = await _extract_match_filters(ctx, args)
        if filter_error:
            await ctx.send(filter_error)
            return
        
        # --- 1. Argument Parsing ---
        stat_alias = "winrate"
        limit = 20
        show_bottom = False
        champion_filter = None
        role_filter = None
        min_games = None
        
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
            
            matched_role = resolve_role_name(full_filter_str)

            if matched_role:
                role_filter = matched_role
            else:
                champion_filter = resolve_champion_name(full_filter_str) or full_filter_str

        limit = max(1, min(limit, 50))
        
        if min_games is None:
            min_games = 1
        
        # --- 2. Fetch Data ---
        display_name, data_key, formatter = stat_map[stat_alias]
        if not champion_filter and not role_filter and data_key in ["healing_pm", "avg_healing", "damage_healing_pm"]:
            role_filter = "Support"
        leaderboard_data = get_leaderboard(
            data_key, limit, show_bottom,
            champion=champion_filter, role=role_filter, min_games=min_games, filters=match_filters
        )
        if not leaderboard_data:
            filter_name = champion_filter.title() if champion_filter else role_filter if role_filter else ""
            # Add a note if it's a healing stat and no filter was applied
            if not filter_name and data_key in ["healing_pm", "avg_healing", "damage_healing_pm"]:
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
        filter_text += _title_filter_suffix(match_filters)

        embed_title = f"🏆 {'Bottom' if show_bottom else 'Top'} {len(leaderboard_data)} Players by {display_name}{filter_text}"
        embed_color = 0xE74C3C if show_bottom else 0x2ECC71
        embed = discord.Embed(title=embed_title, color=embed_color)
        
        footer_parts = []
        if min_games > 1:
            footer_parts.append(f"Players must have at least {min_games} games with the specified filter to qualify.")
        active_filters = _filter_summary(match_filters)
        if active_filters:
            footer_parts.append("Filters: " + "; ".join(active_filters))
        if footer_parts:
            embed.set_footer(text=" • ".join(footer_parts))

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

    MAP_HELP = """
Shortcut leaderboard for one map.

**Usage:** `!map <map name> [stat] [champion/role] [limit] [-b] [-m <games>] [filters]`

This is the same as using `!lb ... map <map name>`.

**Examples:**
- `!map jaguar falls` - Winrate leaderboard on Jaguar Falls.
- `!map brightmarsh wr support` - Support winrate on Brightmarsh.
- `!map stone keep night kp moji` - Moji KP on Stone Keep Night.
- `!map ascension peak dmg flank 10` - Top 10 flank damage/min on Ascension Peak.
- `!map serpent beach wr barik team2` - Barik WR on Team 2 on Serpent Beach.
- `!map jaguar falls wr inara 4-3` - Inara WR in 4-3 games on Jaguar Falls.
- `!map brightmarsh dhpm ying` - Ying damage + healing/min on Brightmarsh.
"""

    @commands.command(name="map", aliases=["maplb"], help=MAP_HELP)
    async def map_cmd(self, ctx, *args):
        if not args:
            await ctx.send("Usage: `!map <map name> [stat] [champion/role] [filters]`")
            return

        resolved_map, remaining_args = _resolve_leading_map(args)
        if not resolved_map:
            await ctx.send(f"Could not find a map matching `{' '.join(str(arg) for arg in args)}`.")
            return

        leaderboard_args = list(remaining_args) + ["map", resolved_map]
        await self.leaderboard_cmd.callback(self, ctx, *leaderboard_args)

    @commands.command(
        name="compare",
        help=(
            "Head-to-head comparison between two players.\n"
            "Usage: `!compare <user1|ign> [user2|ign]`\n"
            "If the second player is omitted, compares against you. Each argument "
            "accepts mentions, IDs, usernames, main IGNs, or alt IGNs.\n"
            "Examples:\n"
            "- `!compare pjamo`\n"
            "- `!compare @user`\n"
            "- `!compare pjamo nozy`\n"
            "- `!compare @user pjamo`\n"
            "- `!compare lulub DTC`"
        ),
    )
    async def compare_cmd(self, ctx, user1: PlayerConverter, user2: PlayerConverter = None):
        # If user2 is not provided, default to the command author
        user2 = user2 or ctx.author

        if user1 == user2:
            await ctx.send("You can't compare a player to themselves!")
            return

        pid1 = resolve_player_id(user1)
        pid2 = resolve_player_id(user2)
        if not pid1 or not pid2:
            await ctx.send("Could not find stats for one or both players. Ensure they have linked their IGNs.")
            return

        result = compare_by_player_ids(pid1, pid2)
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
        user1_icon = _avatar_url(user1)
        user2_icon = _avatar_url(user2)
        if user1_icon:
            embed.set_author(name=user1.display_name, icon_url=user1_icon)
        else:
            embed.set_author(name=user1.display_name)
        embed.set_footer(
            text=f"Compared with {user2.display_name}",
            icon_url=user2_icon if user2_icon else None,
        )

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

    CHAMPION_LEADERBOARD_HELP = """
Shows champion rankings aggregated across all players.

**Usage:** `!champ_lb [stat] [role] [limit] [-b] [-m <games>] [filters]`

**Arguments:**
- `[stat]`: The statistic to rank by. Defaults to `winrate`.
- `[role]`: Filter by a role (`damage`, `flank`, `tank`, `support`, `point tank`, `off tank`).
- `[limit]`: The number of champions to show. Defaults to `20`.
- `[-b]`: Optional flag to show the bottom of the leaderboard.
- `[-m <games>]`: Optional flag to set a minimum number of games to qualify. Defaults to 1.
- `[filters]`: `map <name>`, `wins`, `losses`, `team1/team2`, `4-3`, `close`, `stomp`, `sweep`, `with <player>`, `against <player>`

**Available Stats:**
- `winrate` (or `wr`): Overall Winrate
- `kda`: Kill/Death/Assist Ratio
- `kp`: Kill Participation (% of team kills + assists)
- `dmg_share`: Damage Share (% of team damage)
- `kpm`: Kills per Minute
- `deaths_pm`: Deaths per Minute
- `dmg` (or `dpm`): Damage per Minute
- `taken_pm`: Damage Taken per Minute
- `heal_pm`: Healing per Minute
- `dhpm`: Damage + Healing per Minute
- `self_heal_pm`: Self Healing per Minute
- `creds_pm`: Credits per Minute
- `avg_kills`: Average Kills per Match
- `avg_deaths`: Average Deaths per Match
- `avg_dmg`: Average Damage per Match
- `avg_taken`: Average Damage Taken per Match
- `delta`: Average Damage Delta (Dealt - Taken)
- `avg_heal`: Average Healing per Match
- `avg_self_heal`: Average Self Healing per Match
- `avg_shield`: Average Shielding per Match
- `avg_creds`: Average Credits per Match
- `obj_time`: Average Objective Time per Match

**Examples:**
- `!clb`: Top champions by winrate.
- `!clb dmg`: Top 20 champions by damage per minute.
- `!clb winrate tank`: Top 20 tanks by winrate.
- `!clb winrate point tank`: Top point tanks by winrate.
- `!clb kp -m 50`: Top champions by kill participation (min 50 games).
- `!clb deaths_pm -b`: Bottom 20 champions by deaths per minute.
- `!clb wr support team1 map stone keep night`: Support champion winrate on Team 1 for Stone Keep Night.
- `!clb kp close`: Champion KP in close games only.
- `!clb wr flank losses`: Flank champion winrate in losses only.
- `!clb dmg damage team2 stomp`: Damage champion damage/min on Team 2 in stomps.
- `!clb wr support against nozy`: Support champion winrate against nozy.
- `!clb dhpm support`: Support champion damage + healing per minute.
"""

    @commands.command(name="champ_lb", aliases=["clb", "champleaderboard"], help=CHAMPION_LEADERBOARD_HELP)
    async def champion_leaderboard_cmd(self, ctx, *args):
        # --- Stat Mapping (Same as player leaderboard) ---
        stat_map = {
            "winrate": ("Winrate", "winrate", lambda v, s: f"{v:.2f}% ({s['wins']}-{s['losses']})"),
            "kda": ("KDA Ratio", "kda", lambda v, s: f"{v:.2f} ({s['k']}/{s['d']}/{s['a']})"),
            "kp": ("Kill Participation", "kp", lambda v, s: f"{v:.2f}%"),
            "dmg_share": ("Damage Share", "dmg_share", lambda v, s: f"{v:.2f}%"),
            "kpm": ("Kills/Min", "kills_pm", lambda v, s: f"{v:.2f}"),
            "deaths_pm": ("Deaths/Min", "deaths_pm", lambda v, s: f"{v:.2f}"),
            "dmg_pm": ("Damage/Min", "damage_dealt_pm", lambda v, s: f"{int(v):,}"),
            "taken_pm": ("Damage Taken/Min", "damage_taken_pm", lambda v, s: f"{int(v):,}"),
            "heal_pm": ("Healing/Min", "healing_pm", lambda v, s: f"{int(v):,}"),
            "dhpm": ("Damage + Healing/Min", "damage_healing_pm", lambda v, s: f"{int(v):,}"),
            "dmg_heal_pm": ("Damage + Healing/Min", "damage_healing_pm", lambda v, s: f"{int(v):,}"),
            "dmg_healing_pm": ("Damage + Healing/Min", "damage_healing_pm", lambda v, s: f"{int(v):,}"),
            "damage_healing_pm": ("Damage + Healing/Min", "damage_healing_pm", lambda v, s: f"{int(v):,}"),
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

        args, match_filters, filter_error = await _extract_match_filters(ctx, args)
        if filter_error:
            await ctx.send(filter_error)
            return
        
        # --- 1. Argument Parsing ---
        stat_alias = "winrate"
        limit = 20
        show_bottom = False
        role_filter = None
        min_games = None
        
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
        
        # Check for role filter in unprocessed args
        if unprocessed_args:
            full_filter_str = " ".join(unprocessed_args).lower()
            
            matched_role = resolve_role_name(full_filter_str)

            if matched_role:
                role_filter = matched_role

        limit = max(1, min(limit, 50))
        
        if min_games is None:
            min_games = 1
        
        # --- 2. Fetch Data ---
        display_name, data_key, formatter = stat_map[stat_alias]
        leaderboard_data = get_champion_leaderboard(
            data_key, limit, show_bottom,
            role=role_filter, min_games=min_games, filters=match_filters
        )
        
        if not leaderboard_data:
            filter_msg = f" in the '{role_filter}' role" if role_filter else ""
            await ctx.send(f"Could not generate a champion leaderboard for `{display_name}`{filter_msg}. No qualified champion data found.")
            return

        # --- 3. Build Embed ---
        filter_text = f" ({role_filter})" if role_filter else ""
        filter_text += _title_filter_suffix(match_filters)
        
        embed_title = f"🏆 {'Bottom' if show_bottom else 'Top'} {len(leaderboard_data)} Champions by {display_name}{filter_text}"
        embed_color = 0xE74C3C if show_bottom else 0x2ECC71
        embed = discord.Embed(title=embed_title, color=embed_color)
        
        footer_parts = []
        if min_games > 1:
            footer_parts.append(f"Champions must have at least {min_games} games played to qualify.")
        active_filters = _filter_summary(match_filters)
        if active_filters:
            footer_parts.append("Filters: " + "; ".join(active_filters))
        if footer_parts:
            embed.set_footer(text=" • ".join(footer_parts))

        description = []
        for i, data_row in enumerate(leaderboard_data):
            champ_name = data_row['champ']
            value = data_row['value']
            games = data_row['games_played']
            
            rank = (data_row['total_champions'] - i) if show_bottom else (i + 1)
            formatted_value = formatter(value, data_row)

            description.append(f"`{rank:2}.` **{champ_name}** - {formatted_value} *({games} games)*")
        
        embed.description = "\n".join(description)
        await ctx.send(embed=embed)


async def setup(bot):
    await bot.add_cog(Stats(bot))

