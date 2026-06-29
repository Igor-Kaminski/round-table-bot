# cogs/general.py

import discord
from discord import app_commands
from discord.ext import commands
from db import (
    get_ign_link_info,
    get_ign_for_discord_id,
    get_player_info,
    link_ign,
    unlink_ign,
    add_alt_ign,
)
from utils.checks import is_exec
from utils.views import LinkConfirmView


class SlashContext:
    def __init__(self, interaction: discord.Interaction):
        self.interaction = interaction
        self.author = interaction.user
        self.user = interaction.user
        self.guild = interaction.guild
        self.channel = interaction.channel
        self.bot = interaction.client
        self.message = getattr(interaction, "message", None)

    async def send(self, content=None, **kwargs):
        kwargs = {key: value for key, value in kwargs.items() if value is not None}
        if self.interaction.response.is_done():
            return await self.interaction.followup.send(content=content, **kwargs)
        return await self.interaction.response.send_message(content=content, **kwargs)


class General(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    HELP_GROUPS = {
        "Stats": [
            ("stats", "Player stats by role/champion/filter."),
            ("top", "Your champion table with custom stat columns."),
            ("history", "Recent match history for a player."),
            ("match", "Saved screenshot for a match ID."),
        ],
        "Leaderboards & Maps": [
            ("lb", "Player leaderboard for WR, KP, DPM, healing, and more."),
            ("clb", "Champion leaderboard across roles and filters."),
            ("map", "Shortcut leaderboard for one map."),
            ("mapwr", "A player's winrate on every map."),
            ("cstats", "Overall server stats for one champion."),
            ("talents", "Talent winrates for one champion."),
            ("pickrate", "Champion pickrates across filters."),
            ("champmapwr", "One champion's record on every map."),
            ("champcompare", "Compare two champions overall and by map."),
        ],
        "Matchups": [
            ("compare", "Head-to-head comparison between two players."),
            ("mates", "Best and worst teammates by winrate together."),
            ("enemies", "Enemy players you beat most and lose to most."),
            ("duo", "Focused record with one teammate."),
            ("rivals", "Focused record against one enemy player."),
            ("withchamps", "Records when champions are on your team."),
            ("againstchamps", "Records when champions are against you."),
            ("champwith", "Allied champion records for a champion."),
            ("champagainst", "Enemy champion matchup records for a champion."),
        ],
        "Filters & Examples": [
            ("filters", "Show every filter style: season, map, score, with/against."),
            ("examples", "Command examples from simple to spicy."),
        ],
        "Account": [
            ("link", "Link Discord to IGN."),
            ("add_alt", "Add an alternate IGN."),
            ("alts", "Show linked main and alt IGNs."),
            ("unlink", "Unlink your Discord from an IGN."),
        ],
        "Exec": [
            ("query", "Run a SELECT query."),
            ("ingest_text", "Insert a pasted scoreboard."),
            ("delete_match", "Delete a match by ID."),
            ("add", "Attach a saved screenshot to a match."),
            ("replace", "Replace a saved match screenshot."),
            ("fetch_embeds", "Backfill queue embeds."),
            ("link_disc", "Update a player's Discord ID."),
            ("show_alts", "Show a player's alts."),
            ("delete_alt", "Delete an alt IGN."),
            ("player_id", "Get internal player ID."),
            ("old_stats", "Legacy raw stats lookup."),
        ],
    }

    HELP_GROUP_ALIASES = {
        "leaderboards": "Leaderboards & Maps",
        "leaderboard commands": "Leaderboards & Maps",
        "maps": "Leaderboards & Maps",
        "map commands": "Leaderboards & Maps",
        "matchups": "Matchups",
        "matchupcommands": "Matchups",
        "matchup commands": "Matchups",
        "account": "Account",
        "accounts": "Account",
        "exec": "Exec",
        "admin": "Exec",
        "filtercommands": "Filters & Examples",
        "filter commands": "Filters & Examples",
        "examplecommands": "Filters & Examples",
        "example commands": "Filters & Examples",
        "statcommands": "Stats",
        "stat commands": "Stats",
        "stats commands": "Stats",
    }

    def _help_overview_embed(self):
        embed = discord.Embed(
            title="BOSSMAN Help",
            description=(
                "Use `!help <command>` for full details, or `!examples <topic>` for practical commands.\n"
                "Useful starts: `!help leaderboards`, `!help matchups`, `!help statcommands`."
            ),
            color=discord.Color.blue(),
        )
        for group_name, commands_info in self.HELP_GROUPS.items():
            value = "\n".join(f"`!{name}` - {summary}" for name, summary in commands_info)
            embed.add_field(name=group_name, value=value, inline=False)
        return embed

    def _help_group_embed(self, group_name):
        commands_info = self.HELP_GROUPS[group_name]
        embed = discord.Embed(
            title=f"{group_name} Commands",
            description="Use `!help <command>` for full usage, aliases, and examples.",
            color=discord.Color.blue(),
        )
        embed.add_field(
            name="Commands",
            value="\n".join(f"`!{name}` - {summary}" for name, summary in commands_info),
            inline=False,
        )
        return embed

    def _command_help_embed(self, command):
        aliases = ", ".join(f"`!{alias}`" for alias in command.aliases) if command.aliases else "None"
        help_text = command.help or command.short_doc or "No detailed help available."
        if len(help_text) > 3500:
            help_text = help_text[:3490].rstrip() + "\n..."

        embed = discord.Embed(
            title=f"!{command.qualified_name}",
            description=help_text,
            color=discord.Color.green(),
        )
        embed.add_field(name="Aliases", value=aliases, inline=False)
        return embed

    async def _send_help(self, ctx, topic=None):
        topic_key = (topic or "").strip().lower()
        if not topic_key:
            await ctx.send(embed=self._help_overview_embed())
            return

        command = self.bot.get_command(topic_key)
        if command:
            await ctx.send(embed=self._command_help_embed(command))
            return

        group_name = self.HELP_GROUP_ALIASES.get(topic_key)
        if group_name:
            await ctx.send(embed=self._help_group_embed(group_name))
            return

        await ctx.send(f"Unknown help topic `{topic}`. Try `!help`, `!help leaderboards`, or `!examples`.")

    @commands.command(
        name="help",
        aliases=["h", "commands"],
        help="Show grouped help. Usage: `!help [command|group]`, e.g. `!help leaderboard` or `!help mates`.",
    )
    async def help_cmd(self, ctx, *, topic: str = None):
        await self._send_help(ctx, topic)

    @app_commands.command(name="help", description="Show grouped help for bot commands.")
    @app_commands.describe(topic="Optional command or group, e.g. leaderboard, matchups, mates.")
    async def help_slash(self, interaction: discord.Interaction, topic: str = None):
        await self._send_help(SlashContext(interaction), topic)

    @commands.command(
        name="link",
        help=(
            "Link your Discord account to an in-game name (IGN).\n"
            "Usage: `!link <ign>`\n"
            "If the IGN has prior match history from scoreboard ingestion, those "
            "matches are automatically claimed for your account. If you already "
            "have a primary IGN linked, you'll be prompted to confirm replacing "
            "it (the old one is kept as an alt)."
        ),
    )
    async def link(self, ctx, ign: str):
        discord_id = str(ctx.author.id)
        try:
            existing_discord_id, ign_exists, actual_ign = get_ign_link_info(ign)
            user_main_ign = get_ign_for_discord_id(discord_id)

            if ign_exists and existing_discord_id is not None and str(existing_discord_id) == discord_id:
                await ctx.send(f"IGN `{actual_ign}` is already linked to your account.")
                return

            if ign_exists and existing_discord_id is not None:
                await ctx.send(
                    f"❌ IGN `{actual_ign}` is already linked to another Discord account. "
                    "Please contact an exec if this is an error."
                )
                return

            if user_main_ign:
                view = LinkConfirmView(discord_id, ign)
                extra = (
                    f"\nConfirming will also merge any existing match history under `{actual_ign}` into your account."
                    if ign_exists
                    else ""
                )
                await ctx.send(
                    f"⚠️ You already have an IGN (`{user_main_ign}`) linked to your account.\n"
                    "If this is an alternate account, use `!add_alt <ign>` instead.\n"
                    f"Otherwise, you can confirm to **replace** your primary IGN with `{ign}`.{extra}",
                    view=view,
                )
                return

            success = link_ign(ign, discord_id)
            if success:
                if ign_exists:
                    await ctx.send(
                        f"✅ Successfully linked your Discord to IGN `{actual_ign}`. "
                        "Existing match history under that IGN is now attached to your account."
                    )
                else:
                    await ctx.send(f"✅ Successfully linked your Discord to IGN `{ign}`.")
            else:
                await ctx.send("❌ Failed to link your Discord to IGN. Please contact an exec.")
        except Exception as e:
            print(f"Error in link command: {e}")
            await ctx.send("An error occurred while linking your account.")

    @app_commands.command(name="link", description="Link your Discord account to an in-game name.")
    @app_commands.describe(ign="Your in-game name.")
    async def link_slash(self, interaction: discord.Interaction, ign: str):
        await self.link.callback(self, SlashContext(interaction), ign)

    @commands.command(
        name="add_alt",
        help=(
            "Add an alternate in-game name to your account.\n"
            "Usage: `!add_alt <alt_ign>` — for yourself.\n"
            "       `!add_alt @user <alt_ign>` — execs only, targets another user.\n"
            "Any existing match history recorded under the alt IGN (e.g. from "
            "scoreboards ingested before you linked) is merged into your stats."
        ),
    )
    async def add_alt_cmd(self, ctx, *, raw: str = None):
        if not raw or not raw.strip():
            await ctx.send(
                "Usage: `!add_alt <alt_ign>` or (execs only) `!add_alt @user <alt_ign>`"
            )
            return

        raw = raw.strip()
        target_user = ctx.author
        alt_ign = raw

        head, _, rest = raw.partition(" ")
        rest = rest.strip()
        if rest:
            try:
                candidate = await commands.MemberConverter().convert(ctx, head)
                target_user = candidate
                alt_ign = rest
            except commands.BadArgument:
                pass

        if target_user.id != ctx.author.id and not is_exec(ctx):
            await ctx.send(
                "You can only add alts to your own account. Ask an exec to target someone else."
            )
            return

        try:
            result = add_alt_ign(str(target_user.id), alt_ign)
        except Exception as e:
            print(f"Error in add_alt command: {e}")
            await ctx.send("An error occurred while adding the alt IGN.")
            return

        who = "your account" if target_user.id == ctx.author.id else target_user.mention

        if result["success"]:
            merged = result["merged_matches"]
            if merged:
                await ctx.send(
                    f"✅ Added alt IGN `{alt_ign}` to {who}. "
                    f"Merged **{merged}** existing match{'es' if merged != 1 else ''} into "
                    f"{'your' if target_user.id == ctx.author.id else 'their'} stats."
                )
            else:
                await ctx.send(
                    f"✅ Added alt IGN `{alt_ign}` to {who}. "
                    "No prior match history was recorded under that IGN."
                )
            return

        reason = result.get("reason")
        if reason == "no_main_ign":
            if target_user.id == ctx.author.id:
                await ctx.send("❌ Link a main IGN first with `!link <ign>` before adding alts.")
            else:
                await ctx.send(f"❌ {target_user.mention} doesn't have a main IGN linked yet.")
        elif reason == "already_linked":
            await ctx.send(f"❌ `{alt_ign}` is already the main IGN on that account.")
        elif reason == "duplicate_alt":
            await ctx.send(f"❌ `{alt_ign}` is already listed as an alt.")
        elif reason == "conflict_other_user":
            await ctx.send(
                f"❌ `{alt_ign}` is already claimed by a different Discord account. "
                "Ask an exec to sort it out."
            )
        elif reason == "empty":
            await ctx.send("❌ Please provide an alt IGN.")
        else:
            await ctx.send(f"❌ Failed to add alt IGN `{alt_ign}`. Please contact an exec.")

    @app_commands.command(name="add_alt", description="Add an alternate in-game name.")
    @app_commands.describe(alt_ign="Alternate in-game name.", user="Optional target user for execs.")
    async def add_alt_slash(self, interaction: discord.Interaction, alt_ign: str, user: discord.Member = None):
        raw = f"{user.mention} {alt_ign}" if user else alt_ign
        await self.add_alt_cmd.callback(self, SlashContext(interaction), raw=raw)

    @commands.command(
        name="alts",
        aliases=["my_alts"],
        help=(
            "Show a player's linked in-game names (main + alternates).\n"
            "Usage: `!alts` (yourself), `!alts @user`, or `!alts <ign>`."
        ),
    )
    async def alts_cmd(self, ctx, *, target: str = None):
        # Default target is the command author.
        discord_id = str(ctx.author.id)
        display = ctx.author.display_name
        is_self = True

        if target:
            target = target.strip()
            try:
                member = await commands.MemberConverter().convert(ctx, target)
                discord_id = str(member.id)
                display = member.display_name
                is_self = member.id == ctx.author.id
            except commands.BadArgument:
                # Fall back to IGN lookup via the converter chain.
                from utils.converters import PlayerConverter
                try:
                    resolved = await PlayerConverter().convert(ctx, target)
                except commands.BadArgument as e:
                    await ctx.send(str(e))
                    return
                resolved_id = getattr(resolved, "id", None)
                if resolved_id is None:
                    await ctx.send(
                        f"`{target}` has match history but no Discord account is linked to it yet."
                    )
                    return
                discord_id = str(resolved_id)
                display = getattr(resolved, "display_name", target)
                is_self = resolved_id == ctx.author.id

        info = get_player_info(discord_id)
        if not info:
            if is_self:
                await ctx.send("You don't have any IGN linked yet. Use `!link <ign>` to link one.")
            else:
                await ctx.send(f"{display} doesn't have any IGN linked.")
            return

        title = "Your linked IGNs" if is_self else f"Linked IGNs for {display}"
        lines = [f"**Main:** `{info['main_ign']}`"]
        if info["alt_igns"]:
            alts_str = ", ".join(f"`{a}`" for a in info["alt_igns"])
            lines.append(f"**Alts ({len(info['alt_igns'])}):** {alts_str}")
        else:
            lines.append("**Alts:** _none_")

        embed = discord.Embed(
            title=title,
            description="\n".join(lines),
            color=discord.Color.blue(),
        )
        await ctx.send(embed=embed)

    @app_commands.command(name="alts", description="Show a player's linked in-game names.")
    @app_commands.describe(user="Optional Discord member.", player="Optional IGN.")
    async def alts_slash(self, interaction: discord.Interaction, user: discord.Member = None, player: str = None):
        target = user.mention if user else player
        await self.alts_cmd.callback(self, SlashContext(interaction), target=target)

    @commands.command(
        name="unlink",
        help=(
            "Unlink your Discord account from your IGN. Your match history is "
            "preserved and can be reclaimed later with `!link <ign>`."
        ),
    )
    async def unlink(self, ctx):
        discord_id = str(ctx.author.id)
        try:
            user_ign = get_ign_for_discord_id(discord_id)
            if not user_ign:
                await ctx.send("You don't have an IGN linked to your account.")
                return

            success = unlink_ign(discord_id)
            if success:
                await ctx.send(
                    f"✅ Unlinked IGN `{user_ign}` from your Discord account. "
                    "Your match history is preserved and can be reclaimed with `!link`."
                )
            else:
                await ctx.send("❌ Failed to unlink your account. Please contact an administrator.")
        except Exception as e:
            print(f"Error in unlink command: {e}")
            await ctx.send("An error occurred while unlinking your account.")

    @app_commands.command(name="unlink", description="Unlink your Discord account from your IGN.")
    async def unlink_slash(self, interaction: discord.Interaction):
        await self.unlink.callback(self, SlashContext(interaction))


async def setup(bot):
    await bot.add_cog(General(bot))
