# cogs/general.py

import discord
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


class General(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

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


async def setup(bot):
    await bot.add_cog(General(bot))
