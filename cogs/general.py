# cogs/general.py

import discord
from discord.ext import commands
from db import (
    get_ign_link_info,
    get_ign_for_discord_id,
    link_ign,
)
from utils.views import LinkConfirmView


class General(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @commands.command(name="link", help="Link your Discord account to an in-game name (IGN).")
    async def link(self, ctx, ign: str):
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
            print(f"Error in link command: {e}")
            await ctx.send("An error occurred while linking your account.")


async def setup(bot):
    await bot.add_cog(General(bot))

