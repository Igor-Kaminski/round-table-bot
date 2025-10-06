# utils/converters.py

import discord
from discord.ext import commands
from db import get_discord_id_for_ign


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
                    return await ctx.bot.fetch_user(int(argument))
                except discord.NotFound:
                    pass

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
                    return await ctx.bot.fetch_user(int(found_id))
                except discord.NotFound:
                    pass

            raise commands.BadArgument(f'User or IGN "{argument}" not found.')
