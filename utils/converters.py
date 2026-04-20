# utils/converters.py

import discord
from discord.ext import commands
from db import get_discord_id_for_ign, get_player_by_ign, get_player_id


DEFAULT_AVATAR_URL = "https://cdn.discordapp.com/embed/avatars/0.png"


class _UnlinkedAvatar:
    """Stand-in for ``discord.Member.display_avatar`` used by UnlinkedPlayer."""

    url = DEFAULT_AVATAR_URL


class UnlinkedPlayer:
    """Lightweight proxy representing a player row with no linked Discord user.

    Returned by :class:`PlayerConverter` when an IGN matches a row in the
    ``players`` table (main or alt) but no Discord account has claimed it yet.
    It exposes just enough of the :class:`discord.Member` interface for stats
    commands to render without reaching for guild data.
    """

    def __init__(self, player_id, ign):
        self.player_id = player_id
        self.ign = ign
        self.id = None
        self.name = ign
        self.display_name = ign
        self.mention = f"`{ign}`"
        self.display_avatar = _UnlinkedAvatar()
        self.is_unlinked = True

    def __eq__(self, other):
        return isinstance(other, UnlinkedPlayer) and self.player_id == other.player_id

    def __hash__(self):
        return hash(("UnlinkedPlayer", self.player_id))


def resolve_player_id(target_user):
    """Return the internal ``player_id`` for a Discord user or :class:`UnlinkedPlayer`."""
    pid = getattr(target_user, "player_id", None)
    if pid is not None:
        return pid
    discord_id = getattr(target_user, "id", None)
    if discord_id is None:
        return None
    return get_player_id(str(discord_id))


class PlayerConverter(commands.Converter):
    async def convert(self, ctx, argument):
        if argument.lower() == "me":
            return ctx.author

        try:
            return await commands.MemberConverter().convert(ctx, argument)
        except commands.MemberNotFound:
            if argument.isdigit():
                try:
                    return await ctx.bot.fetch_user(int(argument))
                except discord.NotFound:
                    pass

            lower_arg = argument.lower()
            if ctx.guild is not None:
                for member in ctx.guild.members:
                    if member.display_name.lower() == lower_arg or member.name.lower() == lower_arg:
                        return member
                for member in ctx.guild.members:
                    if member.display_name.lower().startswith(lower_arg) or member.name.lower().startswith(lower_arg):
                        return member

            found_id = get_discord_id_for_ign(argument)
            if found_id:
                try:
                    return await ctx.bot.fetch_user(int(found_id))
                except discord.NotFound:
                    pass

            # Final fallback: the IGN exists in the DB but isn't linked to any
            # Discord account (typical after a scoreboard ingest). Return a
            # proxy so stats commands can still render.
            row = get_player_by_ign(argument)
            if row:
                return UnlinkedPlayer(row["player_id"], row["player_ign"])

            raise commands.BadArgument(f'User or IGN "{argument}" not found.')
