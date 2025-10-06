# utils/checks.py

BOT_PERMISSION_ROLE_NAMES = ["Executive", "Bot Access"]
BOT_PERMISSION_USER_IDS = [163861584379248651]  # Nick


def is_exec(ctx_or_interaction):
    """Check if user has executive permissions."""
    # Get the user object, whether from a command (author) or a button click (user)
    user = getattr(ctx_or_interaction, "author", None)
    if user is None:
        user = getattr(ctx_or_interaction, "user", None)

    # If we couldn't find a user, deny permission
    if user is None:
        return False

    # The rest of the permission checks
    if hasattr(user, "roles"):
        if any(role.name in BOT_PERMISSION_ROLE_NAMES for role in user.roles):
            return True
    if user.id in BOT_PERMISSION_USER_IDS:
        return True

    return False
