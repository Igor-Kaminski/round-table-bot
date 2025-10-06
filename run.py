# run.py

import discord
from discord.ext import commands
import os
import dotenv
from db import create_database

# Load environment variables
dotenv.load_dotenv()

# Initialize database
create_database()

# Bot configuration
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
bot = commands.Bot(command_prefix=["!"], intents=intents)

GUILD_ID = int(os.getenv("GUILD_ID"))


@bot.event
async def on_ready():
    print(f"Logged in as {bot.user}")
    
    # Load all cogs
    cogs = ["cogs.admin", "cogs.general", "cogs.stats", "cogs.listeners"]
    for cog in cogs:
        try:
            await bot.load_extension(cog)
            print(f"✅ Loaded {cog}")
        except Exception as e:
            print(f"❌ Failed to load {cog}: {e}")
    
    # Sync slash commands
    try:
        synced = await bot.tree.sync(guild=discord.Object(id=GUILD_ID))
        print(f"Synced {len(synced)} command(s) to guild")
    except Exception as e:
        print(f"Failed to sync commands: {e}")


# Run the bot
bot.run(os.getenv("BOT_TOKEN"))
