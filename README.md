# ROUND TABLE BOT

### Requirements

* Python 3.8+
* Bot Token from the Discord Developer portal

### 2. Setup & Configuration
1.  **Dependencies:** You'll need to install the required Python libraries. 
    ```bash
    pip install python-dotenv
    pip install discord.py
    ```

2.  **Environment File:** Create a file named `.env` in the same directory as your `run.py` file. This file will store your secret keys. Add the following lines, replacing the placeholder values with your own:
    ```env
    BOT_TOKEN=YOUR_DISCORD_BOT_TOKEN_HERE
    GUILD_ID=YOUR_SERVER_ID_HERE
    ```

### 3. Running the Bot
Once the setup is complete, you can start the bot by running the main Python script from your terminal:
```bash
python bot.py