# utils/views.py

import discord
from discord.ui import View, Button, Modal, TextInput, Select
from db import (
    link_ign,
    match_exists,
    queue_exists,
    insert_scoreboard,
)
from core.constants import CHAMPION_ROLES


class LinkConfirmView(View):
    def __init__(self, discord_id, ign):
        super().__init__(timeout=60)
        self.discord_id = discord_id
        self.ign = ign

    @discord.ui.button(label="Confirm (replace IGN)", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, button: Button):
        link_ign(self.ign, self.discord_id, force=True)
        await interaction.response.send_message(
            f"IGN `{self.ign}` has been linked to your account (previous link replaced).",
            ephemeral=True,
        )
        self.stop()

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: Button):
        await interaction.response.send_message("Linking cancelled.", ephemeral=True)
        self.stop()


class TopChampsView(View):
    def __init__(self, author_id, all_champ_data, target_user_name):
        super().__init__(timeout=90)
        self.author_id = author_id
        self.all_champ_data = all_champ_data
        self.target_user_name = target_user_name
        self.current_sort_key = "games"  # Default sort
        self.current_role_filter = None  # Default no filter

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        # Only allow the original command user to interact
        return interaction.user.id == self.author_id

    
    async def on_timeout(self) -> None:
        # The message is being deleted by `delete_after`, so we don't need to do anything here.
        # This prevents the bot from trying to edit a message that no longer exists.
        pass

    def _generate_description(self) -> str:
        """Generates the formatted text block based on current sort/filter."""
        
        filtered_data = self.all_champ_data
        if self.current_role_filter:
            filtered_data = [
                champ for champ in self.all_champ_data
                if CHAMPION_ROLES.get(champ["champ"]) == self.current_role_filter
            ]

        if not filtered_data:
            return f"```\nNo champions played in the '{self.current_role_filter}' role.\n```"

        sorted_data = sorted(filtered_data, key=lambda x: x[self.current_sort_key], reverse=True)

        lines = []
        # CHANGED: All columns are now left-aligned (<) with new widths to create spacing.
        header = f"{'Champion':<16}{'KDA':<8}{'WR':<9}{'Matches':<10}{'Time'}"
        separator = "-" * len(header)
        
        roles_to_display = [self.current_role_filter] if self.current_role_filter else ["Damage", "Flank", "Tank", "Support"]

        for role in roles_to_display:
            champs_in_role = [c for c in sorted_data if CHAMPION_ROLES.get(c["champ"]) == role]
            if not champs_in_role:
                continue
            
            lines.append(header)
            lines.append(separator)
            lines.append(f"#   {role}")
            
            for i, champ in enumerate(champs_in_role, 1):
                name = champ['champ']
                if len(name) > 12:
                    name = name[:11] + "…"
                kda, wr, matches, time_played = f"{champ['kda_ratio']:.2f}", f"{champ['winrate']:.1f}%", str(champ['games']), champ['time_played']
                
                # CHANGED: Data rows now match the header's left-alignment and spacing.
                lines.append(f"{str(i)+'.':<4}{name:<12}{kda:<8}{wr:<9}{matches:<10}{time_played}")
            lines.append("")
        
        return "```\n" + "\n".join(lines) + "\n```"

    @discord.ui.select(
        placeholder="Sort by Matches",
        options=[
            discord.SelectOption(label="Sort by Matches", value="games", description="Default sorting, most played first."),
            discord.SelectOption(label="Sort by KDA", value="kda_ratio", description="Highest KDA ratio first."),
            discord.SelectOption(label="Sort by Winrate", value="winrate", description="Highest winrate first."),
        ]
    )
    async def sort_select(self, interaction: discord.Interaction, select: Select):
        self.current_sort_key = select.values[0]
        select.placeholder = f"Sort by {select.values[0].replace('_', ' ').capitalize()}"
        
        new_description = self._generate_description()
        await interaction.response.edit_message(content=None, embed=discord.Embed(
            title=f"Top Champions for {self.target_user_name}",
            description=new_description,
            color=discord.Color.blue()
        ), view=self)

    @discord.ui.button(label="All Roles", style=discord.ButtonStyle.primary, row=2)
    async def all_roles_button(self, interaction: discord.Interaction, button: Button):
        self.current_role_filter = None
        new_description = self._generate_description()
        await interaction.response.edit_message(content=None, embed=discord.Embed(
            title=f"Top Champions for {self.target_user_name}",
            description=new_description,
            color=discord.Color.blue()
        ), view=self)

    @discord.ui.button(label="Damage", style=discord.ButtonStyle.secondary, row=2)
    async def damage_button(self, interaction: discord.Interaction, button: Button):
        self.current_role_filter = "Damage"
        new_description = self._generate_description()
        await interaction.response.edit_message(content=None, embed=discord.Embed(
            title=f"Top Champions for {self.target_user_name}",
            description=new_description,
            color=discord.Color.red()
        ), view=self)

    @discord.ui.button(label="Flank", style=discord.ButtonStyle.secondary, row=2)
    async def flank_button(self, interaction: discord.Interaction, button: Button):
        self.current_role_filter = "Flank"
        new_description = self._generate_description()
        await interaction.response.edit_message(content=None, embed=discord.Embed(
            title=f"Top Champions for {self.target_user_name}",
            description=new_description,
            color=discord.Color.purple()
        ), view=self)

    @discord.ui.button(label="Tank", style=discord.ButtonStyle.secondary, row=3)
    async def tank_button(self, interaction: discord.Interaction, button: Button):
        self.current_role_filter = "Tank"
        new_description = self._generate_description()
        await interaction.response.edit_message(content=None, embed=discord.Embed(
            title=f"Top Champions for {self.target_user_name}",
            description=new_description,
            color=discord.Color.orange()
        ), view=self)

    @discord.ui.button(label="Support", style=discord.ButtonStyle.secondary, row=3)
    async def support_button(self, interaction: discord.Interaction, button: Button):
        self.current_role_filter = "Support"
        new_description = self._generate_description()
        await interaction.response.edit_message(content=None, embed=discord.Embed(
            title=f"Top Champions for {self.target_user_name}",
            description=new_description,
            color=discord.Color.green()
        ), view=self)


class QueueNumModal(Modal):
    def __init__(self, match_data_text, author_id, parse_match_textbox_func):
        super().__init__(title="Enter Queue Number")
        self.queue_num_input = TextInput(label="Queue Number", required=True)
        self.add_item(self.queue_num_input)
        self.match_data_text = match_data_text
        self.author_id = author_id
        self.parse_match_textbox = parse_match_textbox_func

    async def on_submit(self, interaction):
        queue_num = self.queue_num_input.value.strip()
        try:
            cleaned_text = self.match_data_text.strip().strip("`")
            match_data = self.parse_match_textbox(cleaned_text)
            match_id = match_data["match_id"]

            if match_exists(match_id):
                await interaction.response.send_message(f"Match ID {match_id} already exists.", ephemeral=True)
                return
            if queue_exists(queue_num):
                await interaction.response.send_message(f"Queue number {queue_num} already exists.", ephemeral=True)
                return

            # Insert the data directly.
            insert_scoreboard(match_data, int(queue_num))
            await interaction.response.send_message(f"✅ Match {match_id} for queue {queue_num} successfully recorded.", ephemeral=True)

        except ValueError as ve:
            await interaction.response.send_message(f"Malformed match data: {ve}", ephemeral=True)
        except Exception as e:
            await interaction.response.send_message(f"Error processing match data: {e}", ephemeral=True)


class QueueNumView(View):
    def __init__(self, match_data_text, author_id, parse_match_textbox_func, is_exec_func):
        super().__init__(timeout=300)
        self.match_data_text = match_data_text
        self.author_id = author_id
        self.parse_match_textbox = parse_match_textbox_func
        self.is_exec = is_exec_func

    @discord.ui.button(label="Enter Queue Number", style=discord.ButtonStyle.primary)
    async def enter_queue(self, interaction: discord.Interaction, button: Button):
        if not self.is_exec(interaction):
            await interaction.response.send_message("You don't have permission to do this.", ephemeral=True)
            return
        await interaction.response.send_modal(QueueNumModal(self.match_data_text, self.author_id, self.parse_match_textbox))
