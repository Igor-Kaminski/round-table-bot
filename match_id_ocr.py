import easyocr
import re
import tempfile
from run import bot, ALLOWED_CHANNELS

reader = easyocr.Reader(['en'])


def get_match_id(img_path):
    results = reader.readtext(img_path) # stores text by section
    for _, text, prob in results:
        found_id = re.search(r'ID\s\d{10}', text) # check if section is the match ID
        if found_id:
            match_id = int(text[3:13]) # store the 10 digit ID
            return match_id

    return None

# --- temporarily save images sent in match-results and extract the match id ---
async def match_results_id_ocr(message):
    if message.author == bot.user:
        return

    if message.channel.name != "match-results" or message.channel.name not in ALLOWED_CHANNELS:
        return

    if message.attachments:
        for attachment in message.attachments:
            if attachment.filename.lower().endswith(('.png', '.jpg', '.jpeg')):
                with tempfile.NamedTemporaryFile(delete=True, suffix=attachment.filename) as temp_img:
                    await attachment.save(temp_img.name)
                    match_id = get_match_id(temp_img.name)
                    if match_id:
                        await message.channel.send(f"Match ID: {match_id}")
                    else:
                        await message.channel.send("Match ID not found.")