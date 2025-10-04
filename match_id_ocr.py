import easyocr
import re

reader = easyocr.Reader(['en'])

# --- Returns match ID ---
def get_match_id(img_path):
    results = reader.readtext(img_path) # stores text by section
    for _, text, prob in results:
        found_id = re.search(r'ID\s\d{10}', text) # check if section is the match ID
        if found_id:
            match_id = int(text[3:13]) # store the 10 digit ID
            return match_id

    return None