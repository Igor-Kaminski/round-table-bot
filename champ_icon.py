import os
import json
from PIL import Image
import imagehash

# -------------------------------
# Config
# -------------------------------
ICON_FOLDER = "champion_icons"                 # input folder (.webp icons)
OUTPUT_FOLDER = "champion_icons_scoreboard"    # cropped/resized icons
HASH_JSON = "champion_hashes.json"             # output hash file
TARGET_W, TARGET_H = 228, 101                  # scoreboard champ size
os.makedirs(OUTPUT_FOLDER, exist_ok=True)


# -------------------------------
# Functions
# -------------------------------
def normalize_icon(path, save_path, target_w=TARGET_W, target_h=TARGET_H):
    """
    Normalize champion icon to scoreboard format:
    1. Resize input to 256x256 (ensures consistency)
    2. Crop center slice to target_w x target_h (228x101)
    """
    img = Image.open(path).convert("RGB")

    # Step 1: Resize to square for consistency
    img = img.resize((256, 256))

    # Step 2: Compute crop (centered)
    left = (256 - target_w) // 2
    top = (256 - target_h) // 2
    right = left + target_w
    bottom = top + target_h
    img_cropped = img.crop((left, top, right, bottom))

    # Save cropped version
    img_cropped.save(save_path, "PNG")
    return img_cropped


def build_hashes(icon_folder=ICON_FOLDER, output_folder=OUTPUT_FOLDER, hash_json=HASH_JSON):
    """
    Normalize all icons and build perceptual hashes.
    """
    hashes = {}
    for file in os.listdir(icon_folder):
        if file.lower().endswith(".webp"):
            name = os.path.splitext(file)[0]  # use file name as key
            src = os.path.join(icon_folder, file)
            dst = os.path.join(output_folder, name + ".png")

            # Normalize (resize + crop)
            icon = normalize_icon(src, dst)

            # Compute perceptual hash
            champ_hash = str(imagehash.phash(icon))
            hashes[name] = champ_hash

    # Save hashes JSON
    with open(hash_json, "w") as f:
        json.dump(hashes, f, indent=2)

    print(f"✅ Processed {len(hashes)} icons")
    print(f"✅ Cropped scoreboard-style icons saved to: {output_folder}")
    print(f"✅ Hashes saved to: {hash_json}")
    return hashes


# -------------------------------
# Run
# -------------------------------
if __name__ == "__main__":
    hashes = build_hashes()
    print(json.dumps(hashes, indent=2))
