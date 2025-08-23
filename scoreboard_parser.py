# scoreboard_parser.py (row/column boxed OCR)
import cv2
import json
import imagehash
from PIL import Image
import easyocr
import numpy as np
import re
from pathlib import Path

HASH_JSON = "champion_hashes.json"
SCOREBOARD_IMG = "TeamMatch.png"
ICON_W, ICON_H = 228, 101
OCR_LANGS = ['en']

KNOWN_REGIONS = ["North America", "Europe",
                 "Brazil", "Australia", "Asia", "Russia", "Japan"]
KNOWN_MAPS = [
    "Stone Keep (Night)", "Stone Keep", "Jaguar Falls", "Serpent Beach", "Brightmarsh", "Frozen Guard",
    "Frog Isle", "Ascension Peak", "Fish Market", "Timber Mill", "Warder's Gate", "Splitstone Quarry",
    "Ice Mines", "Bazaar", "Shattered Desert"
]

# ---------------- utils ----------------


def load_hashes(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)  # {key: hex}


def phash_distance(h1, hex2):  # ImageHash vs hex string
    return h1 - imagehash.hex_to_hash(hex2)


def to_int(s):
    s = s.replace(",", "").strip()
    return int(s) if re.fullmatch(r"\d+", s or "") else 0


def parse_kda(s):
    m = re.search(r"(\d+)\s*/\s*(\d+)\s*/\s*(\d+)", s or "")
    return (int(m.group(1)), int(m.group(2)), int(m.group(3))) if m else (0, 0, 0)


def pick_best_text(blocks, numeric=False):
    """Join OCR cell pieces left→right; optionally allow only numbers/commas/slash."""
    blocks.sort(key=lambda t: t[0])  # by x
    txt = " ".join(t for _, t in blocks).strip()
    if numeric:
        # keep digits, commas, slashes
        txt = re.sub(r"[^0-9,/\s]", "", txt).strip()
    return txt

# ---------------- core detector(s) ----------------


class Scoreboard:
    def __init__(self, hash_json=HASH_JSON, ocr_langs=OCR_LANGS):
        self.hashes = load_hashes(hash_json)
        self.reader = easyocr.Reader(ocr_langs)

    # 1) detect champ icon boxes (row anchors)
    def detect_icon_boxes(self, img):
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        _, thr = cv2.threshold(gray, 200, 255, cv2.THRESH_BINARY_INV)
        cnts, _ = cv2.findContours(
            thr, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        boxes = [cv2.boundingRect(c) for c in cnts]
        champs = [b for b in boxes if 180 < b[2] <
                  260 and 80 < b[3] < 120 and b[0] < 60]
        champs = sorted(champs, key=lambda b: b[1])  # by y

        if len(champs) < 10:
            # fallback — works for your examples
            guess_tops = [63, 170, 278, 385, 493, 832, 936, 1039, 1142, 1245]
            champs = [(4, y, ICON_W, ICON_H) for y in guess_tops][:10]
        return champs[:10]

    # 2) detect column x from header labels
    def detect_header_columns(self, img):
        h, w, _ = img.shape
        header_roi = img[0:120, 0:w]  # top band
        ocr = self.reader.readtext(header_roi, detail=1, paragraph=False)
        # map header tokens to approximate x
        tokens = []
        for (box, txt, conf) in ocr:
            x = min(p[0] for p in box)
            y = min(p[1] for p in box)
            if y > 80:  # ignore below header stripe
                continue
            tokens.append((x, txt.strip()))

        # Normalize “Objective Time” into one column start
        col_names = {
            "Player": None, "Credits": None, "K/D/A": None,
            "Damage": None, "Taken": None, "Objective Time": None,
            "Shielding": None, "Healing": None
        }

        # crude matches that survive minor OCR variants
        def find_x(name, alt=None):
            for x, t in tokens:
                if re.fullmatch(name, t, flags=re.I) or (alt and re.fullmatch(alt, t, flags=re.I)):
                    return x
            return None

        x_player = find_x(r"Player")
        x_credits = find_x(r"Credits?")
        x_kda = find_x(r"(K/?D/?A|K/D/A)")
        x_damage = find_x(r"Damage")
        x_taken = find_x(r"Taken")
        x_obj = find_x(r"Objective")
        x_time = find_x(r"Time")
        x_shield = find_x(r"Shielding?")
        x_heal = find_x(r"Healing?")

        # merge objective/time
        x_obj_time = None
        if x_obj is not None and x_time is not None:
            x_obj_time = min(x_obj, x_time)
        elif x_obj is not None:
            x_obj_time = x_obj
        elif x_time is not None:
            x_obj_time = x_time

        # If any missing, fall back to safe defaults derived from image width
        # left boundary is right of champ icon
        left_after_icon = 4 + ICON_W + 12
        defaults = {
            "Player": left_after_icon,
            "Credits": left_after_icon + 350,
            "K/D/A": left_after_icon + 520,
            "Damage": left_after_icon + 680,
            "Taken": left_after_icon + 860,
            "Objective Time": left_after_icon + 1010,
            "Shielding": left_after_icon + 1180,
            "Healing": left_after_icon + 1360,
        }

        columns = {
            "Player": x_player if x_player is not None else defaults["Player"],
            "Credits": x_credits if x_credits is not None else defaults["Credits"],
            "K/D/A": x_kda if x_kda is not None else defaults["K/D/A"],
            "Damage": x_damage if x_damage is not None else defaults["Damage"],
            "Taken": x_taken if x_taken is not None else defaults["Taken"],
            "Objective Time": x_obj_time if x_obj_time is not None else defaults["Objective Time"],
            "Shielding": x_shield if x_shield is not None else defaults["Shielding"],
            "Healing": x_heal if x_heal is not None else defaults["Healing"],
        }

        # turn x-starts into [x1,x2) windows by midpoints
        keys = ["Player", "Credits", "K/D/A", "Damage",
                "Taken", "Objective Time", "Shielding", "Healing"]
        xs = [columns[k] for k in keys]
        xs_sorted = sorted([(x, k)
                           for k, x in columns.items()], key=lambda t: t[0])

        bounds = {}
        for i, (x, k) in enumerate(xs_sorted):
            x1 = x
            x2 = w if i == len(xs_sorted) - \
                1 else int((x + xs_sorted[i+1][0]) / 2)
            bounds[k] = (x1, x2)
        return bounds

    def match_champion(self, icon_bgr):
        rgb = cv2.cvtColor(icon_bgr, cv2.COLOR_BGR2RGB)
        h = imagehash.phash(Image.fromarray(rgb))
        best_key, best_d = None, 10**9
        for k, hx in self.hashes.items():
            d = phash_distance(h, hx)
            if d < best_d:
                best_key, best_d = k, d
        # distance cutoff (tune if needed)
        return best_key if best_d <= 20 else "Unknown"

    # 3) OCR cells inside fixed boxes
    def read_cell(self, img, x1, y1, x2, y2, numeric=False):
        roi = img[max(0, y1):y2, max(0, x1):x2]
        if roi.size == 0:
            return ""
        res = self.reader.readtext(roi, detail=1, paragraph=False)
        blocks = []
        for (box, txt, conf) in res:
            x = min(p[0] for p in box) + x1
            blocks.append((x, txt))
        return pick_best_text(blocks, numeric=numeric)

    def extract_meta(self, img):
        # middle band contains time/region/map/scores
        h, w, _ = img.shape
        mid = img[int(h*0.35):int(h*0.65), 0:w]
        ocr = self.reader.readtext(mid, detail=1, paragraph=False)
        texts = [t for (_, t, _) in ocr]
        joined = " ".join(texts)

        # time
        m = re.search(r"(\d+)\s*minutes", joined, flags=re.I)
        time_minutes = int(m.group(1)) if m else None
        # region
        region = None
        for r in KNOWN_REGIONS:
            if re.search(rf"\b{re.escape(r)}\b", joined):
                region = r
                break
        # map
        map_name = None
        for mname in sorted(KNOWN_MAPS, key=len, reverse=True):
            if re.search(rf"\b{re.escape(mname)}\b", joined):
                map_name = mname
                break
        # scores
        t1 = re.search(r"Team\s*1\s*Score\s*:\s*(\d+)", joined, flags=re.I)
        t2 = re.search(r"Team\s*2\s*Score\s*:\s*(\d+)", joined, flags=re.I)
        team1 = int(t1.group(1)) if t1 else None
        team2 = int(t2.group(1)) if t2 else None
        return time_minutes, region, map_name, team1, team2

    def parse(self, img_path=SCOREBOARD_IMG):
        img = cv2.imread(img_path)
        if img is None:
            raise FileNotFoundError(img_path)

        # anchors
        icon_boxes = self.detect_icon_boxes(img)            # (x,y,w,h) *10
        col_bounds = self.detect_header_columns(img)        # {col: (x1,x2)}

        # champion names (by hash)
        champs = []
        for (x, y, w, h) in icon_boxes:
            champs.append(self.match_champion(img[y:y+h, x:x+w]))

        # metadata
        time_m, region, map_name, t1, t2 = self.extract_meta(img)

        data = {
            "match": {
                "time_minutes": time_m or 0,
                "region": region or "Unknown",
                "map": map_name or "Unknown",
                "team1_score": t1 or 0,
                "team2_score": t2 or 0
            },
            "teams": {"team1": [], "team2": []}
        }

        # per-row cell OCR
        # player column starts AFTER icon; clamp x1 to icon right edge
        for i, (x, y, w, h) in enumerate(icon_boxes):
            y1, y2 = y, y+h

            def box_for(col):
                cx1, cx2 = col_bounds[col]
                # player col must start right of icon
                if col == "Player":
                    cx1 = max(cx1, x + w + 6)
                return cx1, y1, cx2, y2

            # read cells
            player_raw = self.read_cell(
                img, *box_for("Player"),  numeric=False)
            credits_raw = self.read_cell(
                img, *box_for("Credits"), numeric=True)
            kda_raw = self.read_cell(img, *box_for("K/D/A"),  numeric=True)
            dmg_raw = self.read_cell(img, *box_for("Damage"), numeric=True)
            taken_raw = self.read_cell(img, *box_for("Taken"),  numeric=True)
            obj_raw = self.read_cell(
                img, *box_for("Objective Time"), numeric=True)
            sh_raw = self.read_cell(img, *box_for("Shielding"), numeric=True)
            heal_raw = self.read_cell(img, *box_for("Healing"),  numeric=True)

            # clean player: drop 'R P' and trailing account id number lines if they leaked
            # keep the first token that has letters
            parts = [p for p in re.split(r"\s+", player_raw.strip()) if p]
            if parts:
                # remove pure 'R' 'P' tokens and pure numbers
                cleaned = [p for p in parts if not re.fullmatch(
                    r"[RP]|[0-9]+", p)]
                player = " ".join(cleaned) or parts[0]
            else:
                player = "Unknown"

            k, d, a = parse_kda(kda_raw)
            entry = {
                "champion": champs[i] if i < len(champs) else "Unknown",
                "player": player,
                "credits": to_int(credits_raw),
                "kills": k, "deaths": d, "assists": a,
                "damage": to_int(dmg_raw),
                "taken": to_int(taken_raw),
                "objective_time": to_int(obj_raw),
                "shielding": to_int(sh_raw),
                "healing": to_int(heal_raw)
            }

            (data["teams"]["team1"] if i <
             5 else data["teams"]["team2"]).append(entry)

        # warnings only if missing/defaults
        for k, v in data["match"].items():
            if v in (None, 0, "Unknown"):
                print(f"⚠️ Missing or default value for: {k}")

        return data


# ---------------- run ----------------
if __name__ == "__main__":
    sb = Scoreboard(HASH_JSON)
    result = sb.parse(SCOREBOARD_IMG)
    out = Path("parsed_scoreboard.json")
    with open(out, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"✅ Scoreboard exported to {out.name}")
