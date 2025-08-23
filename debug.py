import cv2
import json

# -------------------------------
# Config
# -------------------------------
IMG_PATH = "TeamMatch.png"
JSON_PATH = "match_data.json"

# Player stat boxes (x1, y1, x2, y2) relative to row start
ADJUSTED_BOXES = {
    "player":   (140, 0, 460, 70),
    "credits":  (620, 0, 790, 100),
    "KDA":      (795, 0, 1010, 100),
    "damage":   (1020, 0, 1260, 100),
    "taken":    (1270, 0, 1500, 100),
    "objective_time": (1510, 0, 1650, 100),
    "shielding": (1660, 0, 1910, 100),
    "healing":   (1920, 0, 2140, 100)
}

# -------------------------------
# Player row spacing
# -------------------------------
ROW_HEIGHT = 120     # row height
START_Y = 60         # starting Y offset for Team 1
NUM_ROWS = 5         # rows per team
TEAM_GAP = 260       # vertical gap between Team 1 and Team 2
X_SHIFT = 120        # global horizontal shift
Y_SHIFT = 0          # global vertical shift

# -------------------------------
# Match-level boxes (independent config)
# -------------------------------
MATCH_BOXES = {
    "duration": (150, 720, 400, 770),     # "12 minutes"
    "region":   (150, 770, 480, 820),     # "North America"
    "map":      (150, 820, 530, 880),     # "Splitstone Quarry"
    "team1_score": (700, 670, 1100, 730),  # Team 1 Score
    "team2_score": (690, 830, 1100, 880)  # Team 2 Score
}

MATCH_X_SHIFT = 370   # separate shift controls
MATCH_Y_SHIFT = 32

# -------------------------------
# Output
# -------------------------------
OUT_PATH = "debug_all_boxes.png"

# -------------------------------
# Draw boxes for debugging
# -------------------------------


def draw_boxes():
    with open(JSON_PATH, "r", encoding="utf-8") as f:
        truth = json.load(f)

    img = cv2.imread(IMG_PATH)

    # Colors for clarity per column
    colors = {
        "player": (0, 255, 0),
        "credits": (255, 0, 0),
        "KDA": (0, 0, 255),
        "damage": (255, 255, 0),
        "taken": (255, 0, 255),
        "objective_time": (0, 255, 255),
        "shielding": (128, 0, 255),
        "healing": (0, 128, 255),

        "duration": (255, 128, 0),
        "region":   (128, 255, 0),
        "map":      (0, 128, 255),
        "team1_score": (255, 0, 128),
        "vs":          (200, 200, 200),
        "team2_score": (0, 255, 128)
    }

    # --- Draw Team 1 ---
    for i in range(NUM_ROWS):
        y_offset = START_Y + i * ROW_HEIGHT
        for key, (x1, y1, x2, y2) in ADJUSTED_BOXES.items():
            box = (x1 + X_SHIFT,
                   y_offset + y1 + Y_SHIFT,
                   x2 + X_SHIFT,
                   y_offset + y2 + Y_SHIFT)
            cv2.rectangle(img, (box[0], box[1]), (box[2], box[3]),
                          colors.get(key, (255, 255, 255)), 2)
            cv2.putText(img, key, (box[0], box[1]-5),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.5,
                        colors.get(key, (255, 255, 255)), 1)

    # --- Draw Team 2 ---
    team2_start_y = START_Y + NUM_ROWS * ROW_HEIGHT + TEAM_GAP
    for i in range(NUM_ROWS):
        y_offset = team2_start_y + i * ROW_HEIGHT
        for key, (x1, y1, x2, y2) in ADJUSTED_BOXES.items():
            box = (x1 + X_SHIFT,
                   y_offset + y1 + Y_SHIFT,
                   x2 + X_SHIFT,
                   y_offset + y2 + Y_SHIFT)
            cv2.rectangle(img, (box[0], box[1]), (box[2], box[3]),
                          colors.get(key, (255, 255, 255)), 2)
            cv2.putText(img, key, (box[0], box[1]-5),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.5,
                        colors.get(key, (255, 255, 255)), 1)

    # --- Draw Match Data ---
    for key, (x1, y1, x2, y2) in MATCH_BOXES.items():
        box = (x1 + MATCH_X_SHIFT,
               y1 + MATCH_Y_SHIFT,
               x2 + MATCH_X_SHIFT,
               y2 + MATCH_Y_SHIFT)
        cv2.rectangle(img, (box[0], box[1]), (box[2], box[3]),
                      colors.get(key, (200, 200, 200)), 2)
        cv2.putText(img, key, (box[0], box[1]-5),
                    cv2.FONT_HERSHEY_SIMPLEX, 0.5,
                    colors.get(key, (200, 200, 200)), 1)

    cv2.imwrite(OUT_PATH, img)
    print(
        f"Debug image with Team 1, Team 2, and Match data boxes saved to {OUT_PATH}")


if __name__ == "__main__":
    draw_boxes()
