# core/constants.py

CHAMPION_ROLES = {
    # Damage
    "Bomb King": "Damage", "Cassie": "Damage", "Dredge": "Damage", "Drogoz": "Damage",
    "Imani": "Damage", "Kinessa": "Damage", "Lian": "Damage", "Octavia": "Damage",
    "Saati": "Damage", "Sha Lin": "Damage", "Strix": "Damage", "Tiberius": "Damage",
    "Tyra": "Damage", "Viktor": "Damage", "Willo": "Damage", "Betty la Bomba": "Damage",
    "Omen": "Damage", "Vivian": "Damage",
    # Flank
    "Androxus": "Flank", "Buck": "Flank", "Caspian": "Flank", "Evie": "Flank",
    "Koga": "Flank", "Lex": "Flank", "Maeve": "Flank",
    "Skye": "Flank", "Talus": "Flank", "Vatu": "Flank", "Vora": "Flank",
    "VII": "Flank", "Zhin": "Flank",
    # Tank
    "Ash": "Tank", "Atlas": "Tank", "Azaan": "Tank", "Barik": "Tank", "Fernando": "Tank",
    "Inara": "Tank", "Khan": "Tank", "Makoa": "Tank", "Raum": "Tank", "Ruckus": "Tank",
    "Terminus": "Tank", "Torvald": "Tank", "Yagorath": "Tank", "Nyx": "Tank", 
    # Support
    "Corvus": "Support", "Furia": "Support", "Grohk": "Support", "Grover": "Support",
    "Io": "Support", "Jenos": "Support", "Lillith": "Support", "Mal'Damba": "Support",
    "Moji": "Support", "Pip": "Support", "Rei": "Support", "Seris": "Support", "Ying": "Support",
}

POINT_TANKS = {"Barik", "Fernando", "Inara", "Nyx", "Terminus"}

CHAMPION_ALIASES = {
    "andy": "Androxus",
    "bk": "Bomb King",
    "bomb": "Bomb King",
    "bombking": "Bomb King",
    "damba": "Mal'Damba",
    "lilith": "Lillith",
    "mal damba": "Mal'Damba",
    "maldamba": "Mal'Damba",
    "nando": "Fernando",
    "ruk": "Ruckus",
    "seven": "VII",
}

ROLE_ALIASES = {
    'dmg': 'Damage', 'damage': 'Damage',
    'sup': 'Support', 'supp': 'Support', 'suppo': 'Support', 'support': 'Support',
    'tank': 'Tank', 'frontline': 'Tank',
    'point': 'Point Tank', 'pointtank': 'Point Tank', 'point tank': 'Point Tank', 'pt': 'Point Tank',
    'main tank': 'Point Tank', 'maintank': 'Point Tank', 'mt': 'Point Tank',
    'off': 'Off Tank', 'offtank': 'Off Tank', 'off tank': 'Off Tank', 'ot': 'Off Tank',
    'flank': 'Flank',
}


def _normalize_lookup(value):
    return " ".join(str(value).lower().replace("'", "").split())


def _compact_lookup(value):
    return _normalize_lookup(value).replace(" ", "")


def _champion_lookup_keys(champion):
    normalized = _normalize_lookup(champion)
    compact = normalized.replace(" ", "")
    return normalized, compact


def _unique_champion_matches(predicate):
    matches = []
    for champion in CHAMPION_ROLES:
        if predicate(champion) and champion not in matches:
            matches.append(champion)
    return matches


def resolve_role_name(name):
    normalized = _normalize_lookup(name)
    compact = normalized.replace(" ", "")

    if normalized in ROLE_ALIASES:
        return ROLE_ALIASES[normalized]
    if compact in ROLE_ALIASES:
        return ROLE_ALIASES[compact]

    roles = {"Damage", "Flank", "Tank", "Support", "Point Tank", "Off Tank"}
    for role in roles:
        role_key = role.lower()
        if normalized == role_key or compact == role_key.replace(" ", ""):
            return role

    return None


def get_champions_for_role(role):
    role = resolve_role_name(role) or role
    if role == "Point Tank":
        return [champ for champ in CHAMPION_ROLES if champ in POINT_TANKS]
    if role == "Off Tank":
        return [champ for champ, champ_role in CHAMPION_ROLES.items() if champ_role == "Tank" and champ not in POINT_TANKS]
    return [champ for champ, champ_role in CHAMPION_ROLES.items() if champ_role == role]


def resolve_champion_name(name):
    normalized = _normalize_lookup(name)
    compact = normalized.replace(" ", "")
    if not normalized:
        return None

    if normalized in CHAMPION_ALIASES:
        return CHAMPION_ALIASES[normalized]
    if compact in CHAMPION_ALIASES:
        return CHAMPION_ALIASES[compact]

    for champion in CHAMPION_ROLES:
        champion_key, champion_compact = _champion_lookup_keys(champion)
        if normalized == champion_key or compact == champion_compact:
            return champion

    prefix_matches = _unique_champion_matches(
        lambda champion: any(
            key.startswith(needle)
            for key in _champion_lookup_keys(champion)
            for needle in (normalized, compact)
            if needle
        )
    )
    if len(prefix_matches) == 1:
        return prefix_matches[0]

    contains_matches = _unique_champion_matches(
        lambda champion: any(
            needle in key
            for key in _champion_lookup_keys(champion)
            for needle in (normalized, compact)
            if needle
        )
    )
    if len(contains_matches) == 1:
        return contains_matches[0]

    return None

ALLOWED_CHANNELS = ["match-results", "boss-matchresults", "admin"]
