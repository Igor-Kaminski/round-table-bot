import re
import time
from datetime import datetime, timedelta

from discord.ext import commands

from core.constants import resolve_champion_name
from db import resolve_map_name
from utils.converters import PlayerConverter, resolve_player_id


RESULT_FILTER_ALIASES = {
    "win": "wins", "wins": "wins", "won": "wins", "wonly": "wins",
    "winonly": "wins", "winsonly": "wins",
    "loss": "losses", "losses": "losses", "lost": "losses", "lonly": "losses",
    "lossonly": "losses", "lossesonly": "losses",
}

TEAM_FILTER_ALIASES = {
    "team1": 1, "t1": 1, "first": 1, "fp": 1, "firstpick": 1,
    "team2": 2, "t2": 2, "second": 2, "lastpick": 2, "lp": 2,
}

SCORE_FILTER_ALIASES = {
    "close": "close", "closegame": "close",
    "stomp": "stomp", "stomps": "stomp",
    "sweep": "sweep", "sweeps": "sweep",
}

TIME_FILTER_KEYWORDS = {
    "time", "last", "since", "after", "from", "between", "before", "until", "season",
}

MISSING_SEASON_MESSAGE = "Executive team regrets to inform you that our accounting team from previous quarters has lost these records."

SEASON_FILTERS = {
    "2": {
        "before": "2025-10-04",
        "label": "Season 2",
    },
    "3": {
        "after": "2025-10-04",
        "before": "2026-06-12",
        "label": "Season 3",
    },
    "3.5": {
        "after": "2026-04-01",
        "before": "2026-06-12",
        "label": "Season 3.5",
    },
    "4": {
        "after": "2026-06-12",
        "label": "Season 4",
    },
}

FILTER_KEYWORDS = {
    "map", "talent", "tal", "champ", "champs", "champion", "champions",
    "withchamp", "withchamps", "ally", "allies", "allychamp", "allychamps",
    "notchamp", "notchamps", "exclude", "without",
    "notwithchamp", "notwithchamps", "notally", "notallies", "noally", "noallies",
    "with", "against", "vs", "versus", "notvs", "notversus",
    *TIME_FILTER_KEYWORDS,
    *RESULT_FILTER_ALIASES.keys(),
    *TEAM_FILTER_ALIASES.keys(),
    *SCORE_FILTER_ALIASES.keys(),
}


def compact_arg(arg):
    return str(arg).lower().replace("_", "").replace("-", "").replace("'", "")


def parse_scoreline(arg):
    if "-" not in arg:
        return None
    left, right = arg.split("-", 1)
    if left.isdigit() and right.isdigit():
        return int(left), int(right)
    return None


def _is_filter_boundary(arg):
    raw_key = str(arg).lower()
    key = compact_arg(arg)
    return (
        raw_key == "-m"
        or raw_key == "-not"
        or raw_key.startswith("-")
        or key in FILTER_KEYWORDS
        or season_key(arg) is not None
        or _parse_period_token(arg) is not None
        or parse_scoreline(raw_key) is not None
    )


def _parse_period_token(arg):
    key = compact_arg(arg)
    match = re.fullmatch(r"(\d+)(d|day|days|w|week|weeks|h|hour|hours)", key)
    if not match:
        return None

    amount = int(match.group(1))
    unit = match.group(2)
    if amount <= 0:
        return None

    if unit.startswith("h"):
        seconds = amount * 60 * 60
        label_unit = "hour" if amount == 1 else "hours"
    elif unit.startswith("w"):
        seconds = amount * 7 * 24 * 60 * 60
        label_unit = "week" if amount == 1 else "weeks"
    else:
        seconds = amount * 24 * 60 * 60
        label_unit = "day" if amount == 1 else "days"
    return seconds, f"last {amount} {label_unit}"


def _parse_split_period(args, index):
    if index >= len(args):
        return None
    parsed = _parse_period_token(args[index])
    if parsed:
        return parsed, index + 1
    if index + 1 < len(args) and str(args[index]).isdigit():
        parsed = _parse_period_token(f"{args[index]}{args[index + 1]}")
        if parsed:
            return parsed, index + 2
    return None


def _parse_date_start(arg):
    value = str(arg).strip()
    for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
        try:
            return int(datetime.strptime(value, fmt).timestamp())
        except ValueError:
            continue
    return None


def _parse_date_end(arg):
    value = str(arg).strip()
    for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
        try:
            end_of_day = datetime.strptime(value, fmt) + timedelta(days=1)
            return int(end_of_day.timestamp())
        except ValueError:
            continue
    return None


def _parse_champion_list(args, start_index):
    champions = []
    index = start_index

    while index < len(args) and not _is_filter_boundary(args[index]):
        resolved_champion = None
        consumed_until = index
        for end in range(len(args), index, -1):
            candidate = " ".join(str(part) for part in args[index:end])
            resolved_champion = resolve_champion_name(candidate)
            if resolved_champion:
                consumed_until = end
                break
        if not resolved_champion:
            break
        if resolved_champion not in champions:
            champions.append(resolved_champion)
        index = consumed_until

    return champions, index


def _date_filter_error(arg):
    return f"`{arg}` is not a valid date. Use `YYYY-MM-DD`, for example `2026-05-25`."


def season_key(value):
    compact = compact_arg(value).replace(".", "").replace(" ", "")
    if compact.startswith("season"):
        compact = compact[len("season"):]
    elif compact.startswith("s"):
        compact = compact[1:]
    if compact == "35":
        return "3.5"
    if compact in {"1", "2", "3", "4"}:
        return compact
    return None


def _apply_season_filter(filters, season):
    if season == "1":
        return MISSING_SEASON_MESSAGE

    season_filter = SEASON_FILTERS.get(season)
    if not season_filter:
        return "Unknown season. Use `season 4`, `season 3.5`, `season 3`, or `season 2`."

    if season_filter.get("after"):
        filters["registered_after"] = _parse_date_start(season_filter["after"])
    else:
        filters.pop("registered_after", None)

    if season_filter.get("before"):
        filters["registered_before"] = _parse_date_start(season_filter["before"])
    else:
        filters.pop("registered_before", None)

    filters["time_label"] = season_filter["label"]
    return None


def _set_last_registered_filter(filters, seconds, label):
    filters["registered_after"] = int(time.time()) - seconds
    filters.pop("registered_before", None)
    filters["time_label"] = f"Recorded {label}"


def filter_summary(filters):
    if not filters:
        return []

    labels = []
    if filters.get("time_label"):
        labels.append(filters["time_label"])
    if filters.get("map"):
        labels.append(f"Map: {filters['map']}")
    if filters.get("talent"):
        labels.append(f"Talent: {filters['talent']}")
    if filters.get("include_champions"):
        labels.append("Champs: " + ", ".join(filters["include_champions"]))
    if filters.get("exclude_champions"):
        labels.append("Not Champs: " + ", ".join(filters["exclude_champions"]))
    for champ in filters.get("with_champions", []):
        labels.append(f"With Champ {champ}")
    for champ in filters.get("not_with_champions", []):
        labels.append(f"Not With Champ {champ}")
    if filters.get("result") == "wins":
        labels.append("Wins Only")
    elif filters.get("result") == "losses":
        labels.append("Losses Only")
    if filters.get("team") == 1:
        labels.append("Team 1")
    elif filters.get("team") == 2:
        labels.append("Team 2")
    if filters.get("scoreline"):
        labels.append(f"Scoreline {filters['scoreline'][0]}-{filters['scoreline'][1]}")
    elif filters.get("score_category") == "close":
        labels.append("Close Games")
    elif filters.get("score_category") == "stomp":
        labels.append("Stomps")
    elif filters.get("score_category") == "sweep":
        labels.append("Sweeps")
    for champ in filters.get("vs_champions", []):
        labels.append(f"Vs {champ}")
    for champ in filters.get("not_vs_champions", []):
        labels.append(f"Not Vs {champ}")
    if filters.get("with_player_name"):
        labels.append(f"With {filters['with_player_name']}")
    if filters.get("against_player_name"):
        labels.append(f"Against {filters['against_player_name']}")
    return labels


def title_filter_suffix(filters):
    parts = []
    if filters.get("time_label"):
        parts.append(f"({filters['time_label']})")
    if filters.get("map"):
        parts.append(f"on {filters['map']}")
    if filters.get("talent"):
        parts.append(f"with {filters['talent']}")
    if filters.get("include_champions"):
        parts.append("on " + "/".join(filters["include_champions"]))
    if filters.get("exclude_champions"):
        parts.append("without " + "/".join(filters["exclude_champions"]))
    for champ in filters.get("with_champions", []):
        parts.append(f"with {champ}")
    for champ in filters.get("not_with_champions", []):
        parts.append(f"not with {champ}")
    if filters.get("result") == "wins":
        parts.append("in Wins")
    elif filters.get("result") == "losses":
        parts.append("in Losses")
    if filters.get("team") == 1:
        parts.append("on Team 1")
    elif filters.get("team") == 2:
        parts.append("on Team 2")
    if filters.get("scoreline"):
        parts.append(f"at {filters['scoreline'][0]}-{filters['scoreline'][1]}")
    elif filters.get("score_category") == "close":
        parts.append("in Close Games")
    elif filters.get("score_category") == "stomp":
        parts.append("in Stomps")
    elif filters.get("score_category") == "sweep":
        parts.append("in Sweeps")
    for champ in filters.get("vs_champions", []):
        parts.append(f"vs {champ}")
    for champ in filters.get("not_vs_champions", []):
        parts.append(f"not vs {champ}")
    if filters.get("with_player_name"):
        parts.append(f"with {filters['with_player_name']}")
    if filters.get("against_player_name"):
        parts.append(f"against {filters['against_player_name']}")
    return " " + " ".join(parts) if parts else ""


def slash_filter_args(
    *,
    time_range=None,
    since=None,
    until=None,
    map_name=None,
    talent=None,
    result=None,
    team=None,
    score=None,
    with_player=None,
    against_player=None,
):
    args = []
    if time_range:
        if season_key(time_range):
            args.extend(split_words(time_range))
        else:
            args.extend(["last", time_range])
    elif since and until:
        args.extend(["from", since, "to", until])
    elif since:
        args.extend(["since", since])
    elif until:
        args.extend(["until", until])

    if map_name:
        args.extend(["map", map_name])
    if talent:
        args.extend(["talent", talent])
    if result:
        args.append(result)
    if team:
        args.append(team)
    if score:
        args.append(score)
    if with_player:
        args.extend(["with", str(with_player.id)])
    if against_player:
        args.extend(["against", str(against_player.id)])
    return args


def split_words(value):
    return str(value).split() if value else []


def stat_flag(value):
    if not value:
        return None
    value = value.strip().lower()
    return value if value.startswith("-") else f"-{value}"


async def extract_match_filters(ctx, args):
    args = list(args)
    filters = {}
    remaining = []
    i = 0

    while i < len(args):
        arg = str(args[i])
        raw_key = arg.lower()
        key = compact_arg(arg)

        if raw_key == "-m":
            remaining.append(arg)
            if i + 1 < len(args) and str(args[i + 1]).isdigit():
                remaining.append(str(args[i + 1]))
                i += 2
            else:
                i += 1
            continue

        inline_min_games = re.fullmatch(r"-m(\d+)", raw_key)
        if inline_min_games:
            remaining.extend(["-m", inline_min_games.group(1)])
            i += 1
            continue

        season = season_key(arg)
        consumed_until = i + 1
        if key == "season":
            if i + 1 >= len(args):
                return remaining, filters, "Use `season 4`, `season 3.5`, `season 3`, or `season 2`."
            season = season_key(f"season {args[i + 1]}")
            consumed_until = i + 2
        if season:
            season_error = _apply_season_filter(filters, season)
            if season_error:
                return remaining, filters, season_error
            i = consumed_until
            continue

        standalone_period = _parse_period_token(arg)
        if standalone_period:
            seconds, label = standalone_period
            _set_last_registered_filter(filters, seconds, label)
            i += 1
            continue

        if key in {"time", "last"}:
            period = _parse_split_period(args, i + 1)
            if not period:
                return remaining, filters, f"`{arg}` needs a period like `3d`, `7d`, `14d`, or `30d`."
            (seconds, label), consumed_until = period
            _set_last_registered_filter(filters, seconds, label)
            i = consumed_until
            continue

        if key in {"since", "after"}:
            if i + 1 >= len(args):
                return remaining, filters, f"`{arg}` needs a date like `2026-05-25` or a period like `7d`."
            period = _parse_split_period(args, i + 1)
            if period:
                (seconds, label), consumed_until = period
                _set_last_registered_filter(filters, seconds, label)
                i = consumed_until
                continue
            start_ts = _parse_date_start(args[i + 1])
            if start_ts is None:
                return remaining, filters, _date_filter_error(args[i + 1])
            filters["registered_after"] = start_ts
            filters.pop("registered_before", None)
            filters["time_label"] = f"Recorded since {args[i + 1]}"
            i += 2
            continue

        if key in {"before", "until"}:
            if i + 1 >= len(args):
                return remaining, filters, f"`{arg}` needs a date like `2026-05-25`."
            end_ts = _parse_date_end(args[i + 1])
            if end_ts is None:
                return remaining, filters, _date_filter_error(args[i + 1])
            filters["registered_before"] = end_ts
            if filters.get("time_label", "").startswith("Recorded since "):
                filters["time_label"] = f"{filters['time_label']} until {args[i + 1]}"
            else:
                filters["time_label"] = f"Recorded before {args[i + 1]}"
            i += 2
            continue

        if key in {"from", "between"}:
            if i + 2 >= len(args):
                return remaining, filters, f"`{arg}` needs a start and end date, like `from 2026-05-01 to 2026-05-25`."
            start_arg = args[i + 1]
            end_index = i + 2
            if compact_arg(args[end_index]) in {"to", "and"} or str(args[end_index]) == "-":
                end_index += 1
            if end_index >= len(args):
                return remaining, filters, f"`{arg}` needs an end date."
            end_arg = args[end_index]
            start_ts = _parse_date_start(start_arg)
            end_ts = _parse_date_end(end_arg)
            if start_ts is None:
                return remaining, filters, _date_filter_error(start_arg)
            if end_ts is None:
                return remaining, filters, _date_filter_error(end_arg)
            if start_ts >= end_ts:
                return remaining, filters, "`from` date must be before the `to` date."
            filters["registered_after"] = start_ts
            filters["registered_before"] = end_ts
            filters["time_label"] = f"Recorded {start_arg} to {end_arg}"
            i = end_index + 1
            continue

        if key == "map":
            resolved_map = None
            consumed_until = i + 1
            for end in range(len(args), i + 1, -1):
                candidate = " ".join(str(part) for part in args[i + 1:end])
                resolved_map = resolve_map_name(candidate)
                if resolved_map:
                    consumed_until = end
                    break
            if not resolved_map:
                return remaining, filters, f"Could not find a map matching `{arg if i + 1 >= len(args) else args[i + 1]}`."
            filters["map"] = resolved_map
            i = consumed_until
            continue

        if key in {"talent", "tal"}:
            talent_start = i + 1
            if talent_start >= len(args):
                return remaining, filters, f"`{arg}` needs a talent after it, like `talent scorch`."
            consumed_until = talent_start
            talent_parts = []
            while consumed_until < len(args) and not _is_filter_boundary(args[consumed_until]):
                talent_parts.append(str(args[consumed_until]))
                consumed_until += 1
            if not talent_parts:
                return remaining, filters, f"`{arg}` needs a talent after it, like `talent scorch`."
            filters["talent"] = " ".join(talent_parts).strip()
            i = consumed_until
            continue

        if key == "only":
            i += 1
            continue

        if key in RESULT_FILTER_ALIASES:
            filters["result"] = RESULT_FILTER_ALIASES[key]
            i += 1
            continue

        if key == "team" and i + 1 < len(args) and compact_arg(args[i + 1]) in {"1", "2"}:
            filters["team"] = int(compact_arg(args[i + 1]))
            i += 2
            continue

        if key in {"first", "last"} and i + 1 < len(args) and compact_arg(args[i + 1]) == "pick":
            filters["team"] = 1 if key == "first" else 2
            i += 2
            continue

        if key in TEAM_FILTER_ALIASES:
            filters["team"] = TEAM_FILTER_ALIASES[key]
            i += 1
            continue

        if key in SCORE_FILTER_ALIASES:
            filters["score_category"] = SCORE_FILTER_ALIASES[key]
            filters.pop("scoreline", None)
            i += 1
            continue

        scoreline = parse_scoreline(raw_key)
        if scoreline:
            filters["scoreline"] = scoreline
            filters.pop("score_category", None)
            i += 1
            continue

        if key in {"vs", "versus", "notvs", "notversus"} or (key == "not" and i + 1 < len(args) and compact_arg(args[i + 1]) in {"vs", "versus"}):
            negated_vs = key in {"notvs", "notversus"} or key == "not"
            champion_start = i + 2 if key == "not" else i + 1
            if champion_start >= len(args):
                return remaining, filters, f"`{arg}` needs a champion after it."
            resolved_champion = None
            consumed_until = champion_start
            for end in range(len(args), champion_start, -1):
                candidate = " ".join(str(part) for part in args[champion_start:end])
                resolved_champion = resolve_champion_name(candidate)
                if resolved_champion:
                    consumed_until = end
                    break
            if not resolved_champion:
                return remaining, filters, f"Could not find a champion matching `{args[champion_start]}`."
            filter_key = "not_vs_champions" if negated_vs else "vs_champions"
            filters.setdefault(filter_key, [])
            if resolved_champion not in filters[filter_key]:
                filters[filter_key].append(resolved_champion)
            i = consumed_until
            continue

        if key in {"champ", "champs", "champion", "champions"}:
            champion_start = i + 1
            if champion_start >= len(args):
                return remaining, filters, f"`{arg}` needs at least one champion after it."
            champions, consumed_until = _parse_champion_list(args, champion_start)
            if not champions:
                return remaining, filters, f"Could not find a champion matching `{args[champion_start]}`."
            filters.setdefault("include_champions", [])
            for champion in champions:
                if champion not in filters["include_champions"]:
                    filters["include_champions"].append(champion)
            i = consumed_until
            continue

        if key in {"withchamp", "withchamps", "ally", "allies", "allychamp", "allychamps"}:
            champion_start = i + 1
            if champion_start >= len(args):
                return remaining, filters, f"`{arg}` needs at least one champion after it."
            champions, consumed_until = _parse_champion_list(args, champion_start)
            if not champions:
                return remaining, filters, f"Could not find a champion matching `{args[champion_start]}`."
            filters.setdefault("with_champions", [])
            for champion in champions:
                if champion not in filters["with_champions"]:
                    filters["with_champions"].append(champion)
            i = consumed_until
            continue

        if key in {"notwithchamp", "notwithchamps", "notally", "notallies", "noally", "noallies"}:
            champion_start = i + 1
            if champion_start >= len(args):
                return remaining, filters, f"`{arg}` needs at least one champion after it."
            champions, consumed_until = _parse_champion_list(args, champion_start)
            if not champions:
                return remaining, filters, f"Could not find a champion matching `{args[champion_start]}`."
            filters.setdefault("not_with_champions", [])
            for champion in champions:
                if champion not in filters["not_with_champions"]:
                    filters["not_with_champions"].append(champion)
            i = consumed_until
            continue

        if raw_key == "-not" or key in {"notchamp", "notchamps", "exclude", "without"} or (
            key == "not"
            and (i + 1 >= len(args) or compact_arg(args[i + 1]) not in {"vs", "versus"})
        ):
            champion_start = i + 1
            if champion_start >= len(args):
                return remaining, filters, f"`{arg}` needs at least one champion after it."
            champions, consumed_until = _parse_champion_list(args, champion_start)
            if not champions:
                return remaining, filters, f"Could not find a champion matching `{args[champion_start]}`."
            filters.setdefault("exclude_champions", [])
            for champion in champions:
                if champion not in filters["exclude_champions"]:
                    filters["exclude_champions"].append(champion)
            i = consumed_until
            continue

        if key in {"with", "against"}:
            if i + 1 >= len(args):
                return remaining, filters, f"`{arg}` needs a player after it."
            player_arg = str(args[i + 1])
            try:
                player = await PlayerConverter().convert(ctx, player_arg)
            except commands.BadArgument as exc:
                if resolve_champion_name(player_arg):
                    return remaining, filters, f"`with {player_arg}` means with a player. Use `withchamp {player_arg}` or `ally {player_arg}` for a champion teammate filter."
                return remaining, filters, str(exc)

            player_id = resolve_player_id(player)
            if not player_id:
                return remaining, filters, f"No stats found for `{player_arg}`."

            filters[f"{key}_player_id"] = player_id
            filters[f"{key}_player_name"] = player.display_name
            i += 2
            continue

        remaining.append(arg)
        i += 1

    return remaining, filters, None
