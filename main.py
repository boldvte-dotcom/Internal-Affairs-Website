from __future__ import annotations

import json
import random
import re
import sqlite3
import string
import time
from pathlib import Path
from typing import Any, Optional

import requests
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

BASE_DIR = Path(__file__).resolve().parent
INDEX_FILE = BASE_DIR / "index.html"
DB_FILE = BASE_DIR / "internal_affairs_clean.db"

ACTION_LOG_WEBHOOK = "https://discord.com/api/webhooks/1490475325874896956/SIqaZjwchopOJiVtJX7o4-AdUaxtmvCbODh-Pta_UpQLuRfN3hJzm1iHJPJR_lKs4okI"
REPORT_LOG_WEBHOOK = "https://discord.com/api/webhooks/1487913699468513290/rBQf5vyiDN2rdZvo1WQBiS_aDBKkT1vcRqqazExBZB2QHqfV-gaIQQ_7z3MLcFAr7MLv"
APPEAL_LOG_WEBHOOK = "https://discord.com/api/webhooks/1490475547925680248/efCrT5jds6-LsKFGQfWugvbS28YseOaG_HM1dhFTc3Uj9G5PGiV0b-WvAekPd4pihmLQ"
PERMISSION_ABUSE_WEBHOOK = "https://discord.com/api/webhooks/1490475547925680248/efCrT5jds6-LsKFGQfWugvbS28YseOaG_HM1dhFTc3Uj9G5PGiV0b-WvAekPd4pihmLQ"

ROBLOX_GROUP_ID = 36058174
ROBLOX_GROUP_URL = "https://www.roblox.com/communities/36058174/Internal-Affairs-SFPD#!/about"

ROBLOX_GROUP_RANK_TO_IA_ROLE = {
    1: "Low Rank",
    2: "Low Rank",
    3: "Low Rank",
    4: "Middle Rank",
    5: "Middle Rank",
    6: "High Rank",
    7: "Headquarters",
    251: "Headquarters",
    252: "Headquarters",
    253: "Joint Chiefs",
    254: "Ownership",
    255: "Ownership",
}

ROLE_LEVELS = {
    "Guest": 0,
    "Low Rank": 1,
    "Middle Rank": 4,
    "High Rank": 6,
    "Headquarters": 7,
    "Joint Chiefs": 253,
    "Ownership": 254,
}

ROLE_PERMISSIONS = {
    "Guest": [
        "report_access",
        "appeal_access",
        "contact_access",
        "ticket_status_access",
    ],
    "Low Rank": [
        "report_access",
        "appeal_access",
        "contact_access",
        "ticket_status_access",
        "view_low_rank_panel",
        "report_blacklist",
        "send_inbox_message",
        "view_tickets",
        "handle_tickets",
    ],
    "Middle Rank": [
        "report_access",
        "appeal_access",
        "contact_access",
        "ticket_status_access",
        "view_low_rank_panel",
        "report_blacklist",
        "send_inbox_message",
        "view_tickets",
        "handle_tickets",
        "view_middle_rank_panel",
        "view_logged_users",
        "force_logout",
        "give_permissions",
        "review_appeals",
    ],
    "High Rank": [
        "report_access",
        "appeal_access",
        "contact_access",
        "ticket_status_access",
        "view_low_rank_panel",
        "report_blacklist",
        "send_inbox_message",
        "view_tickets",
        "handle_tickets",
        "view_middle_rank_panel",
        "view_logged_users",
        "force_logout",
        "give_permissions",
        "review_appeals",
        "view_high_rank_panel",
        "remove_permissions",
        "terminate_user",
        "revoke_report_blacklist",
        "revoke_terminate",
        "view_permission_abuse_database",
        "unsuspend_permission_abuse",
        "bulk_force_logout",
    ],
    "Headquarters": [
        "report_access",
        "appeal_access",
        "contact_access",
        "ticket_status_access",
        "view_low_rank_panel",
        "report_blacklist",
        "send_inbox_message",
        "view_tickets",
        "handle_tickets",
        "view_middle_rank_panel",
        "view_logged_users",
        "force_logout",
        "give_permissions",
        "review_appeals",
        "view_high_rank_panel",
        "remove_permissions",
        "terminate_user",
        "revoke_report_blacklist",
        "revoke_terminate",
        "view_permission_abuse_database",
        "unsuspend_permission_abuse",
        "bulk_force_logout",
        "view_headquarters_panel",
        "check_information",
        "send_global_message",
    ],
    "Joint Chiefs": [
        "report_access",
        "appeal_access",
        "contact_access",
        "ticket_status_access",
        "view_low_rank_panel",
        "report_blacklist",
        "send_inbox_message",
        "view_tickets",
        "handle_tickets",
        "view_middle_rank_panel",
        "view_logged_users",
        "force_logout",
        "give_permissions",
        "review_appeals",
        "view_high_rank_panel",
        "remove_permissions",
        "terminate_user",
        "revoke_report_blacklist",
        "revoke_terminate",
        "view_permission_abuse_database",
        "unsuspend_permission_abuse",
        "bulk_force_logout",
        "view_headquarters_panel",
        "check_information",
        "send_global_message",
        "view_joint_chiefs_panel",
        "manage_website_shutdown",
    ],
    "Ownership": [
        "report_access",
        "appeal_access",
        "contact_access",
        "ticket_status_access",
        "view_low_rank_panel",
        "report_blacklist",
        "send_inbox_message",
        "view_tickets",
        "handle_tickets",
        "view_middle_rank_panel",
        "view_logged_users",
        "force_logout",
        "give_permissions",
        "review_appeals",
        "view_high_rank_panel",
        "remove_permissions",
        "terminate_user",
        "revoke_report_blacklist",
        "revoke_terminate",
        "view_permission_abuse_database",
        "unsuspend_permission_abuse",
        "bulk_force_logout",
        "view_headquarters_panel",
        "check_information",
        "send_global_message",
        "view_joint_chiefs_panel",
        "manage_website_shutdown",
        "view_ownership_panel",
        "manage_join_limit",
    ],
}

app = FastAPI(title="Internal Affairs Website")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn


def now_ts() -> int:
    return int(time.time())


def human_time(ts: Optional[int] = None) -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts or now_ts()))


def json_dump(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"))


def json_load(value: Optional[str]) -> Any:
    if not value:
        return []
    try:
        return json.loads(value)
    except Exception:
        return []


def make_session_key() -> str:
    return "".join(random.choices(string.ascii_letters + string.digits, k=48))


def make_custom_id() -> str:
    return "IA-" + "".join(random.choices(string.ascii_uppercase + string.digits, k=10))


def make_verification_code() -> str:
    return "IA-" + "".join(random.choices(string.ascii_uppercase + string.digits, k=8))


def make_visitor_key() -> str:
    return "VIS-" + "".join(random.choices(string.ascii_letters + string.digits, k=24))


def role_level(role_name: str) -> int:
    return ROLE_LEVELS.get(role_name, 0)


def role_permissions(role_name: str) -> list[str]:
    return ROLE_PERMISSIONS.get(role_name, ROLE_PERMISSIONS["Guest"])


def merged_permissions(row: sqlite3.Row) -> set[str]:
    return set(json_load(row["base_permissions"])) | set(json_load(row["extra_permissions"]))


def grantable_permissions_for(role_name: str) -> list[str]:
    actor_level = role_level(role_name)
    grantable = set()
    for name, level in ROLE_LEVELS.items():
        if level < actor_level:
            grantable |= set(role_permissions(name))
    blocked = {"manage_website_shutdown", "view_joint_chiefs_panel", "view_ownership_panel", "send_global_message", "manage_join_limit"}
    return sorted(grantable - blocked)


def is_active(until_ts: Optional[int]) -> bool:
    return until_ts is None or until_ts > now_ts()


def post_webhook(url: str, title: str, fields: list[dict[str, str]], footer: str = "") -> None:
    if not url or "PUT_" in url:
        return
    try:
        requests.post(
            url,
            json={
                "embeds": [{
                    "title": title,
                    "color": 0xB42020,
                    "fields": [{"name": x["name"], "value": x["value"], "inline": False} for x in fields],
                    "footer": {"text": footer} if footer else {},
                }]
            },
            timeout=8,
        )
    except Exception:
        pass


def log_action(actor: sqlite3.Row, action: str, against_custom_id: str = "N/A", against_username: str = "N/A") -> None:
    post_webhook(
        ACTION_LOG_WEBHOOK,
        "Internal Affairs Action Logged",
        [
            {"name": "Roblox Username", "value": actor["roblox_username"] or actor["username"]},
            {"name": "Custom ID", "value": actor["custom_id"]},
            {"name": "Date", "value": human_time()},
            {"name": "Action", "value": action},
            {"name": "Against Custom ID", "value": against_custom_id},
            {"name": "Against Username", "value": against_username},
        ],
        footer=actor["custom_id"],
    )


def add_event(recipient_custom_id: str, event_type: str, payload: dict[str, Any]) -> None:
    conn = db()
    conn.execute(
        "INSERT INTO client_events (recipient_custom_id, event_type, payload_json, created_at) VALUES (?, ?, ?, ?)",
        (recipient_custom_id, event_type, json_dump(payload), now_ts()),
    )
    conn.commit()
    conn.close()


def add_inbox_item(recipient_custom_id: str, title: str, message: str, kind: str = "system") -> None:
    conn = db()
    conn.execute(
        """
        INSERT INTO inbox_items (
            recipient_custom_id, title, message, kind, unread, created_at
        ) VALUES (?, ?, ?, ?, 1, ?)
        """,
        (recipient_custom_id, title, message, kind, now_ts()),
    )
    conn.commit()
    conn.close()


def find_target_session(target_ref: str) -> Optional[sqlite3.Row]:
    conn = db()
    row = conn.execute(
        """
        SELECT * FROM sessions
        WHERE custom_id = ? OR username = ? OR roblox_username = ?
        ORDER BY last_seen_at DESC
        LIMIT 1
        """,
        (target_ref, target_ref, target_ref),
    ).fetchone()
    conn.close()
    return row


def public_avatar_url(user_id: Optional[int]) -> str:
    if not user_id:
        return ""
    try:
        res = requests.get(
            "https://thumbnails.roblox.com/v1/users/avatar-headshot",
            params={"userIds": str(user_id), "size": "150x150", "format": "Png", "isCircular": "false"},
            timeout=8,
        )
        res.raise_for_status()
        data = res.json().get("data", [])
        if data and data[0].get("imageUrl"):
            return data[0]["imageUrl"]
    except Exception:
        pass
    return ""


def inbox_for(custom_id: str) -> dict[str, Any]:
    conn = db()
    rows = conn.execute(
        "SELECT * FROM inbox_items WHERE recipient_custom_id = ? ORDER BY created_at DESC",
        (custom_id,),
    ).fetchall()
    unread = conn.execute(
        "SELECT COUNT(*) AS c FROM inbox_items WHERE recipient_custom_id = ? AND unread = 1",
        (custom_id,),
    ).fetchone()["c"]
    conn.close()
    return {"items": [dict(x) for x in rows], "unread_count": unread}


def pending_events_for(custom_id: str) -> list[dict[str, Any]]:
    conn = db()
    rows = conn.execute(
        "SELECT * FROM client_events WHERE recipient_custom_id = ? AND consumed_at IS NULL ORDER BY created_at ASC",
        (custom_id,),
    ).fetchall()
    conn.close()
    return [{"id": x["id"], "event_type": x["event_type"], "payload": json.loads(x["payload_json"])} for x in rows]


def active_messages_for(custom_id: str) -> list[dict[str, Any]]:
    conn = db()
    rows = conn.execute("SELECT * FROM messages ORDER BY created_at DESC").fetchall()
    out = []
    for row in rows:
        expires_at = row["created_at"] + row["duration_seconds"]
        if expires_at <= now_ts():
            continue
        ack = conn.execute(
            "SELECT 1 FROM message_acknowledgements WHERE message_id = ? AND recipient_custom_id = ?",
            (row["id"], custom_id),
        ).fetchone()
        out.append({
            "id": row["id"],
            "title": row["title"],
            "message": row["message"],
            "duration_seconds": row["duration_seconds"],
            "acknowledged": bool(ack),
        })
    conn.close()
    return out


def parse_duration(raw: str) -> tuple[Optional[int], str]:
    raw = raw.strip().upper()
    if raw == "PERMANENTLY":
        return None, "PERMANENTLY"
    match = re.fullmatch(r"(\d+)([DWM])", raw)
    if not match:
        raise HTTPException(status_code=400, detail="Use 7D, 2W, 1M or PERMANENTLY.")
    amount = int(match.group(1))
    unit = match.group(2)
    if unit == "D":
        return now_ts() + amount * 86400, f"{amount}D"
    if unit == "W":
        return now_ts() + amount * 7 * 86400, f"{amount}W"
    return now_ts() + amount * 30 * 86400, f"{amount}M"


def parse_shutdown_duration(raw: str) -> tuple[int, str]:
    raw = raw.strip().upper()
    match = re.fullmatch(r"(\d+)([SMHDW])", raw)
    if not match:
        raise HTTPException(status_code=400, detail="Use 30S, 2H, 3D, 2W or 1M.")
    amount = int(match.group(1))
    unit = match.group(2)
    if unit == "S":
        seconds = amount
        label = f"{amount} second" + ("" if amount == 1 else "s")
    elif unit == "H":
        seconds = amount * 3600
        label = f"{amount} hour" + ("" if amount == 1 else "s")
    elif unit == "D":
        seconds = amount * 86400
        label = f"{amount} day" + ("" if amount == 1 else "s")
    elif unit == "W":
        seconds = amount * 7 * 86400
        label = f"{amount} week" + ("" if amount == 1 else "s")
    else:
        seconds = amount * 30 * 86400
        label = f"{amount} month" + ("" if amount == 1 else "s")
    if seconds > 30 * 86400:
        raise HTTPException(status_code=400, detail="Shutdown cannot exceed 1 month.")
    return now_ts() + seconds, label


def parse_join_period(raw: str) -> str:
    value = raw.strip().upper()
    allowed = {"SECOND", "HOUR", "DAY", "WEEK", "MONTH"}
    if value not in allowed:
        raise HTTPException(status_code=400, detail="Use SECOND, HOUR, DAY, WEEK or MONTH.")
    return value


def join_limit_window(period: str, ts: Optional[int] = None) -> tuple[int, int]:
    cur = ts or now_ts()
    lt = time.localtime(cur)
    if period == "SECOND":
        start = cur
        end = cur + 1
    elif period == "HOUR":
        start = cur - lt.tm_min * 60 - lt.tm_sec
        end = start + 3600
    elif period == "DAY":
        start = cur - lt.tm_hour * 3600 - lt.tm_min * 60 - lt.tm_sec
        end = start + 86400
    elif period == "WEEK":
        day_start = cur - lt.tm_hour * 3600 - lt.tm_min * 60 - lt.tm_sec
        start = day_start - lt.tm_wday * 86400
        end = start + 7 * 86400
    else:
        first = time.mktime((lt.tm_year, lt.tm_mon, 1, 0, 0, 0, 0, 0, -1))
        if lt.tm_mon == 12:
            nxt = time.mktime((lt.tm_year + 1, 1, 1, 0, 0, 0, 0, 0, -1))
        else:
            nxt = time.mktime((lt.tm_year, lt.tm_mon + 1, 1, 0, 0, 0, 0, 0, -1))
        start = int(first)
        end = int(nxt)
    return int(start), int(end)


def score_answer(text: str, keywords: list[str]) -> int:
    lowered = text.lower()
    score = 20 + min(len(text.strip()) // 30, 20)
    for word in keywords:
        if word in lowered:
            score += 15
    return min(score, 100)


def roblox_lookup_username(username: str) -> tuple[int, str]:
    res = requests.post(
        "https://users.roblox.com/v1/usernames/users",
        json={"usernames": [username], "excludeBannedUsers": False},
        timeout=10,
    )
    res.raise_for_status()
    data = res.json().get("data", [])
    if not data:
        raise HTTPException(status_code=400, detail="Roblox account not found.")
    item = data[0]
    return int(item["id"]), item["name"]


def roblox_lookup_profile_link(link: str) -> tuple[int, str]:
    link = link.strip()
    username_match = re.search(r"/users/profile\?username=([^/?&#]+)", link, re.I)
    if username_match:
        return roblox_lookup_username(username_match.group(1))
    direct_match = re.search(r"/users/(\d+)/profile", link, re.I)
    if direct_match:
        user_id = int(direct_match.group(1))
        res = requests.get(f"https://users.roblox.com/v1/users/{user_id}", timeout=10)
        res.raise_for_status()
        data = res.json()
        return user_id, data["name"]
    raise HTTPException(status_code=400, detail="Invalid Roblox profile link.")


def roblox_get_profile_description(user_id: int) -> str:
    res = requests.get(f"https://users.roblox.com/v1/users/{user_id}", timeout=10)
    res.raise_for_status()
    data = res.json()
    return data.get("description", "") or ""


def roblox_get_group_role(user_id: int) -> tuple[str, Optional[int]]:
    if not ROBLOX_GROUP_ID:
        return "Guest", None
    res = requests.get(f"https://groups.roblox.com/v2/users/{user_id}/groups/roles", timeout=10)
    res.raise_for_status()
    rows = res.json().get("data", [])
    for item in rows:
        group = item.get("group", {})
        if int(group.get("id", 0)) == int(ROBLOX_GROUP_ID):
            rank = int(item.get("role", {}).get("rank", 0))
            return ROBLOX_GROUP_RANK_TO_IA_ROLE.get(rank, "Guest"), rank
    return "Guest", None


def log_permission_abuse(actor: sqlite3.Row, target: sqlite3.Row, action_name: str, suspended: bool) -> None:
    post_webhook(
        PERMISSION_ABUSE_WEBHOOK,
        "Permission Abuse Logged",
        [
            {"name": "Custom ID", "value": actor["custom_id"]},
            {"name": "Username", "value": actor["roblox_username"] or actor["username"]},
            {"name": "Role", "value": actor["role_name"]},
            {"name": "Against Custom ID", "value": target["custom_id"]},
            {"name": "Against Username", "value": target["roblox_username"] or target["username"]},
            {"name": "Against Role", "value": target["role_name"]},
            {"name": "Action", "value": action_name},
            {"name": "Date", "value": human_time()},
            {"name": "Suspended", "value": "Yes" if suspended else "No"},
        ],
        footer=actor["custom_id"],
    )


def handle_permission_abuse(actor: sqlite3.Row, target: sqlite3.Row, action_name: str) -> None:
    conn = db()
    existing = conn.execute(
        """
        SELECT * FROM permission_abuse_cases
        WHERE abuser_custom_id = ? AND resolved_at IS NULL
        ORDER BY id DESC LIMIT 1
        """,
        (actor["custom_id"],),
    ).fetchone()

    warning_count = (existing["warning_count"] + 1) if existing else 1
    suspended = warning_count >= 2
    reason = "Permission abuse against a higher rank."

    if existing:
        conn.execute(
            """
            UPDATE permission_abuse_cases
            SET target_custom_id = ?, target_username = ?, target_role_name = ?,
                action_name = ?, warning_count = ?, suspended = ?, suspended_reason = ?, updated_at = ?
            WHERE id = ?
            """,
            (
                target["custom_id"],
                target["roblox_username"] or target["username"],
                target["role_name"],
                action_name,
                warning_count,
                1 if suspended else 0,
                reason if suspended else None,
                now_ts(),
                existing["id"],
            ),
        )
    else:
        conn.execute(
            """
            INSERT INTO permission_abuse_cases (
                abuser_custom_id, abuser_username, abuser_role_name,
                target_custom_id, target_username, target_role_name,
                action_name, warning_count, suspended, suspended_reason, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                actor["custom_id"],
                actor["roblox_username"] or actor["username"],
                actor["role_name"],
                target["custom_id"],
                target["roblox_username"] or target["username"],
                target["role_name"],
                action_name,
                warning_count,
                1 if suspended else 0,
                reason if suspended else None,
                now_ts(),
                now_ts(),
            ),
        )

    if suspended:
        conn.execute(
            "UPDATE sessions SET suspended = 1, suspended_reason = ? WHERE custom_id = ?",
            (reason, actor["custom_id"]),
        )

    conn.commit()
    conn.close()

    add_event(
        actor["custom_id"],
        "permission_abuse_suspended" if suspended else "permission_abuse_warning",
        {"message": "You have been temporarily suspended for permission abuse." if suspended else "Warning: do not use punishment commands against someone with a higher rank than you."},
    )
    log_permission_abuse(actor, target, action_name, suspended)


def require_not_higher_rank(actor: sqlite3.Row, target: sqlite3.Row, action_name: str) -> None:
    if role_level(target["role_name"]) > role_level(actor["role_name"]):
        handle_permission_abuse(actor, target, action_name)
        raise HTTPException(status_code=403, detail="Permission abuse detected.")


def active_shutdown() -> Optional[sqlite3.Row]:
    conn = db()
    row = conn.execute(
        "SELECT * FROM website_shutdown WHERE active = 1 AND until_ts > ? ORDER BY id DESC LIMIT 1",
        (now_ts(),),
    ).fetchone()
    conn.close()
    return row


def shutdown_block_for(row: sqlite3.Row) -> Optional[dict[str, Any]]:
    shutdown = active_shutdown()
    if not shutdown:
        return None
    if row["role_name"] in {"High Rank", "Headquarters", "Joint Chiefs", "Ownership"}:
        return None
    return {"active": True, "time_label": shutdown["time_label"]}


def check_join_limit_for_new_visitor(visitor_key: str) -> dict[str, Any]:
    conn = db()
    seen = conn.execute("SELECT 1 FROM known_visitors WHERE visitor_key = ?", (visitor_key,)).fetchone()
    limit_row = conn.execute("SELECT * FROM join_limits WHERE active = 1 ORDER BY id DESC LIMIT 1").fetchone()

    if seen or not limit_row:
        if not seen:
            conn.execute("INSERT INTO known_visitors (visitor_key, first_seen_at) VALUES (?, ?)", (visitor_key, now_ts()))
            conn.commit()
        conn.close()
        return {"allowed": True}

    period = limit_row["period_name"]
    start_ts, _end_ts = join_limit_window(period)
    count = conn.execute(
        "SELECT COUNT(*) AS c FROM join_limit_joins WHERE join_limit_id = ? AND window_start_ts = ?",
        (limit_row["id"], start_ts),
    ).fetchone()["c"]

    if count >= limit_row["join_amount"]:
        conn.close()
        return {
            "allowed": False,
            "join_amount": limit_row["join_amount"],
            "period_name": period,
            "message": f'The website has been restricted to "{limit_row["join_amount"]}" joins, per "{period.title()}". Please, retry another time!',
        }

    conn.execute("INSERT INTO known_visitors (visitor_key, first_seen_at) VALUES (?, ?)", (visitor_key, now_ts()))
    conn.execute(
        "INSERT INTO join_limit_joins (join_limit_id, visitor_key, window_start_ts, created_at) VALUES (?, ?, ?, ?)",
        (limit_row["id"], visitor_key, start_ts, now_ts()),
    )
    conn.commit()
    conn.close()
    return {"allowed": True}


def setup_db() -> None:
    conn = db()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            session_key TEXT PRIMARY KEY,
            custom_id TEXT NOT NULL,
            visitor_key TEXT,
            username TEXT NOT NULL,
            roblox_username TEXT,
            roblox_user_id INTEGER,
            role_name TEXT NOT NULL DEFAULT 'Guest',
            verified INTEGER NOT NULL DEFAULT 0,
            verification_code TEXT,
            verification_target TEXT,
            base_permissions TEXT NOT NULL,
            extra_permissions TEXT NOT NULL DEFAULT '[]',
            forced_logout INTEGER NOT NULL DEFAULT 0,
            suspended INTEGER NOT NULL DEFAULT 0,
            suspended_reason TEXT,
            created_at INTEGER NOT NULL,
            last_seen_at INTEGER NOT NULL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS known_visitors (
            visitor_key TEXT PRIMARY KEY,
            first_seen_at INTEGER NOT NULL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS join_limits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            active INTEGER NOT NULL DEFAULT 1,
            join_amount INTEGER NOT NULL,
            period_name TEXT NOT NULL,
            created_by_custom_id TEXT NOT NULL,
            created_by_username TEXT NOT NULL,
            created_at INTEGER NOT NULL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS join_limit_joins (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            join_limit_id INTEGER NOT NULL,
            visitor_key TEXT NOT NULL,
            window_start_ts INTEGER NOT NULL,
            created_at INTEGER NOT NULL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            author_custom_id TEXT NOT NULL,
            author_username TEXT NOT NULL,
            target_username TEXT NOT NULL,
            division TEXT NOT NULL,
            urgency TEXT NOT NULL,
            reason TEXT NOT NULL,
            evidence TEXT NOT NULL,
            created_at INTEGER NOT NULL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS report_blacklists (
            username TEXT PRIMARY KEY,
            custom_id TEXT,
            reason TEXT NOT NULL,
            time_label TEXT NOT NULL,
            until_ts INTEGER
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS terminations (
            username TEXT PRIMARY KEY,
            custom_id TEXT,
            reason TEXT NOT NULL,
            time_label TEXT NOT NULL,
            until_ts INTEGER
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sender_username TEXT NOT NULL,
            sender_custom_id TEXT NOT NULL,
            title TEXT NOT NULL,
            message TEXT NOT NULL,
            duration_seconds INTEGER NOT NULL,
            created_at INTEGER NOT NULL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS message_acknowledgements (
            message_id INTEGER NOT NULL,
            recipient_custom_id TEXT NOT NULL,
            acknowledged_at INTEGER NOT NULL,
            PRIMARY KEY (message_id, recipient_custom_id)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS inbox_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            recipient_custom_id TEXT NOT NULL,
            title TEXT NOT NULL,
            message TEXT NOT NULL,
            kind TEXT NOT NULL,
            unread INTEGER NOT NULL DEFAULT 1,
            created_at INTEGER NOT NULL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS client_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            recipient_custom_id TEXT NOT NULL,
            event_type TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            consumed_at INTEGER,
            created_at INTEGER NOT NULL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS appeals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            appellant_custom_id TEXT NOT NULL,
            appellant_username TEXT NOT NULL,
            punishment_details TEXT NOT NULL,
            learned_answer TEXT NOT NULL,
            future_answer TEXT NOT NULL,
            extra_answer TEXT NOT NULL,
            score_1 INTEGER NOT NULL,
            score_2 INTEGER NOT NULL,
            score_3 INTEGER NOT NULL,
            score_4 INTEGER NOT NULL,
            overall_score INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'PENDING',
            created_at INTEGER NOT NULL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS website_shutdown (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            active INTEGER NOT NULL DEFAULT 1,
            time_label TEXT NOT NULL,
            until_ts INTEGER NOT NULL,
            created_by_custom_id TEXT NOT NULL,
            created_by_username TEXT NOT NULL,
            created_at INTEGER NOT NULL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS tickets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            opener_custom_id TEXT NOT NULL,
            opener_username TEXT NOT NULL,
            subject TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'OPEN',
            claimed_by_custom_id TEXT,
            claimed_by_username TEXT,
            close_reason TEXT,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ticket_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticket_id INTEGER NOT NULL,
            sender_custom_id TEXT NOT NULL,
            sender_username TEXT NOT NULL,
            sender_role TEXT NOT NULL,
            message TEXT NOT NULL,
            created_at INTEGER NOT NULL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS permission_abuse_cases (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            abuser_custom_id TEXT NOT NULL,
            abuser_username TEXT NOT NULL,
            abuser_role_name TEXT NOT NULL,
            target_custom_id TEXT,
            target_username TEXT NOT NULL,
            target_role_name TEXT NOT NULL,
            action_name TEXT NOT NULL,
            warning_count INTEGER NOT NULL DEFAULT 1,
            suspended INTEGER NOT NULL DEFAULT 0,
            suspended_reason TEXT,
            resolved_by_custom_id TEXT,
            resolved_by_username TEXT,
            resolved_at INTEGER,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        )
    """)
    conn.commit()
    conn.close()


setup_db()


def session_row(session_key: str) -> sqlite3.Row:
    conn = db()
    row = conn.execute("SELECT * FROM sessions WHERE session_key = ?", (session_key,)).fetchone()
    if row:
        conn.execute("UPDATE sessions SET last_seen_at = ? WHERE session_key = ?", (now_ts(), session_key))
        conn.commit()
    conn.close()
    if not row:
        raise HTTPException(status_code=401, detail="Invalid session.")
    return row


def require_permission(session_key: str, permission: str) -> sqlite3.Row:
    row = session_row(session_key)
    if permission not in merged_permissions(row):
        raise HTTPException(status_code=403, detail="No permission.")
    return row


@app.get("/")
def root():
    return FileResponse(INDEX_FILE)


@app.post("/api/bootstrap")
def bootstrap(payload: dict[str, Any] | None = None):
    payload = payload or {}
    visitor_key = (payload.get("visitor_key") or "").strip() or make_visitor_key()
    join_check = check_join_limit_for_new_visitor(visitor_key)
    if not join_check["allowed"]:
        raise HTTPException(status_code=429, detail=json.dumps(join_check))

    session_key = make_session_key()
    custom_id = make_custom_id()
    username = f"Guest-{custom_id[-4:]}"
    conn = db()
    conn.execute(
        """
        INSERT INTO sessions (
            session_key, custom_id, visitor_key, username, role_name, base_permissions,
            extra_permissions, created_at, last_seen_at
        ) VALUES (?, ?, ?, ?, 'Guest', ?, '[]', ?, ?)
        """,
        (session_key, custom_id, visitor_key, username, json_dump(role_permissions("Guest")), now_ts(), now_ts()),
    )
    conn.commit()
    conn.close()
    return {"session_key": session_key, "custom_id": custom_id, "visitor_key": visitor_key}


@app.get("/api/me")
def me(session_key: str):
    row = session_row(session_key)
    conn = db()
    blacklist = conn.execute(
        "SELECT * FROM report_blacklists WHERE custom_id = ? OR username = ?",
        (row["custom_id"], row["roblox_username"] or row["username"]),
    ).fetchone()
    termination = conn.execute(
        "SELECT * FROM terminations WHERE custom_id = ? OR username = ?",
        (row["custom_id"], row["roblox_username"] or row["username"]),
    ).fetchone()
    join_limit = conn.execute("SELECT * FROM join_limits WHERE active = 1 ORDER BY id DESC LIMIT 1").fetchone()
    conn.close()
    inbox = inbox_for(row["custom_id"])
    return {
        "custom_id": row["custom_id"],
        "username": row["username"],
        "roblox_username": row["roblox_username"],
        "roblox_user_id": row["roblox_user_id"],
        "avatar_url": public_avatar_url(row["roblox_user_id"]),
        "role_name": row["role_name"],
        "verified": bool(row["verified"]),
        "forced_logout": bool(row["forced_logout"]),
        "suspended": bool(row["suspended"]),
        "suspended_reason": row["suspended_reason"],
        "base_permissions": json_load(row["base_permissions"]),
        "extra_permissions": json_load(row["extra_permissions"]),
        "grantable_permissions": grantable_permissions_for(row["role_name"]),
        "messages": active_messages_for(row["custom_id"]),
        "events": pending_events_for(row["custom_id"]),
        "inbox_unread_count": inbox["unread_count"],
        "report_blacklisted": bool(blacklist and is_active(blacklist["until_ts"])),
        "terminated": bool(termination and is_active(termination["until_ts"])),
        "terminated_reason": termination["reason"] if termination and is_active(termination["until_ts"]) else None,
        "terminated_time_label": termination["time_label"] if termination and is_active(termination["until_ts"]) else None,
        "shutdown": shutdown_block_for(row),
        "join_limit": dict(join_limit) if join_limit else None,
    }


@app.get("/api/logged-accounts")
def logged_accounts(session_key: str):
    session_row(session_key)
    conn = db()
    rows = conn.execute(
        """
        SELECT custom_id, username, roblox_username, roblox_user_id, role_name
        FROM sessions
        WHERE verified = 1 AND roblox_username IS NOT NULL
        ORDER BY last_seen_at DESC
        """
    ).fetchall()
    conn.close()
    items = []
    for row in rows:
        item = dict(row)
        item["avatar_url"] = public_avatar_url(row["roblox_user_id"])
        items.append(item)
    return {"items": items}


@app.post("/api/logout")
def logout(payload: dict[str, Any]):
    row = session_row(payload["session_key"])
    guest_name = f"Guest-{row['custom_id'][-4:]}"
    conn = db()
    conn.execute(
        """
        UPDATE sessions
        SET username = ?, roblox_username = NULL, roblox_user_id = NULL,
            role_name = 'Guest', verified = 0, verification_code = NULL,
            verification_target = NULL, base_permissions = ?, extra_permissions = '[]',
            forced_logout = 0
        WHERE session_key = ?
        """,
        (guest_name, json_dump(role_permissions("Guest")), row["session_key"]),
    )
    conn.commit()
    conn.close()
    return {"success": True}


@app.post("/api/verification/start")
def verification_start(payload: dict[str, Any]):
    row = session_row(payload["session_key"])
    profile_link = payload.get("profile_link", "").strip()
    user_id, username = roblox_lookup_profile_link(profile_link)
    code = make_verification_code()

    conn = db()
    while conn.execute("SELECT 1 FROM sessions WHERE verification_code = ?", (code,)).fetchone():
        code = make_verification_code()
    conn.execute(
        """
        UPDATE sessions
        SET verification_code = ?, verification_target = ?, roblox_user_id = ?, roblox_username = ?
        WHERE session_key = ?
        """,
        (code, profile_link, user_id, username, row["session_key"]),
    )
    conn.commit()
    conn.close()
    return {"success": True, "verification_code": code, "roblox_username": username}


@app.post("/api/verification/check")
def verification_check(payload: dict[str, Any]):
    row = session_row(payload["session_key"])
    if not row["verification_code"] or not row["roblox_user_id"]:
        raise HTTPException(status_code=400, detail="Verification not started.")

    description = roblox_get_profile_description(int(row["roblox_user_id"]))
    if row["verification_code"] not in description:
        raise HTTPException(status_code=400, detail="Something went wrong, please retry.")

    role_name, _rank = roblox_get_group_role(int(row["roblox_user_id"]))
    username = row["roblox_username"] or row["username"]

    conn = db()
    conn.execute(
        """
        UPDATE sessions
        SET verified = 1, username = ?, role_name = ?, base_permissions = ?,
            verification_code = NULL, forced_logout = 0
        WHERE session_key = ?
        """,
        (username, role_name, json_dump(role_permissions(role_name)), row["session_key"]),
    )
    conn.commit()
    conn.close()

    updated = session_row(row["session_key"])
    log_action(updated, "Verified Roblox Account")
    return {"success": True, "role_name": role_name, "username": username}


@app.post("/api/consume-event")
def consume_event(payload: dict[str, Any]):
    row = session_row(payload["session_key"])
    conn = db()
    conn.execute(
        "UPDATE client_events SET consumed_at = ? WHERE id = ? AND recipient_custom_id = ?",
        (now_ts(), int(payload["event_id"]), row["custom_id"]),
    )
    conn.commit()
    conn.close()
    return {"success": True}


@app.get("/api/inbox")
def get_inbox(session_key: str):
    row = session_row(session_key)
    return inbox_for(row["custom_id"])


@app.post("/api/mark-inbox-read")
def mark_inbox_read(payload: dict[str, Any]):
    row = session_row(payload["session_key"])
    inbox_id = payload.get("inbox_id")
    conn = db()
    if inbox_id:
        conn.execute(
            "UPDATE inbox_items SET unread = 0 WHERE id = ? AND recipient_custom_id = ?",
            (int(inbox_id), row["custom_id"]),
        )
    else:
        conn.execute("UPDATE inbox_items SET unread = 0 WHERE recipient_custom_id = ?", (row["custom_id"],))
    conn.commit()
    conn.close()
    return {"success": True}


@app.post("/api/delete-inbox-item")
def delete_inbox_item(payload: dict[str, Any]):
    row = session_row(payload["session_key"])
    inbox_id = int(payload["inbox_id"])
    conn = db()
    conn.execute(
        "DELETE FROM inbox_items WHERE id = ? AND recipient_custom_id = ?",
        (inbox_id, row["custom_id"]),
    )
    conn.commit()
    conn.close()
    return {"success": True}


@app.post("/api/acknowledge-message")
def acknowledge_message(payload: dict[str, Any]):
    row = session_row(payload["session_key"])
    conn = db()
    conn.execute(
        "INSERT OR REPLACE INTO message_acknowledgements (message_id, recipient_custom_id, acknowledged_at) VALUES (?, ?, ?)",
        (int(payload["message_id"]), row["custom_id"], now_ts()),
    )
    conn.commit()
    conn.close()
    return {"success": True}


@app.get("/api/logged-users")
def logged_users(session_key: str):
    require_permission(session_key, "view_logged_users")
    conn = db()
    rows = conn.execute(
        """
        SELECT username, roblox_username, custom_id, role_name, verified, forced_logout, suspended
        FROM sessions
        ORDER BY last_seen_at DESC
        """
    ).fetchall()
    conn.close()
    return {"users": [dict(x) for x in rows]}


@app.post("/api/force-logout")
def force_logout(payload: dict[str, Any]):
    actor = require_permission(payload["session_key"], "force_logout")
    target_custom_id = payload.get("target_custom_id", "").strip()
    target = find_target_session(target_custom_id)
    if not target:
        raise HTTPException(status_code=404, detail="User not found.")

    require_not_higher_rank(actor, target, "Force Logout")

    conn = db()
    conn.execute(
        """
        UPDATE sessions
        SET forced_logout = 1, verified = 0, role_name = 'Guest',
            base_permissions = ?, extra_permissions = '[]', verification_code = NULL
        WHERE custom_id = ?
        """,
        (json_dump(role_permissions("Guest")), target["custom_id"]),
    )
    conn.commit()
    conn.close()

    add_event(target["custom_id"], "force_logout", {"message": "You have been logged out of your account and thus need to verify again!"})
    add_inbox_item(target["custom_id"], "Internal Affairs System", "You have been logged out of your account and thus need to verify again!")
    log_action(actor, "Force Logout", target["custom_id"], target["roblox_username"] or target["username"])
    return {"success": True}


@app.post("/api/bulk-force-logout")
def bulk_force_logout(payload: dict[str, Any]):
    actor = require_permission(payload["session_key"], "bulk_force_logout")
    target_ids = payload.get("target_custom_ids", [])
    reason = payload.get("reason", "").strip() or "Bulk force logout"
    if not target_ids:
        raise HTTPException(status_code=400, detail="Select at least one user.")

    conn = db()
    done = []
    for target_id in target_ids:
        target = conn.execute("SELECT * FROM sessions WHERE custom_id = ?", (target_id,)).fetchone()
        if not target:
            continue
        if role_level(target["role_name"]) > role_level(actor["role_name"]):
            continue
        conn.execute(
            """
            UPDATE sessions
            SET forced_logout = 1, verified = 0, role_name = 'Guest',
                base_permissions = ?, extra_permissions = '[]', verification_code = NULL
            WHERE custom_id = ?
            """,
            (json_dump(role_permissions("Guest")), target["custom_id"]),
        )
        done.append(target)
    conn.commit()
    conn.close()

    for target in done:
        add_event(target["custom_id"], "force_logout", {"message": f"You have been logged out of your account. Reason: {reason}"})
        add_inbox_item(target["custom_id"], "Internal Affairs System", f"You have been logged out of your account. Reason: {reason}")
    log_action(actor, f"Bulk Force Logout ({len(done)})")
    return {"success": True, "count": len(done)}


@app.post("/api/report")
def submit_report(payload: dict[str, Any]):
    row = session_row(payload["session_key"])
    conn = db()
    blacklist = conn.execute(
        "SELECT * FROM report_blacklists WHERE username = ? OR custom_id = ?",
        (row["roblox_username"] or row["username"], row["custom_id"]),
    ).fetchone()
    if blacklist and is_active(blacklist["until_ts"]):
        conn.close()
        raise HTTPException(status_code=403, detail="You have been report blacklisted.")

    conn.execute(
        """
        INSERT INTO reports (
            author_custom_id, author_username, target_username, division,
            urgency, reason, evidence, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            row["custom_id"],
            row["roblox_username"] or row["username"],
            payload.get("target_username", "").strip(),
            payload.get("division", "").strip(),
            payload.get("urgency", "").strip(),
            payload.get("reason", "").strip(),
            payload.get("evidence", "").strip(),
            now_ts(),
        ),
    )
    conn.commit()
    conn.close()

    post_webhook(
        REPORT_LOG_WEBHOOK,
        "Divisional Report Logged & Ready For Review",
        [
            {"name": "Username of the person being reported", "value": payload.get("target_username", "")},
            {"name": "Division", "value": payload.get("division", "")},
            {"name": "Urgency Level", "value": payload.get("urgency", "")},
            {"name": "Reason of your report", "value": payload.get("reason", "")},
            {"name": "Evidence", "value": payload.get("evidence", "")},
        ],
        footer=row["custom_id"],
    )
    log_action(row, "Submitted Divisional Report")
    return {"success": True}


@app.post("/api/report-blacklist")
def report_blacklist(payload: dict[str, Any]):
    actor = require_permission(payload["session_key"], "report_blacklist")
    target_ref = payload.get("target_ref", "").strip()
    reason = payload.get("reason", "").strip()
    until_ts, label = parse_duration(payload.get("time", "").strip())

    target = find_target_session(target_ref)
    if not target:
        raise HTTPException(status_code=404, detail="User not found.")

    require_not_higher_rank(actor, target, "Report Blacklist")

    target_username = target["roblox_username"] or target["username"]
    conn = db()
    conn.execute(
        "REPLACE INTO report_blacklists (username, custom_id, reason, time_label, until_ts) VALUES (?, ?, ?, ?, ?)",
        (target_username, target["custom_id"], reason, label, until_ts),
    )
    conn.commit()
    conn.close()

    add_inbox_item(target["custom_id"], "Internal Affairs System", f"You have been report blacklisted for {reason} for {label}.")
    log_action(actor, "Report Blacklist", target["custom_id"], target_username)
    return {"success": True}


@app.get("/api/report-blacklists")
def list_report_blacklists(session_key: str):
    require_permission(session_key, "revoke_report_blacklist")
    conn = db()
    rows = conn.execute("SELECT username, custom_id, reason, time_label FROM report_blacklists ORDER BY username ASC").fetchall()
    conn.close()
    return {"items": [dict(x) for x in rows]}


@app.post("/api/revoke-report-blacklist")
def revoke_report_blacklist(payload: dict[str, Any]):
    actor = require_permission(payload["session_key"], "revoke_report_blacklist")
    target_username = payload.get("target_username", "").strip()
    conn = db()
    row = conn.execute("SELECT * FROM report_blacklists WHERE username = ?", (target_username,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="No blacklist found.")
    conn.execute("DELETE FROM report_blacklists WHERE username = ?", (target_username,))
    conn.commit()
    conn.close()

    if row["custom_id"]:
        add_event(row["custom_id"], "report_blacklist_revoked", {"message": "Your report blacklist has been revoked."})
        add_inbox_item(row["custom_id"], "Internal Affairs System", "Your report blacklist has been revoked.")
    log_action(actor, "Revoke Report Blacklist", row["custom_id"] or "N/A", target_username)
    return {"success": True}


@app.post("/api/terminate")
def terminate(payload: dict[str, Any]):
    actor = require_permission(payload["session_key"], "terminate_user")
    target_ref = payload.get("target_ref", "").strip()
    reason = payload.get("reason", "").strip()
    until_ts, label = parse_duration(payload.get("time", "").strip())

    target = find_target_session(target_ref)
    if not target:
        raise HTTPException(status_code=404, detail="User not found.")

    require_not_higher_rank(actor, target, "Terminate")

    target_username = target["roblox_username"] or target["username"]
    conn = db()
    conn.execute(
        "REPLACE INTO terminations (username, custom_id, reason, time_label, until_ts) VALUES (?, ?, ?, ?, ?)",
        (target_username, target["custom_id"], reason, label, until_ts),
    )
    conn.commit()
    conn.close()

    add_inbox_item(target["custom_id"], "Internal Affairs System", f"You have been terminated for {reason} for {label}.")
    add_event(target["custom_id"], "terminated", {"reason": reason, "time_label": label})
    log_action(actor, "Terminate User", target["custom_id"], target_username)
    return {"success": True}


@app.get("/api/terminations")
def list_terminations(session_key: str):
    require_permission(session_key, "revoke_terminate")
    conn = db()
    rows = conn.execute("SELECT username, custom_id, reason, time_label FROM terminations ORDER BY username ASC").fetchall()
    conn.close()
    return {"items": [dict(x) for x in rows]}


@app.post("/api/revoke-terminate")
def revoke_terminate(payload: dict[str, Any]):
    actor = require_permission(payload["session_key"], "revoke_terminate")
    target_username = payload.get("target_username", "").strip()
    conn = db()
    row = conn.execute("SELECT * FROM terminations WHERE username = ?", (target_username,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="No termination found.")
    conn.execute("DELETE FROM terminations WHERE username = ?", (target_username,))
    conn.commit()
    conn.close()

    if row["custom_id"]:
        add_event(row["custom_id"], "termination_revoked", {"message": "Your termination has been revoked."})
        add_inbox_item(row["custom_id"], "Internal Affairs System", "Your termination has been revoked.")
    log_action(actor, "Revoke Terminate", row["custom_id"] or "N/A", target_username)
    return {"success": True}


@app.post("/api/give-permissions")
def give_permissions(payload: dict[str, Any]):
    actor = require_permission(payload["session_key"], "give_permissions")
    target_custom_id = payload.get("target_custom_id", "").strip()
    permissions = set(payload.get("permissions", []))
    if not target_custom_id or not permissions:
        raise HTTPException(status_code=400, detail="Missing fields.")

    conn = db()
    target = conn.execute("SELECT * FROM sessions WHERE custom_id = ?", (target_custom_id,)).fetchone()
    if not target:
        conn.close()
        raise HTTPException(status_code=404, detail="User not found.")
    current = set(json_load(target["extra_permissions"]))
    updated = sorted(current | permissions)
    conn.execute("UPDATE sessions SET extra_permissions = ? WHERE custom_id = ?", (json_dump(updated), target_custom_id))
    conn.commit()
    conn.close()

    add_inbox_item(target_custom_id, "Internal Affairs System", f"Permissions granted: {', '.join(sorted(permissions))}")
    log_action(actor, "Give Permissions", target_custom_id, target["roblox_username"] or target["username"])
    return {"success": True}


@app.post("/api/remove-permissions")
def remove_permissions(payload: dict[str, Any]):
    actor = require_permission(payload["session_key"], "remove_permissions")
    target_custom_id = payload.get("target_custom_id", "").strip()
    permissions = set(payload.get("permissions", []))
    if not target_custom_id or not permissions:
        raise HTTPException(status_code=400, detail="Missing fields.")

    conn = db()
    target = conn.execute("SELECT * FROM sessions WHERE custom_id = ?", (target_custom_id,)).fetchone()
    if not target:
        conn.close()
        raise HTTPException(status_code=404, detail="User not found.")

    require_not_higher_rank(actor, target, "Remove Permissions")

    current = set(json_load(target["extra_permissions"]))
    updated = sorted(current - permissions)
    conn.execute("UPDATE sessions SET extra_permissions = ? WHERE custom_id = ?", (json_dump(updated), target_custom_id))
    conn.commit()
    conn.close()

    add_inbox_item(target_custom_id, "Internal Affairs System", f"Permissions removed: {', '.join(sorted(permissions))}")
    log_action(actor, "Remove Permissions", target_custom_id, target["roblox_username"] or target["username"])
    return {"success": True}


@app.post("/api/send-inbox-message")
def send_inbox_message(payload: dict[str, Any]):
    actor = require_permission(payload["session_key"], "send_inbox_message")
    target_custom_id = payload.get("target_custom_id", "").strip()
    message = payload.get("message", "").strip()
    if not target_custom_id or not message:
        raise HTTPException(status_code=400, detail="Missing fields.")

    target = find_target_session(target_custom_id)
    if not target:
        raise HTTPException(status_code=404, detail="Target not found.")

    add_inbox_item(target["custom_id"], "Internal Affairs System", message)
    add_event(target["custom_id"], "inbox_message_received", {"message": "An Internal Affairs message has been sent to your inbox."})
    log_action(actor, "Send Message In Inbox", target["custom_id"], target["roblox_username"] or target["username"])
    return {"success": True}


@app.post("/api/send-global-message")
def send_global_message(payload: dict[str, Any]):
    actor = require_permission(payload["session_key"], "send_global_message")
    message = payload.get("message", "").strip()
    duration = int(payload.get("duration_seconds", 30))
    if not message:
        raise HTTPException(status_code=400, detail="Message is required.")

    conn = db()
    conn.execute(
        "INSERT INTO messages (sender_username, sender_custom_id, title, message, duration_seconds, created_at) VALUES (?, ?, ?, ?, ?, ?)",
        (
            actor["roblox_username"] or actor["username"],
            actor["custom_id"],
            "Global Message From the Internal Affairs Headquarters",
            message,
            duration,
            now_ts(),
        ),
    )
    users = conn.execute("SELECT custom_id FROM sessions").fetchall()
    conn.commit()
    conn.close()

    for user in users:
        add_event(user["custom_id"], "global_message_received", {"message": message, "duration_seconds": duration})

    log_action(actor, "Send Global Message", "GLOBAL", "All Users")
    return {"success": True}


@app.post("/api/check-information")
def check_information(payload: dict[str, Any]):
    require_permission(payload["session_key"], "check_information")
    target_custom_id = payload.get("target_custom_id", "").strip()
    conn = db()
    target = conn.execute("SELECT * FROM sessions WHERE custom_id = ?", (target_custom_id,)).fetchone()
    if not target:
        conn.close()
        raise HTTPException(status_code=404, detail="No user found.")
    blacklist = conn.execute("SELECT * FROM report_blacklists WHERE custom_id = ?", (target_custom_id,)).fetchone()
    termination = conn.execute("SELECT * FROM terminations WHERE custom_id = ?", (target_custom_id,)).fetchone()
    conn.close()
    return {
        "username": target["roblox_username"] or target["username"],
        "custom_id": target["custom_id"],
        "role_name": target["role_name"],
        "report_blacklisted": bool(blacklist and is_active(blacklist["until_ts"])),
        "terminated": bool(termination and is_active(termination["until_ts"])),
        "termination_reason": termination["reason"] if termination and is_active(termination["until_ts"]) else None,
    }


@app.post("/api/appeal")
def appeal(payload: dict[str, Any]):
    row = session_row(payload["session_key"])
    q1 = payload.get("punishment_details", "").strip()
    q2 = payload.get("learned_answer", "").strip()
    q3 = payload.get("future_answer", "").strip()
    q4 = payload.get("extra_answer", "").strip()

    s1 = score_answer(q1, ["sorry", "wrong", "fault", "punishment", "responsibility"])
    s2 = score_answer(q2, ["learned", "yes", "regret", "remorse", "understand"])
    s3 = score_answer(q3, ["avoid", "future", "change", "improve", "careful"])
    s4 = score_answer(q4 or "no", ["respect", "chance", "apologize", "prove"])
    overall = round((s1 + s2 + s3 + s4) / 4)

    conn = db()
    conn.execute(
        """
        INSERT INTO appeals (
            appellant_custom_id, appellant_username, punishment_details,
            learned_answer, future_answer, extra_answer,
            score_1, score_2, score_3, score_4, overall_score, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (row["custom_id"], row["roblox_username"] or row["username"], q1, q2, q3, q4, s1, s2, s3, s4, overall, now_ts()),
    )
    conn.commit()
    conn.close()

    post_webhook(
        APPEAL_LOG_WEBHOOK,
        "Appeal Received",
        [
            {"name": "What punishment did you received and why?", "value": q1 or "No answer"},
            {"name": "Did you learned from your actions?", "value": q2 or "No answer"},
            {"name": "What will you do to avoid such thing from happening again in the future?", "value": q3 or "No answer"},
            {"name": "Do you have anything to add?", "value": q4 or "No"},
            {"name": "Appeal Score", "value": f"{overall}%"},
        ],
        footer=row["custom_id"],
    )
    log_action(row, "Submitted Appeal")
    return {"success": True}


@app.get("/api/appeals")
def appeals(session_key: str):
    require_permission(session_key, "review_appeals")
    conn = db()
    rows = conn.execute("SELECT * FROM appeals ORDER BY created_at DESC").fetchall()
    conn.close()
    return {"items": [dict(x) for x in rows]}


@app.post("/api/website-shutdown")
def website_shutdown(payload: dict[str, Any]):
    actor = require_permission(payload["session_key"], "manage_website_shutdown")
    until_ts, label = parse_shutdown_duration(payload.get("time", "").strip())
    conn = db()
    conn.execute("UPDATE website_shutdown SET active = 0 WHERE active = 1")
    conn.execute(
        """
        INSERT INTO website_shutdown (
            active, time_label, until_ts, created_by_custom_id, created_by_username, created_at
        ) VALUES (1, ?, ?, ?, ?, ?)
        """,
        (label, until_ts, actor["custom_id"], actor["roblox_username"] or actor["username"], now_ts()),
    )
    conn.commit()
    conn.close()
    log_action(actor, "Temporary Website Shutdown")
    return {"success": True, "time_label": label}


@app.post("/api/website-shutdown/clear")
def website_shutdown_clear(payload: dict[str, Any]):
    actor = require_permission(payload["session_key"], "manage_website_shutdown")
    conn = db()
    conn.execute("UPDATE website_shutdown SET active = 0 WHERE active = 1")
    conn.commit()
    conn.close()
    log_action(actor, "Remove Website Shutdown")
    return {"success": True}


@app.post("/api/join-limit")
def set_join_limit(payload: dict[str, Any]):
    actor = require_permission(payload["session_key"], "manage_join_limit")
    join_amount = int(payload.get("join_amount", 0))
    period_name = parse_join_period(payload.get("period_name", ""))

    if join_amount <= 0:
        raise HTTPException(status_code=400, detail="Join amount must be above 0.")

    conn = db()
    conn.execute("UPDATE join_limits SET active = 0 WHERE active = 1")
    conn.execute(
        """
        INSERT INTO join_limits (
            active, join_amount, period_name, created_by_custom_id, created_by_username, created_at
        ) VALUES (1, ?, ?, ?, ?, ?)
        """,
        (join_amount, period_name, actor["custom_id"], actor["roblox_username"] or actor["username"], now_ts()),
    )
    conn.commit()
    conn.close()
    log_action(actor, f"Reduce Amount of New Joins ({join_amount} per {period_name})")
    return {"success": True}


@app.post("/api/join-limit/revoke")
def revoke_join_limit(payload: dict[str, Any]):
    actor = require_permission(payload["session_key"], "manage_join_limit")
    conn = db()
    conn.execute("UPDATE join_limits SET active = 0 WHERE active = 1")
    conn.commit()
    conn.close()
    log_action(actor, "Revoke Join Limit")
    return {"success": True}


@app.post("/api/contact/open")
def contact_open(payload: dict[str, Any]):
    row = session_row(payload["session_key"])
    subject = payload.get("subject", "").strip()
    if not subject:
        raise HTTPException(status_code=400, detail="Subject is required.")

    conn = db()
    conn.execute(
        "INSERT INTO tickets (opener_custom_id, opener_username, subject, status, created_at, updated_at) VALUES (?, ?, ?, 'OPEN', ?, ?)",
        (row["custom_id"], row["roblox_username"] or row["username"], subject, now_ts(), now_ts()),
    )
    ticket_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
    conn.execute(
        "INSERT INTO ticket_messages (ticket_id, sender_custom_id, sender_username, sender_role, message, created_at) VALUES (?, ?, ?, ?, ?, ?)",
        (ticket_id, row["custom_id"], row["roblox_username"] or row["username"], row["role_name"], subject, now_ts()),
    )
    conn.commit()
    conn.close()
    log_action(row, "Opened Contact Ticket")
    return {"success": True, "ticket_id": ticket_id}


@app.get("/api/tickets/my")
def tickets_my(session_key: str):
    row = session_row(session_key)
    conn = db()
    rows = conn.execute("SELECT * FROM tickets WHERE opener_custom_id = ? ORDER BY updated_at DESC", (row["custom_id"],)).fetchall()
    conn.close()
    return {"items": [dict(x) for x in rows]}


@app.get("/api/tickets/all")
def tickets_all(session_key: str):
    require_permission(session_key, "view_tickets")
    conn = db()
    rows = conn.execute("SELECT * FROM tickets ORDER BY updated_at DESC").fetchall()
    conn.close()
    return {"items": [dict(x) for x in rows]}


@app.get("/api/tickets/thread")
def tickets_thread(session_key: str, ticket_id: int):
    row = session_row(session_key)
    conn = db()
    ticket = conn.execute("SELECT * FROM tickets WHERE id = ?", (ticket_id,)).fetchone()
    if not ticket:
        conn.close()
        raise HTTPException(status_code=404, detail="Ticket not found.")
    if row["custom_id"] != ticket["opener_custom_id"] and "view_tickets" not in merged_permissions(row):
        conn.close()
        raise HTTPException(status_code=403, detail="No access.")
    messages = conn.execute("SELECT * FROM ticket_messages WHERE ticket_id = ? ORDER BY created_at ASC", (ticket_id,)).fetchall()
    conn.close()
    return {"ticket": dict(ticket), "messages": [dict(x) for x in messages]}


@app.post("/api/tickets/claim")
def tickets_claim(payload: dict[str, Any]):
    actor = require_permission(payload["session_key"], "handle_tickets")
    ticket_id = int(payload["ticket_id"])
    conn = db()
    ticket = conn.execute("SELECT * FROM tickets WHERE id = ?", (ticket_id,)).fetchone()
    if not ticket:
        conn.close()
        raise HTTPException(status_code=404, detail="Ticket not found.")
    conn.execute(
        "UPDATE tickets SET claimed_by_custom_id = ?, claimed_by_username = ?, updated_at = ? WHERE id = ?",
        (actor["custom_id"], actor["roblox_username"] or actor["username"], now_ts(), ticket_id),
    )
    conn.commit()
    conn.close()
    log_action(actor, "Claim Ticket", ticket["opener_custom_id"], ticket["opener_username"])
    return {"success": True}


@app.post("/api/tickets/reply")
def tickets_reply(payload: dict[str, Any]):
    row = session_row(payload["session_key"])
    ticket_id = int(payload["ticket_id"])
    message = payload.get("message", "").strip()
    if not message:
        raise HTTPException(status_code=400, detail="Message is required.")

    conn = db()
    ticket = conn.execute("SELECT * FROM tickets WHERE id = ?", (ticket_id,)).fetchone()
    if not ticket:
        conn.close()
        raise HTTPException(status_code=404, detail="Ticket not found.")
    if row["custom_id"] != ticket["opener_custom_id"] and "view_tickets" not in merged_permissions(row):
        conn.close()
        raise HTTPException(status_code=403, detail="No access.")
    conn.execute(
        "INSERT INTO ticket_messages (ticket_id, sender_custom_id, sender_username, sender_role, message, created_at) VALUES (?, ?, ?, ?, ?, ?)",
        (ticket_id, row["custom_id"], row["roblox_username"] or row["username"], row["role_name"], message, now_ts()),
    )
    conn.execute("UPDATE tickets SET updated_at = ? WHERE id = ?", (now_ts(), ticket_id))
    conn.commit()
    conn.close()

    recipient = ticket["opener_custom_id"] if row["custom_id"] != ticket["opener_custom_id"] else ticket["claimed_by_custom_id"]
    if recipient:
        add_event(recipient, "ticket_reply", {"message": "A ticket reply has been sent."})
        add_inbox_item(recipient, "Internal Affairs System", "A new reply has been added to your ticket.")
    return {"success": True}


@app.post("/api/tickets/close")
def tickets_close(payload: dict[str, Any]):
    actor = require_permission(payload["session_key"], "handle_tickets")
    ticket_id = int(payload["ticket_id"])
    reason = payload.get("reason", "").strip()
    if not reason:
        raise HTTPException(status_code=400, detail="Reason is required.")

    conn = db()
    ticket = conn.execute("SELECT * FROM tickets WHERE id = ?", (ticket_id,)).fetchone()
    if not ticket:
        conn.close()
        raise HTTPException(status_code=404, detail="Ticket not found.")
    conn.execute(
        "UPDATE tickets SET status = 'CLOSED', close_reason = ?, updated_at = ? WHERE id = ?",
        (reason, now_ts(), ticket_id),
    )
    conn.commit()
    conn.close()

    add_event(ticket["opener_custom_id"], "ticket_closed", {"message": "Your ticket has been closed."})
    add_inbox_item(ticket["opener_custom_id"], "Internal Affairs System", "Your ticket has been closed.")
    log_action(actor, "Close Ticket", ticket["opener_custom_id"], ticket["opener_username"])
    return {"success": True}


@app.get("/api/permission-abuse/list")
def permission_abuse_list(session_key: str):
    require_permission(session_key, "view_permission_abuse_database")
    conn = db()
    rows = conn.execute("SELECT * FROM permission_abuse_cases WHERE resolved_at IS NULL ORDER BY updated_at DESC").fetchall()
    conn.close()
    return {"items": [dict(x) for x in rows]}


@app.post("/api/permission-abuse/unsuspend")
def permission_abuse_unsuspend(payload: dict[str, Any]):
    actor = require_permission(payload["session_key"], "unsuspend_permission_abuse")
    abuser_custom_id = payload.get("abuser_custom_id", "").strip()

    conn = db()
    target = conn.execute("SELECT * FROM sessions WHERE custom_id = ?", (abuser_custom_id,)).fetchone()
    if not target:
        conn.close()
        raise HTTPException(status_code=404, detail="User not found.")

    conn.execute("UPDATE sessions SET suspended = 0, suspended_reason = NULL WHERE custom_id = ?", (abuser_custom_id,))
    conn.execute(
        """
        UPDATE permission_abuse_cases
        SET resolved_by_custom_id = ?, resolved_by_username = ?, resolved_at = ?, updated_at = ?
        WHERE abuser_custom_id = ? AND resolved_at IS NULL
        """,
        (actor["custom_id"], actor["roblox_username"] or actor["username"], now_ts(), now_ts(), abuser_custom_id),
    )
    conn.commit()
    conn.close()

    add_event(abuser_custom_id, "permission_abuse_unsuspended", {"message": "A High Rank has removed your temporary suspension."})
    log_action(actor, "Unsuspend Permission Abuse", abuser_custom_id, target["roblox_username"] or target["username"])
    return {"success": True}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
