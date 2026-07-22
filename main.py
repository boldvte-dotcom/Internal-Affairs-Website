from __future__ import annotations

import base64
import io
import json
import os
import random
import re
import sqlite3
import string
import time
from pathlib import Path
from typing import Any, Optional
from urllib.parse import parse_qs, unquote, urlparse

import requests
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

BASE_DIR = Path(__file__).resolve().parent
INDEX_FILE = BASE_DIR / "index.html"
DB_FILE = BASE_DIR / "internal_affairs_clean.db"

ACTION_LOG_WEBHOOK = "https://discord.com/api/webhooks/1490475325874896956/SIqaZjwchopOJiVtJX7o4-AdUaxtmvCbODh-Pta_UpQLuRfN3hJzm1iHJPJR_lKs4okI"
STAFF_ACTION_LOG_WEBHOOK = "https://discord.com/api/webhooks/1502790486706884608/Z9HqrQLSW17mV20DtseLXSzfosHZkziw-7coVBKpn0QK9wJE0aoN1DHVuHAM7HN459Cj"
REPORT_LOG_WEBHOOK = "https://discord.com/api/webhooks/1487913699468513290/rBQf5vyiDN2rdZvo1WQBiS_aDBKkT1vcRqqazExBZB2QHqfV-gaIQQ_7z3MLcFAr7MLv"
APPEAL_LOG_WEBHOOK = "https://discord.com/api/webhooks/1494822440877035561/dYkRfikUYXYjRq_vr9FVLEYaDFjgCkUxMUncze2DA0Rt3srL-CPc7f59iIhfvUvVa-3X"
PERMISSION_ABUSE_WEBHOOK = "https://discord.com/api/webhooks/1490475547925680248/efCrT5jds6-LsKFGQfWugvbS28YseOaG_HM1dhFTc3Uj9G5PGiV0b-WvAekPd4pihmLQ"
INTERVIEW_LOG_WEBHOOK = "https://discord.com/api/webhooks/1502750825645080780/7dopNSSb1lTZLRYYtr3U8H7I8rqreK-T-QxIx_QpoBO_mAC_vr7raYVkLBvMLlJABZYM"
PATROL_LOG_WEBHOOK = STAFF_ACTION_LOG_WEBHOOK
PATROL_WEEKLY_QUOTA_HOURS = 4.0
PATROL_LOCK_OFFSET_SECONDS = -5 * 60 * 60

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
        "log_patrol",
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
        "log_patrol",
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
        "log_patrol",
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
        "review_patrols",
        "delete_patrols",
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
        "log_patrol",
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
        "review_patrols",
        "delete_patrols",
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
        "manage_interviews",
        "application_blacklist_user",
        "revoke_application_blacklist",
        "manage_staff_access",
    ],
    "Joint Chiefs": [
        "report_access",
        "appeal_access",
        "contact_access",
        "ticket_status_access",
        "view_low_rank_panel",
        "log_patrol",
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
        "review_patrols",
        "delete_patrols",
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
        "manage_interviews",
        "application_blacklist_user",
        "revoke_application_blacklist",
        "manage_staff_access",
    ],
    "Ownership": [
        "report_access",
        "appeal_access",
        "contact_access",
        "ticket_status_access",
        "view_low_rank_panel",
        "log_patrol",
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
        "review_patrols",
        "delete_patrols",
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
        "manage_interviews",
        "application_blacklist_user",
        "revoke_application_blacklist",
        "manage_staff_access",
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


def patrol_local_ts(ts: Optional[int] = None) -> int:
    return (ts or now_ts()) + PATROL_LOCK_OFFSET_SECONDS


def patrol_week_start_ts(ts: Optional[int] = None) -> int:
    local_now = patrol_local_ts(ts)
    local_day_start = local_now - (local_now % 86400)
    # time.gmtime(...).tm_wday uses Monday=0, Sunday=6.
    days_since_monday = time.gmtime(local_now).tm_wday
    return local_day_start - (days_since_monday * 86400) - PATROL_LOCK_OFFSET_SECONDS


def patrol_week_end_ts(ts: Optional[int] = None) -> int:
    return patrol_week_start_ts(ts) + (7 * 86400)


def patrol_lock_window(ts: Optional[int] = None) -> dict[str, Any]:
    current_ts = ts or now_ts()
    local_now = patrol_local_ts(current_ts)
    local_day_start = local_now - (local_now % 86400)
    local_weekday = time.gmtime(local_now).tm_wday
    sunday_review_start_local = local_day_start + (13 * 3600)
    if local_weekday != 6:
        days_until_sunday = (6 - local_weekday) % 7
        sunday_review_start_local = local_day_start + (days_until_sunday * 86400) + (13 * 3600)
    start_ts = sunday_review_start_local - PATROL_LOCK_OFFSET_SECONDS
    end_ts = start_ts + (2 * 3600)
    locked = start_ts <= current_ts < end_ts
    return {
        "locked": locked,
        "start_ts": start_ts,
        "end_ts": end_ts,
        "next_lock_ts": start_ts if current_ts < start_ts else start_ts + (7 * 86400),
        "label": "Sunday 1 PM to 3 PM UTC-5",
    }


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


def make_staff_agent_code() -> str:
    return "CAC-" + "".join(random.choices(string.ascii_uppercase + string.digits, k=4)) + "-" + "".join(random.choices(string.ascii_uppercase + string.digits, k=4))


def make_staff_agent_alias() -> str:
    names = [
        "Jones", "Reed", "Stone", "Hayes", "Knight", "Cross", "Voss", "Ward",
        "Pierce", "Vale", "Brooks", "Frost", "Blake", "Sloan", "Wells", "Carter",
        "Mason", "Rook", "Archer", "Fox", "Hale", "Monroe", "Sterling", "Bishop",
    ]
    return f"Agent {random.choice(names)}"


def role_level(role_name: str) -> int:
    return ROLE_LEVELS.get(role_name, 0)


def rank_power(row: sqlite3.Row) -> int:
    return int(row["group_rank"] or role_level(row["role_name"]))


def role_permissions(role_name: str) -> list[str]:
    return ROLE_PERMISSIONS.get(role_name, ROLE_PERMISSIONS["Guest"])


def merged_permissions(row: sqlite3.Row) -> set[str]:
    if not row["verified"]:
        return set(role_permissions("Guest"))
    if row["staff_mode"]:
        return set(json_load(row["base_permissions"])) | set(json_load(row["extra_permissions"]))
    return set(role_permissions("Guest"))


def grantable_permissions_for(role_name: str) -> list[str]:
    actor_level = role_level(role_name)
    grantable = set()
    for name, level in ROLE_LEVELS.items():
        if level < actor_level:
            grantable |= set(role_permissions(name))
    blocked = {
        "manage_website_shutdown",
        "view_joint_chiefs_panel",
        "view_ownership_panel",
        "send_global_message",
        "manage_join_limit",
        "manage_interviews",
        "application_blacklist_user",
        "revoke_application_blacklist",
    }
    return sorted(grantable - blocked)


def is_active(until_ts: Optional[int]) -> bool:
    return until_ts is None or until_ts > now_ts()


def minutes_to_label(minutes: int) -> str:
    if minutes < 60:
        return f"{minutes} minute" + ("" if minutes == 1 else "s")
    if minutes % 1440 == 0:
        days = minutes // 1440
        return f"{days} day" + ("" if days == 1 else "s")
    if minutes % 60 == 0:
        hours = minutes // 60
        return f"{hours} hour" + ("" if hours == 1 else "s")
    hours = minutes // 60
    mins = minutes % 60
    hour_part = f"{hours} hour" + ("" if hours == 1 else "s")
    minute_part = f"{mins} minute" + ("" if mins == 1 else "s")
    return f"{hour_part} and {minute_part}"


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


def report_image_attachments(evidence_images: list[Any]) -> list[dict[str, Any]]:
    attachments: list[dict[str, Any]] = []
    total_bytes = 0
    allowed_types = {
        "image/png": ".png",
        "image/jpeg": ".jpg",
        "image/webp": ".webp",
        "image/gif": ".gif",
    }
    for index, item in enumerate(evidence_images[:5], start=1):
        if not isinstance(item, dict):
            continue
        content_type = str(item.get("type", "")).strip().lower()
        data_url = str(item.get("data_url", ""))
        if content_type not in allowed_types or not data_url.startswith("data:") or ";base64," not in data_url:
            continue
        try:
            header, encoded = data_url.split(",", 1)
            header_type = header[5:].split(";", 1)[0].lower()
            if header_type in allowed_types:
                content_type = header_type
            raw = base64.b64decode(encoded, validate=True)
        except Exception:
            continue
        if not raw or len(raw) > 8 * 1024 * 1024:
            continue
        if total_bytes + len(raw) > 23 * 1024 * 1024:
            break
        original_name = str(item.get("name", "")).strip()
        safe_name = re.sub(r"[^A-Za-z0-9_.-]+", "_", original_name).strip("._")
        if not safe_name:
            safe_name = f"evidence_{index}{allowed_types[content_type]}"
        if "." not in safe_name:
            safe_name = f"{safe_name}{allowed_types[content_type]}"
        attachments.append({
            "filename": f"{index}_{safe_name}"[:120],
            "content_type": content_type,
            "bytes": raw,
            "size": len(raw),
        })
        total_bytes += len(raw)
    return attachments


def post_report_webhook_with_images(url: str, title: str, fields: list[dict[str, str]], images: list[dict[str, Any]], footer: str = "") -> None:
    if not url or "PUT_" in url:
        return
    if not images:
        post_webhook(url, title, fields, footer)
        return
    embeds = [{
        "title": title,
        "color": 0x32D8FF,
        "fields": [{"name": x["name"], "value": x["value"], "inline": False} for x in fields],
        "footer": {"text": footer} if footer else {},
    }]
    for image in images[:5]:
        embeds.append({
            "title": image["filename"],
            "color": 0xFF2D55,
            "image": {"url": f"attachment://{image['filename']}"},
        })
    files = []
    try:
        for index, image in enumerate(images[:5]):
            files.append((
                f"files[{index}]",
                (image["filename"], io.BytesIO(image["bytes"]), image["content_type"]),
            ))
        requests.post(
            url,
            data={"payload_json": json.dumps({"embeds": embeds})},
            files=files,
            timeout=12,
        )
    except Exception:
        post_webhook(url, title, fields, footer)


def log_action(actor: sqlite3.Row, action: str, against_custom_id: str = "N/A", against_username: str = "N/A") -> None:
    post_webhook(
        ACTION_LOG_WEBHOOK,
        "Website Action Logged",
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


def log_staff_action(actor: sqlite3.Row, action: str, against_custom_id: str = "N/A", against_username: str = "N/A") -> None:
    actor_name = actor["staff_agent_alias"] if actor["staff_mode"] and actor["staff_agent_alias"] else (actor["roblox_username"] or actor["username"])
    post_webhook(
        STAFF_ACTION_LOG_WEBHOOK,
        "Staff Command Logged",
        [
            {"name": "Agent / Username", "value": actor_name},
            {"name": "Custom ID", "value": actor["custom_id"]},
            {"name": "Custom-Agent-Code", "value": actor["staff_agent_code"] or "No active code"},
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


def reset_sessions_to_guest(
    conn: sqlite3.Connection,
    custom_id: str,
    roblox_user_id: Optional[int] = None,
    forced_logout: int = 0
) -> None:
    rows = conn.execute(
        """
        SELECT session_key, custom_id
        FROM sessions
        WHERE custom_id = ?
           OR (? IS NOT NULL AND roblox_user_id = ?)
        """,
        (custom_id, roblox_user_id, roblox_user_id),
    ).fetchall()

    for item in rows:
        guest_name = f"Guest-{item['custom_id'][-4:]}"
        conn.execute(
            """
            UPDATE sessions
            SET username = ?, roblox_username = NULL, roblox_user_id = NULL,
                role_name = 'Guest', verified = 0, verification_code = NULL,
                verification_target = NULL, base_permissions = ?, extra_permissions = '[]',
                forced_logout = ?, staff_mode = 0, staff_agent_code = NULL,
                staff_agent_alias = NULL, staff_access_expires_at = NULL,
                is_group_member = 0, group_rank = NULL
            WHERE session_key = ?
            """,
            (
                guest_name,
                json_dump(role_permissions("Guest")),
                forced_logout,
                item["session_key"],
            ),
        )


def clear_stale_verification_claims(conn: sqlite3.Connection, roblox_user_id: int) -> None:
    rows = conn.execute(
        """
        SELECT session_key, custom_id
        FROM sessions
        WHERE roblox_user_id = ? AND verified = 0
        """,
        (roblox_user_id,),
    ).fetchall()

    for item in rows:
        guest_name = f"Guest-{item['custom_id'][-4:]}"
        conn.execute(
            """
            UPDATE sessions
            SET username = ?, roblox_username = NULL, roblox_user_id = NULL,
                role_name = 'Guest', verification_code = NULL,
                verification_target = NULL, base_permissions = ?, extra_permissions = '[]',
                staff_mode = 0, staff_agent_code = NULL, staff_agent_alias = NULL,
                staff_access_expires_at = NULL, is_group_member = 0, group_rank = NULL
            WHERE session_key = ?
            """,
            (
                guest_name,
                json_dump(role_permissions("Guest")),
                item["session_key"],
            ),
        )


def find_target_session(target_ref: str) -> Optional[sqlite3.Row]:
    target_ref = (target_ref or "").strip()
    if not target_ref:
        return None

    conn = db()
    row = conn.execute(
        """
        SELECT *
        FROM sessions
        WHERE custom_id = ?
           OR username = ?
           OR roblox_username = ?
        ORDER BY last_seen_at DESC
        LIMIT 1
        """,
        (target_ref, target_ref, target_ref),
    ).fetchone()
    conn.close()
    return row


def agent_display_name(row: sqlite3.Row) -> str:
    if row["staff_mode"] and row["staff_agent_alias"]:
        return row["staff_agent_alias"]
    return row["roblox_username"] or row["username"]


def active_staff_access_row(
    conn: sqlite3.Connection,
    *,
    custom_id: Optional[str] = None,
    code_value: Optional[str] = None,
) -> Optional[sqlite3.Row]:
    clauses = ["active = 1", "(expires_at IS NULL OR expires_at > ?)"]
    params: list[Any] = [now_ts()]
    if custom_id:
        clauses.append("target_custom_id = ?")
        params.append(custom_id)
    if code_value:
        clauses.append("code_value = ?")
        params.append(code_value)
    query = f"""
        SELECT *
        FROM custom_agent_codes
        WHERE {" AND ".join(clauses)}
        ORDER BY created_at DESC
        LIMIT 1
    """
    return conn.execute(query, tuple(params)).fetchone()


def clear_staff_mode_for_identity(
    conn: sqlite3.Connection,
    custom_id: str,
    roblox_user_id: Optional[int] = None,
) -> None:
    conn.execute(
        """
        UPDATE sessions
        SET staff_mode = 0, staff_agent_code = NULL, staff_agent_alias = NULL, staff_access_expires_at = NULL
        WHERE custom_id = ?
           OR (? IS NOT NULL AND roblox_user_id = ?)
        """,
        (custom_id, roblox_user_id, roblox_user_id),
    )


def expire_staff_code(
    conn: sqlite3.Connection,
    code_id: int,
    reason: str,
) -> None:
    code = conn.execute("SELECT * FROM custom_agent_codes WHERE id = ?", (code_id,)).fetchone()
    if not code:
        return
    conn.execute(
        """
        UPDATE custom_agent_codes
        SET active = 0, expired_at = ?, updated_at = ?, expired_reason = ?
        WHERE id = ?
        """,
        (now_ts(), now_ts(), reason, code_id),
    )
    if code["target_custom_id"]:
        clear_staff_mode_for_identity(conn, code["target_custom_id"], code["target_roblox_user_id"])


def sync_live_group_state(conn: sqlite3.Connection, row: sqlite3.Row) -> sqlite3.Row:
    if not row["verified"] or not row["roblox_user_id"]:
        return row
    checked_at = row["staff_group_checked_at"] or 0
    if now_ts() - checked_at < 6:
        return row

    try:
        live_role_name, live_rank = roblox_get_group_role(int(row["roblox_user_id"]))
    except Exception:
        conn.execute("UPDATE sessions SET staff_group_checked_at = ? WHERE session_key = ?", (now_ts(), row["session_key"]))
        return conn.execute("SELECT * FROM sessions WHERE session_key = ?", (row["session_key"],)).fetchone()
    live_group_member = 1 if live_role_name != "Guest" else 0
    current_group_member = int(row["is_group_member"] or 0)

    expected_permissions = json_dump(role_permissions(live_role_name))
    if (
        live_group_member != current_group_member
        or row["base_permissions"] != expected_permissions
        or (live_group_member and (live_role_name != row["role_name"] or live_rank != row["group_rank"]))
    ):
        conn.execute(
            """
            UPDATE sessions
            SET role_name = ?, base_permissions = ?, is_group_member = ?, group_rank = ?, staff_group_checked_at = ?
            WHERE custom_id = ?
            """,
            (live_role_name, expected_permissions, live_group_member, live_rank, now_ts(), row["custom_id"]),
        )
        row = conn.execute("SELECT * FROM sessions WHERE session_key = ?", (row["session_key"],)).fetchone()
    else:
        conn.execute("UPDATE sessions SET staff_group_checked_at = ? WHERE session_key = ?", (now_ts(), row["session_key"]))
        row = conn.execute("SELECT * FROM sessions WHERE session_key = ?", (row["session_key"],)).fetchone()

    grant = active_staff_access_row(conn, custom_id=row["custom_id"])
    active_code = active_staff_access_row(conn, code_value=row["staff_agent_code"]) if row["staff_agent_code"] else None
    if row["staff_mode"] and row["staff_agent_code"] and not active_code:
        clear_staff_mode_for_identity(conn, row["custom_id"], row["roblox_user_id"])
        add_event(
            row["custom_id"],
            "staff_access_revoked",
            {"message": "Your Custom-Agent-Code is no longer active, so the staff version has been closed."},
        )
        row = conn.execute("SELECT * FROM sessions WHERE session_key = ?", (row["session_key"],)).fetchone()
        grant = active_staff_access_row(conn, custom_id=row["custom_id"])

    if row["staff_mode"] and not row["is_group_member"]:
        if row["staff_agent_code"]:
            code_row = active_staff_access_row(conn, code_value=row["staff_agent_code"])
            if code_row:
                expire_staff_code(conn, int(code_row["id"]), "Roblox group membership revoked.")
        clear_staff_mode_for_identity(conn, row["custom_id"], row["roblox_user_id"])
        add_event(
            row["custom_id"],
            "staff_access_revoked",
            {"message": "Your staff website access has been revoked because you are no longer in the Roblox group."},
        )
        row = conn.execute("SELECT * FROM sessions WHERE session_key = ?", (row["session_key"],)).fetchone()

    if row["staff_mode"] and row["staff_access_expires_at"] and row["staff_access_expires_at"] <= now_ts():
        if row["staff_agent_code"]:
            code_row = active_staff_access_row(conn, code_value=row["staff_agent_code"])
            if code_row:
                expire_staff_code(conn, int(code_row["id"]), "Custom-Agent-Code expired.")
        clear_staff_mode_for_identity(conn, row["custom_id"], row["roblox_user_id"])
        row = conn.execute("SELECT * FROM sessions WHERE session_key = ?", (row["session_key"],)).fetchone()
    return row


def issue_staff_code(
    conn: sqlite3.Connection,
    actor: sqlite3.Row,
    target: Optional[sqlite3.Row],
    expires_at: Optional[int],
) -> sqlite3.Row:
    code_value = make_staff_agent_code()
    while conn.execute("SELECT 1 FROM custom_agent_codes WHERE code_value = ?", (code_value,)).fetchone():
        code_value = make_staff_agent_code()
    created_at = now_ts()
    conn.execute(
        """
        INSERT INTO custom_agent_codes (
            code_value, target_custom_id, target_username, target_roblox_user_id,
            target_roblox_username, target_role_name, target_group_rank, agent_alias,
            granted_by_custom_id, granted_by_username, expires_at, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            code_value,
            target["custom_id"] if target else None,
            (target["roblox_username"] or target["username"]) if target else None,
            target["roblox_user_id"] if target else None,
            target["roblox_username"] if target else None,
            target["role_name"] if target else None,
            target["group_rank"] if target else None,
            make_staff_agent_alias(),
            actor["custom_id"],
            actor["roblox_username"] or actor["username"],
            expires_at,
            created_at,
            created_at,
        ),
    )
    return conn.execute("SELECT * FROM custom_agent_codes WHERE code_value = ?", (code_value,)).fetchone()


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


def parse_staff_access_duration(raw: str) -> tuple[Optional[int], str]:
    value = (raw or "").strip()
    if not value:
        raise HTTPException(status_code=400, detail="Enter a number of minutes or PERMANENT.")
    if value.upper() == "PERMANENT":
        return None, "Permanent"
    if not re.fullmatch(r"\d+", value):
        raise HTTPException(status_code=400, detail="Enter a number of minutes or PERMANENT.")
    minutes = int(value)
    if minutes <= 0:
        raise HTTPException(status_code=400, detail="Minutes must be above 0.")
    return now_ts() + minutes * 60, minutes_to_label(minutes)


def parse_duration(raw: str) -> tuple[Optional[int], str]:
    raw = raw.strip().upper()
    if raw == "PERMANENTLY":
        return None, "Permanent"
    match = re.fullmatch(r"(\d+)(MO|[SMHDW])", raw)
    if not match:
        raise HTTPException(status_code=400, detail="Use S, M, H, D, W, MO, or PERMANENTLY. Example: 30S, 15M, 2H, 7D, 2W, 1MO.")
    amount = int(match.group(1))
    unit = match.group(2)
    if unit == "S":
        return now_ts() + amount, f"{amount} second" + ("" if amount == 1 else "s")
    if unit == "M":
        return now_ts() + amount * 60, f"{amount} minute" + ("" if amount == 1 else "s")
    if unit == "H":
        return now_ts() + amount * 3600, f"{amount} hour" + ("" if amount == 1 else "s")
    if unit == "D":
        return now_ts() + amount * 86400, f"{amount} day" + ("" if amount == 1 else "s")
    if unit == "W":
        return now_ts() + amount * 7 * 86400, f"{amount} week" + ("" if amount == 1 else "s")
    return now_ts() + amount * 30 * 86400, f"{amount} month" + ("" if amount == 1 else "s")


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
    try:
        res = requests.post(
            "https://users.roblox.com/v1/usernames/users",
            json={"usernames": [username], "excludeBannedUsers": False},
            timeout=10,
        )
        res.raise_for_status()
    except requests.RequestException:
        raise HTTPException(status_code=400, detail="Roblox lookup failed. Please retry in a moment.")
    data = res.json().get("data", [])
    if not data:
        raise HTTPException(status_code=400, detail="Roblox account not found.")
    item = data[0]
    return int(item["id"]), item["name"]


def roblox_user_by_id(user_id: int) -> tuple[int, str]:
    try:
        res = requests.get(f"https://users.roblox.com/v1/users/{user_id}", timeout=10)
        if res.status_code == 404:
            raise HTTPException(status_code=400, detail="Roblox account not found.")
        res.raise_for_status()
    except HTTPException:
        raise
    except requests.RequestException:
        raise HTTPException(status_code=400, detail="Roblox lookup failed. Please retry in a moment.")
    data = res.json()
    return user_id, data["name"]


def roblox_lookup_profile_link(link: str) -> tuple[int, str]:
    link = unquote(link.strip())
    if not link:
        raise HTTPException(status_code=400, detail="Please paste your Roblox profile link.")

    if re.fullmatch(r"\d+", link):
        return roblox_user_by_id(int(link))

    if "roblox.com" not in link.lower() and "/" not in link:
        return roblox_lookup_username(link)

    parsed = urlparse(link if re.match(r"^https?://", link, re.I) else f"https://{link}")
    query = parse_qs(parsed.query)
    username_values = query.get("username") or query.get("Username")
    if username_values and username_values[0].strip():
        return roblox_lookup_username(username_values[0].strip())

    at_username_match = re.search(r"(?:^|/)@([A-Za-z0-9_]{3,20})(?:[/?#]|$)", parsed.path, re.I)
    if at_username_match:
        return roblox_lookup_username(at_username_match.group(1))

    if "/share" in parsed.path.lower():
        raise HTTPException(
            status_code=400,
            detail="Please paste the direct Roblox profile URL, username, or user ID. Roblox share links cannot generate verification codes.",
        )

    any_user_id_match = re.search(r"(?:userId|userid|id)=?(\d{3,})|/(\d{3,})(?:[/?#]|$)", link, re.I)
    if any_user_id_match:
        return roblox_user_by_id(int(any_user_id_match.group(1) or any_user_id_match.group(2)))

    username_match = re.search(r"/users/profile\?username=([^/?&#]+)", link, re.I)
    if username_match:
        return roblox_lookup_username(username_match.group(1))
    alt_username_match = re.search(r"[?&]username=([^/?&#]+)", link, re.I)
    if alt_username_match:
        return roblox_lookup_username(alt_username_match.group(1))
    direct_match = re.search(r"/users/(\d+)/profile", link, re.I)
    if direct_match:
        return roblox_user_by_id(int(direct_match.group(1)))
    users_segment_match = re.search(r"/users/(\d+)", link, re.I)
    if users_segment_match:
        return roblox_user_by_id(int(users_segment_match.group(1)))
    raise HTTPException(status_code=400, detail="Invalid Roblox profile link. Use a Roblox profile URL, username, or user ID.")


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
    reason = "Permission abuse against a member of the same rank or higher."

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
        {"message": "You have been temporarily suspended for permission abuse." if suspended else "Do not attempt to permission abuse otherwise there will be heavy consequences."},
    )
    log_permission_abuse(actor, target, action_name, suspended)


def require_not_higher_rank(actor: sqlite3.Row, target: sqlite3.Row, action_name: str) -> None:
    if rank_power(target) >= rank_power(actor):
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


def interviews_open() -> bool:
    conn = db()
    row = conn.execute("SELECT is_open FROM interview_settings WHERE id = 1").fetchone()
    conn.close()
    return bool(row["is_open"]) if row else False


def application_blacklist_for(custom_id: str) -> Optional[sqlite3.Row]:
    conn = db()
    row = conn.execute(
        "SELECT * FROM application_blacklists WHERE target_custom_id = ? ORDER BY created_at DESC LIMIT 1",
        (custom_id,),
    ).fetchone()
    conn.close()
    if row and is_active(row["until_ts"]):
        return row
    return None


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


def ensure_column(conn: sqlite3.Connection, table_name: str, column_name: str, ddl: str) -> None:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    existing = {row[1] for row in rows}
    if column_name not in existing:
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {ddl}")


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
    ensure_column(conn, "sessions", "visitor_key", "visitor_key TEXT")
    ensure_column(conn, "sessions", "roblox_username", "roblox_username TEXT")
    ensure_column(conn, "sessions", "roblox_user_id", "roblox_user_id INTEGER")
    ensure_column(conn, "sessions", "role_name", "role_name TEXT NOT NULL DEFAULT 'Guest'")
    ensure_column(conn, "sessions", "verified", "verified INTEGER NOT NULL DEFAULT 0")
    ensure_column(conn, "sessions", "verification_code", "verification_code TEXT")
    ensure_column(conn, "sessions", "verification_target", "verification_target TEXT")
    ensure_column(conn, "sessions", "base_permissions", "base_permissions TEXT NOT NULL DEFAULT '[]'")
    ensure_column(conn, "sessions", "extra_permissions", "extra_permissions TEXT NOT NULL DEFAULT '[]'")
    ensure_column(conn, "sessions", "forced_logout", "forced_logout INTEGER NOT NULL DEFAULT 0")
    ensure_column(conn, "sessions", "suspended", "suspended INTEGER NOT NULL DEFAULT 0")
    ensure_column(conn, "sessions", "suspended_reason", "suspended_reason TEXT")
    ensure_column(conn, "sessions", "created_at", "created_at INTEGER NOT NULL DEFAULT 0")
    ensure_column(conn, "sessions", "last_seen_at", "last_seen_at INTEGER NOT NULL DEFAULT 0")
    ensure_column(conn, "sessions", "staff_mode", "staff_mode INTEGER NOT NULL DEFAULT 0")
    ensure_column(conn, "sessions", "staff_agent_code", "staff_agent_code TEXT")
    ensure_column(conn, "sessions", "staff_agent_alias", "staff_agent_alias TEXT")
    ensure_column(conn, "sessions", "staff_access_expires_at", "staff_access_expires_at INTEGER")
    ensure_column(conn, "sessions", "is_group_member", "is_group_member INTEGER NOT NULL DEFAULT 0")
    ensure_column(conn, "sessions", "group_rank", "group_rank INTEGER")
    ensure_column(conn, "sessions", "staff_group_checked_at", "staff_group_checked_at INTEGER NOT NULL DEFAULT 0")

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
        CREATE TABLE IF NOT EXISTS application_blacklists (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            target_custom_id TEXT NOT NULL,
            target_username TEXT NOT NULL,
            reason TEXT NOT NULL,
            time_label TEXT NOT NULL,
            until_ts INTEGER NOT NULL,
            notify_user INTEGER NOT NULL DEFAULT 0,
            created_by_custom_id TEXT NOT NULL,
            created_by_username TEXT NOT NULL,
            created_at INTEGER NOT NULL
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
        CREATE TABLE IF NOT EXISTS interview_settings (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            is_open INTEGER NOT NULL DEFAULT 0,
            updated_by_custom_id TEXT NOT NULL DEFAULT 'SYSTEM',
            updated_by_username TEXT NOT NULL DEFAULT 'System',
            updated_at INTEGER NOT NULL DEFAULT 0
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS interview_applications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            applicant_custom_id TEXT NOT NULL,
            applicant_username TEXT NOT NULL,
            roblox_user_id INTEGER,
            answers_json TEXT NOT NULL,
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
    cur.execute("""
        CREATE TABLE IF NOT EXISTS custom_agent_codes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code_value TEXT NOT NULL UNIQUE,
            target_custom_id TEXT,
            target_username TEXT,
            target_roblox_user_id INTEGER,
            target_roblox_username TEXT,
            target_role_name TEXT,
            target_group_rank INTEGER,
            agent_alias TEXT,
            granted_by_custom_id TEXT NOT NULL,
            granted_by_username TEXT NOT NULL,
            expires_at INTEGER,
            active INTEGER NOT NULL DEFAULT 1,
            linked_at INTEGER,
            expired_at INTEGER,
            expired_reason TEXT,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        )
    """)
    ensure_column(conn, "custom_agent_codes", "target_group_rank", "target_group_rank INTEGER")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS patrol_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            custom_id TEXT NOT NULL,
            username TEXT NOT NULL,
            roblox_username TEXT,
            role_name TEXT NOT NULL,
            group_rank INTEGER,
            logged_hours REAL NOT NULL,
            patrol_notes TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'PENDING',
            denial_reason TEXT,
            reviewed_by_custom_id TEXT,
            reviewed_by_username TEXT,
            reviewed_at INTEGER,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        )
    """)
    ensure_column(conn, "patrol_logs", "group_rank", "group_rank INTEGER")
    ensure_column(conn, "patrol_logs", "patrol_notes", "patrol_notes TEXT NOT NULL DEFAULT ''")
    ensure_column(conn, "patrol_logs", "denial_reason", "denial_reason TEXT")
    ensure_column(conn, "patrol_logs", "reviewed_by_custom_id", "reviewed_by_custom_id TEXT")
    ensure_column(conn, "patrol_logs", "reviewed_by_username", "reviewed_by_username TEXT")
    ensure_column(conn, "patrol_logs", "reviewed_at", "reviewed_at INTEGER")
    cur.execute("""
        INSERT OR IGNORE INTO interview_settings (
            id, is_open, updated_by_custom_id, updated_by_username, updated_at
        ) VALUES (1, 0, 'SYSTEM', 'System', 0)
    """)
    conn.commit()
    conn.close()


setup_db()


def session_row(session_key: str) -> sqlite3.Row:
    conn = db()
    row = conn.execute("SELECT * FROM sessions WHERE session_key = ?", (session_key,)).fetchone()
    if row:
        conn.execute("UPDATE sessions SET last_seen_at = ? WHERE session_key = ?", (now_ts(), session_key))
        row = sync_live_group_state(conn, row)
        conn.commit()
        row = conn.execute("SELECT * FROM sessions WHERE session_key = ?", (session_key,)).fetchone()
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

    conn = db()
    existing = conn.execute(
        """
        SELECT * FROM sessions
        WHERE visitor_key = ?
        ORDER BY last_seen_at DESC
        LIMIT 1
        """,
        (visitor_key,),
    ).fetchone()

    session_key = make_session_key()
    custom_id = existing["custom_id"] if existing else make_custom_id()
    username = (existing["username"] if existing else f"Guest-{custom_id[-4:]}")
    created_at = existing["created_at"] if existing else now_ts()

    conn.execute(
        """
        INSERT INTO sessions (
            session_key, custom_id, visitor_key, username, role_name, base_permissions,
            extra_permissions, created_at, last_seen_at
        ) VALUES (?, ?, ?, ?, 'Guest', ?, '[]', ?, ?)
        """,
        (session_key, custom_id, visitor_key, username, json_dump(role_permissions("Guest")), created_at, now_ts()),
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
    app_blacklist = conn.execute(
        "SELECT * FROM application_blacklists WHERE target_custom_id = ? ORDER BY created_at DESC LIMIT 1",
        (row["custom_id"],),
    ).fetchone()
    termination = conn.execute(
        "SELECT * FROM terminations WHERE custom_id = ? OR username = ?",
        (row["custom_id"], row["roblox_username"] or row["username"]),
    ).fetchone()
    join_limit = conn.execute("SELECT * FROM join_limits WHERE active = 1 ORDER BY id DESC LIMIT 1").fetchone()
    staff_grant = active_staff_access_row(conn, custom_id=row["custom_id"])
    conn.close()
    inbox = inbox_for(row["custom_id"])
    app_blacklisted = bool(app_blacklist and is_active(app_blacklist["until_ts"]))
    effective_permissions = sorted(merged_permissions(row))

    staff_access_allowed = bool(row["verified"]) and (
        bool(row["is_group_member"]) or role_level(row["role_name"]) >= role_level("Headquarters")
    )

    return {
        "custom_id": row["custom_id"],
        "username": row["username"],
        "roblox_username": row["roblox_username"],
        "roblox_user_id": row["roblox_user_id"],
        "avatar_url": public_avatar_url(row["roblox_user_id"]),
        "role_name": row["role_name"],
        "group_rank": row["group_rank"],
        "verified": bool(row["verified"]),
        "in_roblox_group": bool(row["is_group_member"]),
        "forced_logout": bool(row["forced_logout"]),
        "suspended": bool(row["suspended"]),
        "suspended_reason": row["suspended_reason"],
        "base_permissions": json_load(row["base_permissions"]),
        "extra_permissions": json_load(row["extra_permissions"]),
        "effective_permissions": effective_permissions,
        "grantable_permissions": grantable_permissions_for(row["role_name"]),
        "staff_mode": bool(row["staff_mode"]),
        "staff_agent_code": row["staff_agent_code"],
        "staff_agent_alias": row["staff_agent_alias"],
        "staff_access_allowed": staff_access_allowed,
        "staff_access_expires_at": staff_grant["expires_at"] if staff_grant else row["staff_access_expires_at"],
        "messages": active_messages_for(row["custom_id"]),
        "events": pending_events_for(row["custom_id"]),
        "inbox_unread_count": inbox["unread_count"],
        "report_blacklisted": bool(blacklist and is_active(blacklist["until_ts"])),
        "application_blacklisted": app_blacklisted,
        "application_blacklist_reason": app_blacklist["reason"] if app_blacklisted else None,
        "application_blacklist_time_label": app_blacklist["time_label"] if app_blacklisted else None,
        "terminated": bool(termination and is_active(termination["until_ts"])),
        "terminated_reason": termination["reason"] if termination and is_active(termination["until_ts"]) else None,
        "terminated_time_label": termination["time_label"] if termination and is_active(termination["until_ts"]) else None,
        "shutdown": shutdown_block_for(row),
        "join_limit": dict(join_limit) if join_limit else None,
        "interviews_open": interviews_open(),
        "patrol_lock": patrol_lock_window(),
    }


def patrol_public(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": row["id"],
        "custom_id": row["custom_id"],
        "username": row["username"],
        "roblox_username": row["roblox_username"],
        "role_name": row["role_name"],
        "group_rank": row["group_rank"],
        "logged_hours": row["logged_hours"],
        "patrol_notes": row["patrol_notes"],
        "status": row["status"],
        "denial_reason": row["denial_reason"],
        "reviewed_by_custom_id": row["reviewed_by_custom_id"],
        "reviewed_by_username": row["reviewed_by_username"],
        "reviewed_at": row["reviewed_at"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


@app.get("/api/patrols")
def patrols(session_key: str):
    actor = require_permission(session_key, "log_patrol")
    can_review = "review_patrols" in merged_permissions(actor)
    conn = db()
    if can_review:
        rows = conn.execute("SELECT * FROM patrol_logs ORDER BY created_at DESC LIMIT 250").fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM patrol_logs WHERE custom_id = ? ORDER BY created_at DESC LIMIT 100",
            (actor["custom_id"],),
        ).fetchall()
    pending_count = conn.execute("SELECT COUNT(*) AS c FROM patrol_logs WHERE status = 'PENDING'").fetchone()["c"]
    conn.close()
    return {
        "items": [patrol_public(row) for row in rows],
        "pending_count": pending_count,
        "lock": patrol_lock_window(),
        "can_review": can_review,
        "can_delete": "delete_patrols" in merged_permissions(actor),
    }


@app.post("/api/patrols/log")
def log_patrol(payload: dict[str, Any]):
    actor = require_permission(payload["session_key"], "log_patrol")
    lock = patrol_lock_window()
    if lock["locked"]:
        raise HTTPException(status_code=423, detail="Patrol logging is locked while HR+ reviews this week's patrols.")
    try:
        logged_hours = float(payload.get("logged_hours", 0))
    except Exception:
        logged_hours = 0
    if logged_hours <= 0 or logged_hours > 24:
        raise HTTPException(status_code=400, detail="Logged hours must be between 0 and 24.")
    notes = str(payload.get("patrol_notes", "")).strip()[:1000]
    patrol_images = payload.get("patrol_images") or []
    if not isinstance(patrol_images, list):
        patrol_images = []
    patrol_attachments = report_image_attachments(patrol_images)
    patrol_image_names = [f"{item['filename']} ({round(item['size'] / 1024)} KB)" for item in patrol_attachments]
    created = now_ts()
    conn = db()
    cur = conn.execute(
        """
        INSERT INTO patrol_logs (
            custom_id, username, roblox_username, role_name, group_rank,
            logged_hours, patrol_notes, status, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 'PENDING', ?, ?)
        """,
        (
            actor["custom_id"],
            actor["username"],
            actor["roblox_username"],
            actor["role_name"],
            actor["group_rank"],
            logged_hours,
            notes,
            created,
            created,
        ),
    )
    patrol_id = cur.lastrowid
    conn.commit()
    conn.close()
    webhook_fields = [
        {"name": "Username", "value": actor["roblox_username"] or actor["username"]},
        {"name": "Custom ID", "value": actor["custom_id"]},
        {"name": "Current Rank", "value": actor["role_name"]},
        {"name": "Rank ID", "value": str(actor["group_rank"] or "N/A")},
        {"name": "Logged Hours", "value": str(logged_hours)},
        {"name": "Status", "value": "PENDING"},
        {"name": "Notes", "value": notes or "No notes provided."},
        {"name": "Date", "value": human_time(created)},
    ]
    if patrol_image_names:
        webhook_fields.append({
            "name": "Imported Patrol Picture Proof",
            "value": "\n".join(patrol_image_names)[:1024],
        })
    post_report_webhook_with_images(
        PATROL_LOG_WEBHOOK,
        "Patrol Logged - Pending Review",
        webhook_fields,
        patrol_attachments,
        footer=actor["custom_id"],
    )
    log_staff_action(actor, f"Logged Patrol #{patrol_id}")
    return {"success": True, "patrol_id": patrol_id}


@app.post("/api/patrols/review")
def review_patrol(payload: dict[str, Any]):
    actor = require_permission(payload["session_key"], "review_patrols")
    patrol_id = int(payload.get("patrol_id") or 0)
    status = str(payload.get("status", "")).strip().upper()
    denial_reason = str(payload.get("denial_reason", "")).strip()
    if status not in {"APPROVED", "DENIED"}:
        raise HTTPException(status_code=400, detail="Patrol status must be Approved or Denied.")
    if status == "DENIED" and not denial_reason:
        raise HTTPException(status_code=400, detail="A denial reason is required.")
    conn = db()
    row = conn.execute("SELECT * FROM patrol_logs WHERE id = ?", (patrol_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="Patrol log not found.")
    reviewed_at = now_ts()
    conn.execute(
        """
        UPDATE patrol_logs
        SET status = ?, denial_reason = ?, reviewed_by_custom_id = ?,
            reviewed_by_username = ?, reviewed_at = ?, updated_at = ?
        WHERE id = ?
        """,
        (
            status,
            denial_reason if status == "DENIED" else None,
            actor["custom_id"],
            actor["roblox_username"] or actor["username"],
            reviewed_at,
            reviewed_at,
            patrol_id,
        ),
    )
    conn.commit()
    conn.close()
    if status == "DENIED":
        add_inbox_item(
            row["custom_id"],
            "Patrol Log Denied",
            f"Your patrol log for {row['logged_hours']} hour(s) has been denied. Reason: {denial_reason}",
            "patrol",
        )
    post_webhook(
        PATROL_LOG_WEBHOOK,
        f"Patrol {status.title()}",
        [
            {"name": "Patrol ID", "value": str(patrol_id)},
            {"name": "User", "value": row["roblox_username"] or row["username"]},
            {"name": "Logged Hours", "value": str(row["logged_hours"])},
            {"name": "Reviewed By", "value": actor["roblox_username"] or actor["username"]},
            {"name": "Status", "value": status},
            {"name": "Denial Reason", "value": denial_reason or "N/A"},
        ],
        footer=actor["custom_id"],
    )
    log_staff_action(actor, f"Reviewed Patrol #{patrol_id} as {status}", row["custom_id"], row["roblox_username"] or row["username"])
    return {"success": True}


@app.post("/api/patrols/delete")
def delete_patrol(payload: dict[str, Any]):
    actor = require_permission(payload["session_key"], "delete_patrols")
    patrol_id = int(payload.get("patrol_id") or 0)
    conn = db()
    row = conn.execute("SELECT * FROM patrol_logs WHERE id = ?", (patrol_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="Patrol log not found.")
    conn.execute("DELETE FROM patrol_logs WHERE id = ?", (patrol_id,))
    conn.commit()
    conn.close()
    log_staff_action(actor, f"Deleted Patrol #{patrol_id}", row["custom_id"], row["roblox_username"] or row["username"])
    return {"success": True}


@app.get("/api/patrols/weekly")
def patrol_weekly(session_key: str):
    actor = require_permission(session_key, "review_patrols")
    start_ts = patrol_week_start_ts()
    end_ts = patrol_week_end_ts()
    conn = db()
    rows = conn.execute(
        """
        SELECT * FROM patrol_logs
        WHERE created_at >= ? AND created_at < ?
        ORDER BY created_at DESC
        """,
        (start_ts, end_ts),
    ).fetchall()
    staff_rows = conn.execute(
        """
        SELECT custom_id, username, roblox_username, role_name
        FROM sessions
        WHERE verified = 1 AND role_name != 'Guest'
        ORDER BY last_seen_at DESC
        """
    ).fetchall()
    conn.close()
    approved = [row for row in rows if row["status"] == "APPROVED"]
    totals: dict[str, dict[str, Any]] = {}
    for row in staff_rows:
        if row["custom_id"] not in totals:
            totals[row["custom_id"]] = {
                "custom_id": row["custom_id"],
                "username": row["roblox_username"] or row["username"],
                "role_name": row["role_name"],
                "hours": 0.0,
            }
    for row in approved:
        key = row["custom_id"]
        if key not in totals:
            totals[key] = {
                "custom_id": row["custom_id"],
                "username": row["roblox_username"] or row["username"],
                "role_name": row["role_name"],
                "hours": 0.0,
            }
        totals[key]["hours"] += float(row["logged_hours"])
    ranked = sorted(totals.values(), key=lambda item: item["hours"], reverse=True)
    quota_completed = [item for item in ranked if item["hours"] >= PATROL_WEEKLY_QUOTA_HOURS]
    quota_missing = [item for item in ranked if item["hours"] < PATROL_WEEKLY_QUOTA_HOURS]
    return {
        "week_start_ts": start_ts,
        "week_end_ts": end_ts,
        "quota_hours": PATROL_WEEKLY_QUOTA_HOURS,
        "total_hours": round(sum(float(row["logged_hours"]) for row in approved), 2),
        "most_active": ranked[0] if ranked else None,
        "least_active": ranked[-1] if ranked else None,
        "quota_completed": quota_completed,
        "quota_missing": quota_missing,
        "items": [patrol_public(row) for row in rows],
        "lock": patrol_lock_window(),
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
    seen = set()
    items = []
    for row in rows:
        key = row["roblox_username"] or row["custom_id"]
        if key in seen:
            continue
        seen.add(key)
        item = dict(row)
        item["avatar_url"] = public_avatar_url(row["roblox_user_id"])
        items.append(item)
    return {"items": items}


@app.post("/api/logout")
def logout(payload: dict[str, Any]):
    row = session_row(payload["session_key"])
    conn = db()
    reset_sessions_to_guest(conn, row["custom_id"], row["roblox_user_id"], forced_logout=0)
    conn.commit()
    conn.close()
    log_action(row, "Logged Out")
    return {"success": True}


@app.post("/api/verification/start")
def verification_start(payload: dict[str, Any]):
    row = session_row(payload["session_key"])
    profile_link = payload.get("profile_link", "").strip()
    user_id, username = roblox_lookup_profile_link(profile_link)

    conn = db()
    clear_stale_verification_claims(conn, user_id)
    existing = conn.execute(
        """
        SELECT * FROM sessions
        WHERE verified = 1 AND roblox_user_id = ? AND custom_id != ?
        LIMIT 1
        """,
        (user_id, row["custom_id"]),
    ).fetchone()
    if existing:
        if now_ts() - int(existing["last_seen_at"] or 0) <= 12:
            conn.close()
            raise HTTPException(status_code=400, detail="This account is already verified.")
        reset_sessions_to_guest(conn, existing["custom_id"], user_id, forced_logout=0)

    code = make_verification_code()
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

    role_name, rank = roblox_get_group_role(int(row["roblox_user_id"]))
    username = row["roblox_username"] or row["username"]

    conn = db()
    conn.execute(
        """
        UPDATE sessions
        SET verified = 1, username = ?, role_name = ?, base_permissions = ?,
            verification_code = NULL, forced_logout = 0, is_group_member = ?, group_rank = ?, staff_group_checked_at = ?
        WHERE session_key = ?
        """,
        (username, role_name, json_dump(role_permissions(role_name)), 1 if role_name != "Guest" else 0, rank, now_ts(), row["session_key"]),
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
    seen = set()
    users = []
    for row in rows:
        key = row["roblox_username"] or row["custom_id"]
        if key in seen:
            continue
        seen.add(key)
        users.append(dict(row))
    return {"users": users}


@app.get("/api/users-database")
def users_database(session_key: str):
    actor = require_permission(session_key, "check_information")
    conn = db()
    rows = conn.execute(
        """
        SELECT custom_id, username, roblox_username, visitor_key, role_name, created_at, last_seen_at
        FROM sessions
        ORDER BY last_seen_at DESC
        """
    ).fetchall()
    conn.close()

    seen = set()
    items = []
    for row in rows:
        key = row["roblox_username"] or row["visitor_key"] or row["custom_id"]
        if key in seen:
            continue
        seen.add(key)
        items.append(dict(row))

    log_staff_action(actor, "Viewed Website Users Database")
    return {"items": items}


@app.post("/api/force-logout")
def force_logout(payload: dict[str, Any]):
    actor = require_permission(payload["session_key"], "force_logout")
    target_custom_id = payload.get("target_custom_id", "").strip()
    target = find_target_session(target_custom_id)
    if not target:
        raise HTTPException(status_code=404, detail="User not found.")

    require_not_higher_rank(actor, target, "Force Logout")

    conn = db()
    reset_sessions_to_guest(conn, target["custom_id"], target["roblox_user_id"], forced_logout=1)
    conn.commit()
    conn.close()

    add_event(target["custom_id"], "force_logout", {"message": "You have been logged out of your account and thus need to verify again!"})
    add_inbox_item(target["custom_id"], "Internal Affairs System", "You have been logged out of your account and thus need to verify again!")
    log_staff_action(actor, "Force Logout", target["custom_id"], target["roblox_username"] or target["username"])
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
        if rank_power(target) >= rank_power(actor):
            handle_permission_abuse(actor, target, "Bulk Force Logout")
            continue
        reset_sessions_to_guest(conn, target["custom_id"], target["roblox_user_id"], forced_logout=1)
        done.append(target)
    conn.commit()
    conn.close()

    for target in done:
        add_event(target["custom_id"], "force_logout", {"message": f"You have been logged out of your account. Reason: {reason}"})
        add_inbox_item(target["custom_id"], "Internal Affairs System", f"You have been logged out of your account. Reason: {reason}")
    log_staff_action(actor, f"Bulk Force Logout ({len(done)})")
    return {"success": True, "count": len(done)}


@app.post("/api/report")
def submit_report(payload: dict[str, Any]):
    row = session_row(payload["session_key"])
    evidence_images = payload.get("evidence_images") or []
    if not isinstance(evidence_images, list):
        evidence_images = []
    evidence_attachments = report_image_attachments(evidence_images)
    evidence_image_names = []
    for item in evidence_attachments:
        evidence_image_names.append(f"{item['filename']} ({round(item['size'] / 1024)} KB)")
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

    webhook_fields = [
        {"name": "Username of the person being reported", "value": payload.get("target_username", "")},
        {"name": "Division", "value": payload.get("division", "")},
        {"name": "Urgency Level", "value": payload.get("urgency", "")},
        {"name": "Reason of your report", "value": payload.get("reason", "")},
        {"name": "Evidence", "value": payload.get("evidence", "") or "Picture evidence attached."},
    ]
    if evidence_image_names:
        webhook_fields.append({
            "name": "Imported Picture Evidence",
            "value": "\n".join(evidence_image_names)[:1024],
        })
    post_report_webhook_with_images(
        REPORT_LOG_WEBHOOK,
        "Divisional Report Logged & Ready For Review",
        webhook_fields,
        evidence_attachments,
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
    log_staff_action(actor, "Report Blacklist", target["custom_id"], target_username)
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
    log_staff_action(actor, "Revoke Report Blacklist", row["custom_id"] or "N/A", target_username)
    return {"success": True}


@app.post("/api/application-blacklist")
def application_blacklist(payload: dict[str, Any]):
    actor = require_permission(payload["session_key"], "application_blacklist_user")
    target_ref = payload.get("target_ref", "").strip()
    minutes = int(payload.get("minutes", 0))
    reason = payload.get("reason", "").strip()
    notify_user = bool(payload.get("notify_user"))

    if minutes <= 0:
        raise HTTPException(status_code=400, detail="Time must be above 0 minutes.")
    if not reason:
        raise HTTPException(status_code=400, detail="Reason is required.")

    target = find_target_session(target_ref)
    if not target:
        raise HTTPException(status_code=404, detail="User not found.")

    target_username = target["roblox_username"] or target["username"]
    until_ts = now_ts() + minutes * 60
    time_label = minutes_to_label(minutes)

    conn = db()
    conn.execute(
        """
        INSERT INTO application_blacklists (
            target_custom_id, target_username, reason, time_label, until_ts,
            notify_user, created_by_custom_id, created_by_username, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            target["custom_id"],
            target_username,
            reason,
            time_label,
            until_ts,
            1 if notify_user else 0,
            actor["custom_id"],
            actor["roblox_username"] or actor["username"],
            now_ts(),
        ),
    )
    conn.commit()
    conn.close()

    if notify_user:
        add_inbox_item(
            target["custom_id"],
            "Internal Affairs System",
            f"You have been blacklisted from applications for {time_label}. Reason: {reason}"
        )
        add_event(target["custom_id"], "application_blacklisted", {"message": f"You have been blacklisted from applications for {time_label}."})

    log_staff_action(actor, "Application Blacklist", target["custom_id"], target_username)
    return {"success": True}


@app.get("/api/application-blacklists")
def list_application_blacklists(session_key: str):
    require_permission(session_key, "revoke_application_blacklist")
    conn = db()
    rows = conn.execute(
        """
        SELECT target_custom_id, target_username, reason, time_label, until_ts
        FROM application_blacklists
        ORDER BY created_at DESC
        """
    ).fetchall()
    conn.close()

    seen = set()
    items = []
    for row in rows:
        if row["target_custom_id"] in seen:
            continue
        if row["until_ts"] <= now_ts():
            continue
        seen.add(row["target_custom_id"])
        items.append(dict(row))
    return {"items": items}


@app.post("/api/revoke-application-blacklist")
def revoke_application_blacklist(payload: dict[str, Any]):
    actor = require_permission(payload["session_key"], "revoke_application_blacklist")
    target_custom_id = payload.get("target_custom_id", "").strip()
    conn = db()
    row = conn.execute(
        """
        SELECT * FROM application_blacklists
        WHERE target_custom_id = ?
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (target_custom_id,),
    ).fetchone()
    if not row or row["until_ts"] <= now_ts():
        conn.close()
        raise HTTPException(status_code=404, detail="No active application blacklist found.")

    conn.execute("UPDATE application_blacklists SET until_ts = ? WHERE target_custom_id = ?", (now_ts() - 1, target_custom_id))
    conn.commit()
    conn.close()

    add_inbox_item(target_custom_id, "Internal Affairs System", "Your application blacklist has been revoked.")
    add_event(target_custom_id, "application_blacklist_revoked", {"message": "Your application blacklist has been revoked."})
    log_staff_action(actor, "Revoke Application Blacklist", target_custom_id, row["target_username"])
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

    if until_ts is None:
        termination_message = f"You have been permanently terminated from the website. Reason: {reason}"
    else:
        termination_message = f"You have been terminated for {label}. Reason: {reason}"

    add_inbox_item(target["custom_id"], "Internal Affairs System", termination_message)
    add_event(target["custom_id"], "terminated", {"reason": reason, "time_label": label})
    log_staff_action(actor, "Terminate User", target["custom_id"], target_username)
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
    log_staff_action(actor, "Revoke Terminate", row["custom_id"] or "N/A", target_username)
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
    log_staff_action(actor, "Give Permissions", target_custom_id, target["roblox_username"] or target["username"])
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
    log_staff_action(actor, "Remove Permissions", target_custom_id, target["roblox_username"] or target["username"])
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
    log_staff_action(actor, "Send Message In Inbox", target["custom_id"], target["roblox_username"] or target["username"])
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

    log_staff_action(actor, "Send Global Message", "GLOBAL", "All Users")
    return {"success": True}


@app.post("/api/check-information")
def check_information(payload: dict[str, Any]):
    actor = require_permission(payload["session_key"], "check_information")
    target_custom_id = payload.get("target_custom_id", "").strip()
    conn = db()
    target = conn.execute("SELECT * FROM sessions WHERE custom_id = ?", (target_custom_id,)).fetchone()
    if not target:
        conn.close()
        raise HTTPException(status_code=404, detail="No user found.")
    blacklist = conn.execute("SELECT * FROM report_blacklists WHERE custom_id = ?", (target_custom_id,)).fetchone()
    termination = conn.execute("SELECT * FROM terminations WHERE custom_id = ?", (target_custom_id,)).fetchone()
    conn.close()

    log_staff_action(actor, "Check Information", target["custom_id"], target["roblox_username"] or target["username"])
    return {
        "username": target["roblox_username"] or target["username"],
        "custom_id": target["custom_id"],
        "role_name": target["role_name"],
        "joined_at": target["created_at"],
        "last_seen_at": target["last_seen_at"],
        "report_blacklisted": bool(blacklist and is_active(blacklist["until_ts"])),
        "terminated": bool(termination and is_active(termination["until_ts"])),
        "termination_reason": termination["reason"] if termination and is_active(termination["until_ts"]) else None,
    }


@app.post("/api/staff/login")
def staff_login(payload: dict[str, Any]):
    row = session_row(payload["session_key"])
    code_value = payload.get("agent_code", "").strip().upper()
    if not row["verified"] or not row["roblox_user_id"]:
        raise HTTPException(status_code=403, detail="You must verify your Roblox account first.")

    hq_bypass = row["role_name"] in {"Headquarters", "Joint Chiefs", "Ownership"}

    if hq_bypass and not code_value:
        alias = row["staff_agent_alias"] or make_staff_agent_alias()
        conn = db()
        conn.execute(
            """
            UPDATE sessions
            SET staff_mode = 1, staff_agent_code = NULL, staff_agent_alias = ?, staff_access_expires_at = NULL
            WHERE custom_id = ?
            """,
            (alias, row["custom_id"]),
        )
        conn.commit()
        conn.close()
        refreshed = session_row(payload["session_key"])
        log_staff_action(refreshed, "Entered Staff Version (HQ Auto-Access)")
        return {"success": True, "staff_agent_alias": alias, "staff_agent_code": None}

    if not code_value:
        raise HTTPException(status_code=400, detail="Please paste your Custom-Agent-Code in order to access the website.")

    conn = db()
    code = active_staff_access_row(conn, code_value=code_value)
    if not code:
        conn.close()
        raise HTTPException(status_code=404, detail="That Custom-Agent-Code is invalid or expired.")
    if code["linked_at"]:
        conn.close()
        raise HTTPException(status_code=403, detail="That Custom-Agent-Code is already bound to an agent.")
    if code["target_custom_id"] and code["target_custom_id"] != row["custom_id"]:
        conn.close()
        raise HTTPException(status_code=403, detail="That Custom-Agent-Code is reserved for another user.")
    if code["target_roblox_user_id"] and int(code["target_roblox_user_id"]) != int(row["roblox_user_id"]):
        conn.close()
        raise HTTPException(status_code=403, detail="That Custom-Agent-Code is reserved for another Roblox account.")
    if not row["is_group_member"]:
        conn.close()
        raise HTTPException(status_code=403, detail="Only Roblox group members can use a Custom-Agent-Code.")

    alias = code["agent_alias"] or make_staff_agent_alias()
    expires_at = code["expires_at"]
    conn.execute(
        """
        UPDATE custom_agent_codes
        SET target_custom_id = ?, target_username = ?, target_roblox_user_id = ?,
            target_roblox_username = ?, target_role_name = ?, target_group_rank = ?, agent_alias = ?, linked_at = ?, updated_at = ?
        WHERE id = ?
        """,
        (
            row["custom_id"],
            row["roblox_username"] or row["username"],
            row["roblox_user_id"],
            row["roblox_username"],
            row["role_name"],
            row["group_rank"],
            alias,
            now_ts(),
            now_ts(),
            code["id"],
        ),
    )
    conn.execute(
        """
        UPDATE sessions
        SET staff_mode = 1, staff_agent_code = ?, staff_agent_alias = ?, staff_access_expires_at = ?
        WHERE custom_id = ?
        """,
        (code_value, alias, expires_at, row["custom_id"]),
    )
    conn.commit()
    conn.close()

    refreshed = session_row(payload["session_key"])
    log_staff_action(refreshed, "Entered Staff Version")
    return {"success": True, "staff_agent_alias": alias, "staff_agent_code": code_value}


@app.post("/api/staff/logout")
def staff_logout(payload: dict[str, Any]):
    row = session_row(payload["session_key"])
    conn = db()
    clear_staff_mode_for_identity(conn, row["custom_id"], row["roblox_user_id"])
    conn.commit()
    conn.close()
    return {"success": True}


@app.post("/api/staff/code/generate")
def staff_code_generate(payload: dict[str, Any]):
    actor = require_permission(payload["session_key"], "manage_staff_access")
    expires_at, time_label = parse_staff_access_duration(payload.get("duration", "PERMANENT"))
    conn = db()
    code = issue_staff_code(conn, actor, None, expires_at)
    conn.commit()
    conn.close()
    log_staff_action(actor, "Generate new Custom-Agent-Code")
    return {"success": True, "code": code["code_value"], "time_label": time_label}


@app.post("/api/staff/access/grant")
def staff_access_grant(payload: dict[str, Any]):
    actor = require_permission(payload["session_key"], "manage_staff_access")
    target_custom_id = payload.get("target_custom_id", "").strip()
    if not target_custom_id:
        raise HTTPException(status_code=400, detail="Select a user.")
    expires_at, time_label = parse_staff_access_duration(payload.get("duration", "PERMANENT"))

    conn = db()
    target = conn.execute("SELECT * FROM sessions WHERE custom_id = ?", (target_custom_id,)).fetchone()
    if not target:
        conn.close()
        raise HTTPException(status_code=404, detail="User not found.")
    if target["verified"] and target["roblox_user_id"]:
        target = sync_live_group_state(conn, target)
    if not target["verified"] or not target["is_group_member"]:
        conn.close()
        raise HTTPException(status_code=403, detail="Staff access can only be given to verified Roblox group members.")
    code = issue_staff_code(conn, actor, target, expires_at)
    conn.commit()
    conn.close()

    add_inbox_item(
        target["custom_id"],
        "Internal Affairs Staff Access",
        f"Staff website access has been granted to you for {time_label}. Your Custom-Agent-Code is: {code['code_value']}",
    )
    add_event(target["custom_id"], "staff_access_granted", {"message": "Staff website access has been granted to you."})
    log_staff_action(actor, "Give Staff Access", target["custom_id"], target["roblox_username"] or target["username"])
    return {"success": True, "code": code["code_value"], "time_label": time_label}


@app.post("/api/staff/access/revoke")
def staff_access_revoke(payload: dict[str, Any]):
    actor = require_permission(payload["session_key"], "manage_staff_access")
    target_custom_id = payload.get("target_custom_id", "").strip()
    code_id = int(payload.get("code_id", 0) or 0)
    if not target_custom_id and not code_id:
        raise HTTPException(status_code=400, detail="Missing target.")

    conn = db()
    rows = []
    if code_id:
        row = conn.execute("SELECT * FROM custom_agent_codes WHERE id = ?", (code_id,)).fetchone()
        if row:
            rows.append(row)
    else:
        rows = conn.execute(
            """
            SELECT * FROM custom_agent_codes
            WHERE target_custom_id = ? AND active = 1 AND (expires_at IS NULL OR expires_at > ?)
            """,
            (target_custom_id, now_ts()),
        ).fetchall()
    if not rows:
        conn.close()
        raise HTTPException(status_code=404, detail="No active staff access found.")

    affected_custom_ids = set()
    for row in rows:
        if row["target_custom_id"]:
            affected_custom_ids.add(row["target_custom_id"])
        expire_staff_code(conn, int(row["id"]), "Staff access revoked by Headquarters.")
    conn.commit()
    conn.close()

    for affected_custom_id in affected_custom_ids:
        add_event(affected_custom_id, "staff_access_revoked", {"message": "Your staff website access has been revoked."})
        add_inbox_item(affected_custom_id, "Internal Affairs Staff Access", "Your staff website access has been revoked.")
    log_staff_action(actor, "Revoke Staff Access", target_custom_id or "N/A", "Custom-Agent-Code")
    return {"success": True}


@app.get("/api/staff/agents")
def staff_agents_database(session_key: str):
    actor = require_permission(session_key, "manage_staff_access")
    conn = db()
    rows = conn.execute(
        """
        SELECT *
        FROM custom_agent_codes
        WHERE linked_at IS NOT NULL
        ORDER BY updated_at DESC
        """
    ).fetchall()
    conn.close()
    log_staff_action(actor, "Check Agents Database")
    return {"items": [dict(x) for x in rows]}


@app.get("/api/staff/codes")
def staff_codes_database(session_key: str):
    actor = require_permission(session_key, "manage_staff_access")
    conn = db()
    rows = conn.execute(
        """
        SELECT *
        FROM custom_agent_codes
        ORDER BY active DESC, created_at DESC
        """
    ).fetchall()
    conn.close()
    log_staff_action(actor, "Custom-Agent-Code Database")
    return {"items": [dict(x) for x in rows]}


@app.post("/api/staff/code/expire")
def staff_code_expire(payload: dict[str, Any]):
    actor = require_permission(payload["session_key"], "manage_staff_access")
    code_id = int(payload.get("code_id", 0))
    if not code_id:
        raise HTTPException(status_code=400, detail="Missing code.")
    conn = db()
    row = conn.execute("SELECT * FROM custom_agent_codes WHERE id = ?", (code_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="Code not found.")
    expire_staff_code(conn, code_id, "Custom-Agent-Code expired manually.")
    conn.commit()
    conn.close()
    if row["target_custom_id"]:
        add_event(row["target_custom_id"], "staff_access_revoked", {"message": "Your Custom-Agent-Code has expired."})
    log_staff_action(actor, "Expire Custom-Agent-Code", row["target_custom_id"] or "N/A", row["code_value"])
    return {"success": True}


@app.post("/api/appeal")
def appeal(payload: dict[str, Any]):
    row = session_row(payload["session_key"])
    conn = db()
    last_appeal = conn.execute(
        """
        SELECT created_at
        FROM appeals
        WHERE appellant_custom_id = ?
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (row["custom_id"],),
    ).fetchone()
    conn.close()

    if last_appeal:
        next_appeal_at = int(last_appeal["created_at"]) + (14 * 24 * 60 * 60)
        if now_ts() < next_appeal_at:
            raise HTTPException(
                status_code=403,
                detail="You are only allowed to send one appeal every 2 weeks."
            )
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


@app.get("/api/interviews/status")
def interviews_status(session_key: str):
    session_row(session_key)
    return {"is_open": interviews_open()}


@app.post("/api/interviews/toggle")
def interviews_toggle(payload: dict[str, Any]):
    actor = require_permission(payload["session_key"], "manage_interviews")
    open_state = 1 if bool(payload.get("open")) else 0

    conn = db()
    conn.execute(
        """
        UPDATE interview_settings
        SET is_open = ?, updated_by_custom_id = ?, updated_by_username = ?, updated_at = ?
        WHERE id = 1
        """,
        (open_state, actor["custom_id"], actor["roblox_username"] or actor["username"], now_ts()),
    )
    conn.commit()
    conn.close()

    log_staff_action(actor, "Open Interviews" if open_state else "Close Interviews")
    return {"success": True, "is_open": bool(open_state)}


@app.post("/api/interview/apply")
def interview_apply(payload: dict[str, Any]):
    row = session_row(payload["session_key"])
    conn = db()
    last_application = conn.execute(
        """
        SELECT created_at
        FROM interview_applications
        WHERE applicant_custom_id = ?
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (row["custom_id"],),
    ).fetchone()
    conn.close()

    if last_application:
        next_application_at = int(last_application["created_at"]) + (14 * 24 * 60 * 60)
        if now_ts() < next_application_at:
            raise HTTPException(
                status_code=403,
                detail="You are only allowed to send one application every 2 weeks."
            )

    if not row["verified"]:
        raise HTTPException(status_code=403, detail="You must verify before applying.")
    if not interviews_open():
        raise HTTPException(status_code=403, detail="Applications are currently closed.")

    app_blacklist = application_blacklist_for(row["custom_id"])
    if app_blacklist:
        raise HTTPException(
            status_code=403,
            detail=f"You are application blacklisted. Reason: {app_blacklist['reason']} | Duration: {app_blacklist['time_label']}"
        )

    answers = payload.get("answers", [])
    if not isinstance(answers, list) or not answers:
        raise HTTPException(status_code=400, detail="No interview answers were provided.")

    cleaned_answers = []
    for item in answers:
        question = str(item.get("question", "")).strip()
        answer = str(item.get("answer", "")).strip()
        if not question or not answer:
            raise HTTPException(status_code=400, detail="Every interview question must have an answer.")
        cleaned_answers.append({"question": question, "answer": answer})

    overall_score = 0
    for item in cleaned_answers:
        overall_score += score_answer(
            item["answer"],
            ["professional", "discipline", "team", "fair", "learn", "administration", "intelligence", "stress"]
        )
    overall_score = round(overall_score / len(cleaned_answers))

    applicant_username = row["roblox_username"] or row["username"]

    conn = db()
    conn.execute(
        """
        INSERT INTO interview_applications (
            applicant_custom_id, applicant_username, roblox_user_id,
            answers_json, overall_score, created_at
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            row["custom_id"],
            applicant_username,
            row["roblox_user_id"],
            json_dump(cleaned_answers),
            overall_score,
            now_ts(),
        ),
    )
    conn.commit()
    conn.close()

    webhook_fields = [
        {"name": "Applicant", "value": applicant_username},
        {"name": "Custom ID", "value": row["custom_id"]},
    ]
    for item in cleaned_answers[:20]:
        webhook_fields.append({"name": item["question"][:256], "value": item["answer"][:1024]})

    post_webhook(
        INTERVIEW_LOG_WEBHOOK,
        "Interview Submission",
        webhook_fields,
        footer=row["custom_id"],
    )
    log_action(row, "Submitted Interview Application")
    return {"success": True}


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
    log_staff_action(actor, "Temporary Website Shutdown")
    return {"success": True, "time_label": label}


@app.post("/api/website-shutdown/clear")
def website_shutdown_clear(payload: dict[str, Any]):
    actor = require_permission(payload["session_key"], "manage_website_shutdown")
    conn = db()
    conn.execute("UPDATE website_shutdown SET active = 0 WHERE active = 1")
    conn.commit()
    conn.close()
    log_staff_action(actor, "Remove Website Shutdown")
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
    log_staff_action(actor, f"Reduce Amount of New Joins ({join_amount} per {period_name})")
    return {"success": True}


@app.post("/api/join-limit/revoke")
def revoke_join_limit(payload: dict[str, Any]):
    actor = require_permission(payload["session_key"], "manage_join_limit")
    conn = db()
    conn.execute("UPDATE join_limits SET active = 0 WHERE active = 1")
    conn.commit()
    conn.close()
    log_staff_action(actor, "Revoke Join Limit")
    return {"success": True}


@app.post("/api/contact/open")
def contact_open(payload: dict[str, Any]):
    row = session_row(payload["session_key"])
    subject = payload.get("subject", "").strip()
    if not subject:
        raise HTTPException(status_code=400, detail="Subject is required.")

    conn = db()
    existing = conn.execute(
        "SELECT id FROM tickets WHERE opener_custom_id = ? AND status = 'OPEN' ORDER BY updated_at DESC LIMIT 1",
        (row["custom_id"],),
    ).fetchone()
    if existing:
        conn.close()
        return {"success": True, "ticket_id": existing["id"], "already_open": True}

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
    if ticket["status"] != "OPEN":
        conn.close()
        raise HTTPException(status_code=400, detail="Closed tickets cannot be handled.")
    if ticket["claimed_by_custom_id"] and ticket["claimed_by_custom_id"] != actor["custom_id"]:
        conn.close()
        raise HTTPException(status_code=400, detail="This ticket is already being handled.")
    conn.execute(
        "UPDATE tickets SET claimed_by_custom_id = ?, claimed_by_username = ?, updated_at = ? WHERE id = ?",
        (actor["custom_id"], agent_display_name(actor), now_ts(), ticket_id),
    )
    conn.commit()
    conn.close()
    log_staff_action(actor, "Claim Ticket", ticket["opener_custom_id"], ticket["opener_username"])
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
    if ticket["status"] != "OPEN":
        conn.close()
        raise HTTPException(status_code=400, detail="This ticket is closed.")
    if row["custom_id"] != ticket["opener_custom_id"] and "view_tickets" not in merged_permissions(row):
        conn.close()
        raise HTTPException(status_code=403, detail="No access.")
    if row["custom_id"] != ticket["opener_custom_id"] and ticket["claimed_by_custom_id"] and ticket["claimed_by_custom_id"] != row["custom_id"]:
        conn.close()
        raise HTTPException(status_code=403, detail="This ticket is already assigned to another agent.")
    if row["custom_id"] != ticket["opener_custom_id"] and not ticket["claimed_by_custom_id"]:
        conn.execute(
            "UPDATE tickets SET claimed_by_custom_id = ?, claimed_by_username = ? WHERE id = ?",
            (row["custom_id"], agent_display_name(row), ticket_id),
        )
    conn.execute(
        "INSERT INTO ticket_messages (ticket_id, sender_custom_id, sender_username, sender_role, message, created_at) VALUES (?, ?, ?, ?, ?, ?)",
        (ticket_id, row["custom_id"], agent_display_name(row), row["role_name"], message, now_ts()),
    )
    conn.execute("UPDATE tickets SET updated_at = ? WHERE id = ?", (now_ts(), ticket_id))
    conn.commit()
    conn.close()

    recipient = ticket["opener_custom_id"] if row["custom_id"] != ticket["opener_custom_id"] else ticket["claimed_by_custom_id"]
    if recipient:
        add_event(recipient, "ticket_reply", {"message": "A ticket reply has been sent."})
        add_inbox_item(recipient, "Internal Affairs System", "A new reply has been added to your ticket.")

    if "view_tickets" in merged_permissions(row):
        log_staff_action(row, "Ticket Reply", ticket["opener_custom_id"], ticket["opener_username"])
    else:
        log_action(row, "Ticket Reply", ticket["claimed_by_custom_id"] or "N/A", ticket["claimed_by_username"] or "N/A")
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
    if ticket["status"] != "OPEN":
        conn.close()
        raise HTTPException(status_code=400, detail="This ticket is already closed.")
    if ticket["claimed_by_custom_id"] and ticket["claimed_by_custom_id"] != actor["custom_id"]:
        conn.close()
        raise HTTPException(status_code=403, detail="This ticket is assigned to another agent.")
    conn.execute(
        """
        UPDATE tickets
        SET status = 'CLOSED', close_reason = ?, claimed_by_custom_id = COALESCE(claimed_by_custom_id, ?),
            claimed_by_username = COALESCE(claimed_by_username, ?), updated_at = ?
        WHERE id = ?
        """,
        (reason, actor["custom_id"], agent_display_name(actor), now_ts(), ticket_id),
    )
    conn.commit()
    conn.close()

    add_event(ticket["opener_custom_id"], "ticket_closed", {"message": "Your ticket has been closed."})
    add_inbox_item(ticket["opener_custom_id"], "Internal Affairs System", "Your ticket has been closed.")
    log_staff_action(actor, "Close Ticket", ticket["opener_custom_id"], ticket["opener_username"])
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
    log_staff_action(actor, "Unsuspend Permission Abuse", abuser_custom_id, target["roblox_username"] or target["username"])
    return {"success": True}


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8000"))
    host = "0.0.0.0" if os.getenv("RENDER") or os.getenv("PORT") else "127.0.0.1"
    uvicorn.run("main:app", host=host, port=port, reload=True)
