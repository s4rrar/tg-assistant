import os
import sys
import re
import time
import threading
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import telebot
from telebot import apihelper

from db import DB
from config import load_config
import features
from security import load_decrypted_token, save_encrypted_token
from ollama_client import OllamaClient
from excel_io import export_db_to_xlsx, import_xlsx_to_db

TZ = ZoneInfo("Asia/Hebron")
DB_PATH = "database.db"


def split_telegram(text: str, max_len: int = 3800) -> list[str]:
    """Telegram max ~4096. We stay under it."""
    text = (text or "").strip()
    if not text:
        return ["…"]
    if len(text) <= max_len:
        return [text]

    parts: list[str] = []
    buf: list[str] = []
    n = 0
    for line in text.splitlines(True):
        # Hard-split very long lines
        while len(line) > max_len:
            head, line = line[:max_len], line[max_len:]
            if buf:
                parts.append("".join(buf).strip())
                buf, n = [], 0
            parts.append(head.strip())
        if n + len(line) > max_len and buf:
            parts.append("".join(buf).strip())
            buf = [line]
            n = len(line)
        else:
            buf.append(line)
            n += len(line)
    if buf:
        parts.append("".join(buf).strip())
    return [p for p in parts if p]


def first_run_setup_token() -> str:
    token = load_decrypted_token()
    if token:
        return token
    token = input("Enter Telegram Bot Token: ").strip()
    if not token:
        raise SystemExit("Token is required.")
    save_encrypted_token(token)
    print("Saved encrypted token to token.txt")
    return token


def first_run_setup_admins(db: DB) -> None:
    if db.admin_count() > 0:
        return
    raw = input("Enter initial admin Telegram user_id(s), comma-separated: ").strip()
    ids: list[int] = []
    for p in raw.split(","):
        p = p.strip()
        if not p:
            continue
        if not p.isdigit():
            raise SystemExit(f"Invalid admin id: {p}")
        ids.append(int(p))
    if not ids:
        raise SystemExit("At least one admin id is required.")
    for uid in ids:
        db.add_admin_id(uid)
    print(f"Saved {len(ids)} admin(s) to database.db")


def build_system_message(db: DB) -> str:
    # System prompts (array of strings in DB)
    prompts = db.get_system_prompts()
    persona = db.get_setting("persona") or ""
    bot_name = db.get_setting("bot_display_name") or "Bot"

    base = [
        f"You are {bot_name}.",
        "You support Arabic and English naturally (reply in the user's language when possible).",
        "Be concise, helpful, and safe.",
        f"Persona: {persona}",
    ]
    if prompts:
        base.append("System prompts:")
        base.extend([f"- {p}" for p in prompts])
    return "\n".join(base).strip()


def should_respond_in_group(
    message, bot_id: int, bot_username: str, trigger_name: str
) -> tuple[bool, str]:
    """
    Returns: (should_respond, cleaned_user_text)
    Conditions:
      - user replies to bot
      - OR message begins with trigger_name
      - OR message begins with @bot_username
    """
    text = (message.text or "").strip()
    if not text:
        return (False, "")

    if (
        message.reply_to_message
        and message.reply_to_message.from_user
        and message.reply_to_message.from_user.id == bot_id
    ):
        return (True, text)

    uname = re.escape((bot_username or "").lstrip("@"))
    trig = re.escape(trigger_name or "")
    pattern = re.compile(rf"^\s*(?:@{uname}\b|{trig}\b)\s*[:,]?\s*", re.IGNORECASE)
    m = pattern.match(text)
    if not m:
        return (False, "")

    cleaned = text[m.end() :].strip()
    return (bool(cleaned), cleaned)


def user_help_text(trigger_name: str, bot_username: str) -> str:
    # Keep it short + bilingual
    return (
        "Help / المساعدة\n\n"
        "Private chat: just send a message.\n"
        "المحادثة الخاصة: أرسل رسالتك مباشرة.\n\n"
        "Groups: talk to me by one of these:\n"
        f"• `{trigger_name} مرحبا` or `{trigger_name} hello`\n"
        f"• `@{bot_username} hello`\n"
        "• Reply to one of my messages\n\n"
        "Commands:\n"
        "/help – this message\n"
        "/features – list enabled features\n"
    )


def admin_commands_text() -> str:
    return (
        "Admin commands:\n"
        "/commands\n"
        "/bot_enable | /bot_disable\n"
        "/backup_enable | /backup_disable\n"
        "/reload\n"
        "\nFeatures:\n"
        "/features\n"
        "/enabled_features | /disabled_features\n"
        "/feature_enable <name> | /feature_disable <name>\n"
        "/features_enable_all | /features_disable_all\n"
        "\nModeration / data:\n"
        "/admins_list\n"
        "/admin_add <id|@username>\n"
        "/admin_remove <id>\n"
        "/ban <id|@username> [reason]\n"
        "/unban <id|@username>\n"
        "/bans_list\n"
        "/ban_info <id|@username>\n"
        "/prompts_list\n"
        "/prompt_add <text>\n"
        "/prompt_set <id> <text>\n"
        "/prompt_enable <id> | /prompt_disable <id>\n"
        "/prompt_del <id> | /prompts_clear\n"
        "/persona_set <text> | /persona_show\n"
        "/trigger_set <word> | /trigger_show\n"
        "/botname_set <name> | /botname_show\n"
        "/users <query>\n"
        "/user <id|@username>\n"
        "/chat <id|@username> [limit]\n"
        "/chat_search <id|@username> <query> [limit]\n"
        "/stats\n"
        "/export_db\n"
        "Send .xlsx with caption: /import_db\n"
    )


def schedule_daily_backup(bot: telebot.TeleBot, db: DB):
    while True:
        now = datetime.now(TZ)
        next_run = now.replace(hour=2, minute=0, second=0, microsecond=0)
        if next_run <= now:
            next_run += timedelta(days=1)
        time.sleep(max(1, int((next_run - now).total_seconds())))

        if not db.backup_enabled():
            continue

        try:
            date_str = datetime.now(TZ).strftime("%Y-%m-%d")
            out_path = os.path.join("backups", f"{date_str}.xlsx")
            export_db_to_xlsx(DB_PATH, out_path)

            for admin_id in db.list_admins():
                with open(out_path, "rb") as f:
                    bot.send_document(
                        admin_id, f, caption=f"Daily backup: {date_str}.xlsx"
                    )
        except Exception as e:
            for admin_id in db.list_admins():
                try:
                    bot.send_message(admin_id, f"Backup failed: {e}")
                except Exception:
                    pass


def main():
    apihelper.RETRY_ON_ERROR = True

    db = DB(DB_PATH)
    cfg = load_config()
    token = first_run_setup_token()
    first_run_setup_admins(db)

    bot = telebot.TeleBot(token, parse_mode=None, threaded=True, num_threads=8)
    me = bot.get_me()
    bot_id = me.id
    bot_username = (me.username or "").strip() or "bot"

    ollama = OllamaClient(
        base_url=cfg.ollama.url,
        model=cfg.ollama.model,
        timeout_s=cfg.ollama.timeout_s,
    )

    # Backup scheduler thread
    threading.Thread(target=schedule_daily_backup, args=(bot, db), daemon=True).start()

    # Simple per-user rate limit
    last_user_call: dict[int, float] = {}
    rl_lock = threading.Lock()

    def rate_limited(user_id: int, cooldown_s: float | None = None) -> bool:
        with rl_lock:
            now = time.time()
            prev = last_user_call.get(user_id, 0.0)
            cd = float(cooldown_s if cooldown_s is not None else cfg.bot.rate_limit_seconds)
            if now - prev < cd:
                return True
            last_user_call[user_id] = now
            return False

    def require_admin(message) -> bool:
        if not db.is_admin(message.from_user.id):
            safe_reply(message, "Admin only.")
            return False
        return True

    def resolve_id_or_username(token: str) -> int | None:
        token = (token or "").strip()
        if not token:
            return None
        if token.isdigit():
            return int(token)
        if token.startswith("@"):
            return db.get_user_id_by_username(token)
        return None

    def safe_reply(msg, text: str):
        """Send without parse mode to avoid Markdown/HTML entity parsing problems."""
        return bot.reply_to(msg, text, parse_mode=None)

    def safe_send(chat_id: int, text: str):
        return bot.send_message(chat_id, text, parse_mode=None)

    # ---------------- features (modular commands) ----------------
    features.register_features(
        bot,
        db,
        cfg,
        safe_reply=safe_reply,
        require_admin=require_admin,
    )

    # ---------------- user help ----------------
    @bot.message_handler(commands=["start", "help"])
    def cmd_help(message):
        trig = db.get_setting("trigger_name") or "daddygpt"
        safe_reply(message, user_help_text(trig, bot_username))

    # ---------------- admin help ----------------
    @bot.message_handler(commands=["commands"])
    def cmd_commands(message):
        if not require_admin(message):
            return
        safe_reply(message, admin_commands_text())

    @bot.message_handler(commands=["reload"])
    def cmd_reload(message):
        if not require_admin(message):
            return
        try:
            safe_reply(message, "Reloading…")
        except Exception:
            pass
        os.execv(sys.executable, [sys.executable] + sys.argv)

    @bot.message_handler(commands=["bot_enable", "bot_disable"])
    def cmd_bot_toggle(message):
        if not require_admin(message):
            return
        enabled = message.text.strip().endswith("enable")
        db.set_setting("bot_enabled", "1" if enabled else "0")
        safe_reply(message, f"Bot enabled: {db.bot_enabled()}")

    @bot.message_handler(commands=["backup_enable", "backup_disable"])
    def cmd_backup_toggle(message):
        if not require_admin(message):
            return
        enabled = message.text.strip().endswith("enable")
        db.set_setting("backup_enabled", "1" if enabled else "0")
        safe_reply(message, f"Daily 2AM backup enabled: {db.backup_enabled()}")

    @bot.message_handler(commands=["admins_list"])
    def cmd_admins_list(message):
        if not require_admin(message):
            return
        admins = db.list_admins()
        safe_reply(message, "Admins:\n" + "\n".join(str(a) for a in admins))

    @bot.message_handler(commands=["admin_add"])
    def cmd_admin_add(message):
        if not require_admin(message):
            return
        parts = message.text.split(maxsplit=1)
        if len(parts) < 2:
            safe_reply(message, "Usage: /admin_add <id|@username>")
            return
        target = parts[1].strip()
        if target.isdigit():
            db.add_admin_id(int(target))
            safe_reply(message, f"Added admin id {target}")
        else:
            db.add_admin_pending(target)
            safe_reply(
                message,
                "Added pending admin. They become admin after they message the bot once.",
            )

    @bot.message_handler(commands=["admin_remove"])
    def cmd_admin_remove(message):
        if not require_admin(message):
            return
        parts = message.text.split(maxsplit=1)
        if len(parts) < 2 or not parts[1].strip().isdigit():
            safe_reply(message, "Usage: /admin_remove <id>")
            return
        uid = int(parts[1].strip())
        db.remove_admin_id(uid)
        safe_reply(message, f"Removed admin id {uid}")

    @bot.message_handler(commands=["ban"])
    def cmd_ban(message):
        if not require_admin(message):
            return
        parts = message.text.split(maxsplit=2)
        if len(parts) < 2:
            safe_reply(message, "Usage: /ban <id|@username> [reason]")
            return
        target = parts[1].strip()
        reason = parts[2].strip() if len(parts) >= 3 else "banned"
        if target.isdigit():
            db.ban_id(int(target), None, reason)
            safe_reply(message, f"Banned id {target}")
        else:
            db.ban_pending(target, reason)
            safe_reply(
                message, "Banned pending username (applies when they message the bot)."
            )

    @bot.message_handler(commands=["unban"])
    def cmd_unban(message):
        if not require_admin(message):
            return
        parts = message.text.split(maxsplit=1)
        if len(parts) < 2:
            safe_reply(message, "Usage: /unban <id|@username>")
            return
        target = parts[1].strip()
        if target.isdigit():
            db.unban_id(int(target))
            safe_reply(message, f"Unbanned id {target}")
        else:
            db.unban_pending(target)
            safe_reply(message, "Unbanned pending username.")

    @bot.message_handler(commands=["bans_list"])
    def cmd_bans_list(message):
        if not require_admin(message):
            return
        rows = db.list_bans(limit=200)
        if not rows:
            safe_reply(message, "No bans.")
            return
        lines = []
        for r in rows:
            lines.append(
                f"{r['user_id']}  @{r['username'] or '-'}  {r['reason'] or ''}"
            )
        safe_reply(message, ("Bans:\n" + "\n".join(lines))[:3800])

    @bot.message_handler(commands=["ban_info"])
    def cmd_ban_info(message):
        if not require_admin(message):
            return
        parts = message.text.split(maxsplit=1)
        if len(parts) < 2:
            safe_reply(message, "Usage: /ban_info <id|@username>")
            return
        uid = resolve_id_or_username(parts[1].strip())
        if uid is None:
            safe_reply(
                message,
                "User not found (they must have messaged the bot at least once for @username lookup).",
            )
            return
        b = db.get_ban(uid)
        if not b:
            safe_reply(message, "Not banned.")
            return
        safe_reply(
            message,
            f"Banned: {b['user_id']} @{b['username'] or '-'}\nReason: {b['reason'] or ''}",
        )

    @bot.message_handler(commands=["prompts_list"])
    def cmd_prompts_list(message):
        if not require_admin(message):
            return
        rows = db.list_prompts()
        if not rows:
            safe_reply(message, "No prompts.")
            return
        out = []
        for r in rows:
            out.append(f"#{r['id']} enabled={r['enabled']} :: {r['prompt']}")
        safe_reply(message, "\n\n".join(out)[:3800])

    @bot.message_handler(commands=["prompt_add"])
    def cmd_prompt_add(message):
        if not require_admin(message):
            return
        parts = message.text.split(maxsplit=1)
        if len(parts) < 2 or not parts[1].strip():
            safe_reply(message, "Usage: /prompt_add <text>")
            return
        txt = parts[1].strip()
        if len(txt) > 4000:
            safe_reply(message, "Prompt too long.")
            return
        db.add_prompt(txt)
        safe_reply(message, "Prompt added.")

    @bot.message_handler(commands=["prompt_set"])
    def cmd_prompt_set(message):
        if not require_admin(message):
            return
        parts = message.text.split(maxsplit=2)
        if len(parts) < 3 or not parts[1].isdigit():
            safe_reply(message, "Usage: /prompt_set <id> <text>")
            return
        txt = parts[2].strip()
        if len(txt) > 4000:
            safe_reply(message, "Prompt too long.")
            return
        db.set_prompt(int(parts[1]), txt)
        safe_reply(message, "Prompt updated.")

    @bot.message_handler(commands=["prompt_enable", "prompt_disable"])
    def cmd_prompt_toggle(message):
        if not require_admin(message):
            return
        parts = message.text.split(maxsplit=1)
        if len(parts) < 2 or not parts[1].isdigit():
            safe_reply(message, "Usage: /prompt_enable <id> OR /prompt_disable <id>")
            return
        enabled = message.text.strip().startswith("/prompt_enable")
        db.toggle_prompt(int(parts[1]), enabled)
        safe_reply(message, f"Prompt #{parts[1]} enabled={enabled}")

    @bot.message_handler(commands=["prompt_del"])
    def cmd_prompt_del(message):
        if not require_admin(message):
            return
        parts = message.text.split(maxsplit=1)
        if len(parts) < 2 or not parts[1].isdigit():
            safe_reply(message, "Usage: /prompt_del <id>")
            return
        db.delete_prompt(int(parts[1]))
        safe_reply(message, "Prompt deleted.")

    @bot.message_handler(commands=["prompts_clear"])
    def cmd_prompts_clear(message):
        if not require_admin(message):
            return
        db.clear_prompts()
        safe_reply(message, "All prompts cleared.")

    @bot.message_handler(commands=["persona_set", "persona_show"])
    def cmd_persona(message):
        if not require_admin(message):
            return
        if message.text.startswith("/persona_show"):
            safe_reply(message, db.get_setting("persona") or "")
            return
        parts = message.text.split(maxsplit=1)
        if len(parts) < 2:
            safe_reply(message, "Usage: /persona_set <text>")
            return
        db.set_setting("persona", parts[1].strip())
        safe_reply(message, "Persona updated.")

    @bot.message_handler(commands=["trigger_set", "trigger_show"])
    def cmd_trigger(message):
        if not require_admin(message):
            return
        if message.text.startswith("/trigger_show"):
            safe_reply(message, db.get_setting("trigger_name") or "daddygpt")
            return
        parts = message.text.split(maxsplit=1)
        if len(parts) < 2:
            safe_reply(message, "Usage: /trigger_set <word>")
            return
        db.set_setting("trigger_name", parts[1].strip())
        safe_reply(message, "Trigger updated.")

    @bot.message_handler(commands=["botname_set", "botname_show"])
    def cmd_botname(message):
        if not require_admin(message):
            return
        if message.text.startswith("/botname_show"):
            safe_reply(message, db.get_setting("bot_display_name") or "")
            return
        parts = message.text.split(maxsplit=1)
        if len(parts) < 2:
            safe_reply(message, "Usage: /botname_set <name>")
            return
        db.set_setting("bot_display_name", parts[1].strip())
        safe_reply(message, "Bot display name updated.")

    @bot.message_handler(commands=["users"])
    def cmd_users(message):
        if not require_admin(message):
            return
        parts = message.text.split(maxsplit=1)
        if len(parts) < 2:
            safe_reply(message, "Usage: /users <query>")
            return
        rows = db.user_search(parts[1], limit=30)
        if not rows:
            safe_reply(message, "No matches.")
            return
        out = []
        for r in rows:
            name = " ".join([x for x in [r["first_name"], r["last_name"]] if x])
            out.append(f"{r['user_id']}  @{r['username'] or '-'}  {name}".strip())
        safe_reply(message, "\n".join(out)[:3800])

    @bot.message_handler(commands=["user"])
    def cmd_user(message):
        if not require_admin(message):
            return
        parts = message.text.split(maxsplit=1)
        if len(parts) < 2:
            safe_reply(message, "Usage: /user <id|@username>")
            return
        uid = resolve_id_or_username(parts[1].strip())
        if uid is None:
            safe_reply(
                message,
                "User not found (they must have messaged the bot at least once for @username lookup).",
            )
            return

        rows = db.q("SELECT * FROM users WHERE user_id=?", (uid,))
        if not rows:
            safe_reply(message, "User not found.")
            return
        u = rows[0]
        changes = db.user_changes(uid, limit=30)
        out = [
            f"User {uid}",
            f"username: @{u['username'] or '-'}",
            f"name: {(u['first_name'] or '')} {(u['last_name'] or '')}".strip(),
        ]
        if changes:
            out.append("\nRecent changes:")
            for c in changes:
                out.append(f"- {c['field']}: {c['old_value']} -> {c['new_value']}")
        safe_reply(message, "\n".join(out)[:3800])

    @bot.message_handler(commands=["chat"])
    def cmd_chat(message):
        if not require_admin(message):
            return
        parts = message.text.split(maxsplit=2)
        if len(parts) < 2:
            safe_reply(message, "Usage: /chat <id|@username> [limit]")
            return
        uid = resolve_id_or_username(parts[1].strip())
        if uid is None:
            safe_reply(message, "User not found.")
            return
        limit = int(parts[2]) if len(parts) >= 3 and parts[2].isdigit() else 200
        rows = db.get_user_conversation(uid, limit=limit)
        if not rows:
            safe_reply(message, "No messages.")
            return

        lines = []
        for r in rows:
            role = r["role"]
            text = (r["text"] or "").replace("\r", "")
            lines.append(f"[{role}] {text}")
        content = "\n\n".join(lines)
        fname = f"user_{uid}_chat.txt"
        with open(fname, "w", encoding="utf-8") as f:
            f.write(content)
        with open(fname, "rb") as f:
            bot.send_document(message.chat.id, f, caption=f"Chat export for {uid}")
        try:
            os.remove(fname)
        except Exception:
            pass

    @bot.message_handler(commands=["chat_search"])
    def cmd_chat_search(message):
        if not require_admin(message):
            return
        parts = message.text.split(maxsplit=3)
        if len(parts) < 3:
            safe_reply(message, "Usage: /chat_search <id|@username> <query> [limit]")
            return
        uid = resolve_id_or_username(parts[1].strip())
        if uid is None:
            safe_reply(message, "User not found.")
            return
        query = parts[2].strip()
        limit = int(parts[3]) if len(parts) >= 4 and parts[3].isdigit() else 200
        rows = db.search_user_messages(uid, query=query, limit=limit)
        if not rows:
            safe_reply(message, "No matches.")
            return
        lines = []
        for r in rows:
            role = r["role"]
            text = (r["text"] or "").replace("\r", "")
            lines.append(f"[{role}] {text}")
        content = "\n\n".join(lines)
        fname = f"user_{uid}_search.txt"
        with open(fname, "w", encoding="utf-8") as f:
            f.write(content)
        with open(fname, "rb") as f:
            bot.send_document(
                message.chat.id, f, caption=f"Search results for {uid}: '{query}'"
            )
        try:
            os.remove(fname)
        except Exception:
            pass

    @bot.message_handler(commands=["stats"])
    def cmd_stats(message):
        if not require_admin(message):
            return
        c = db.counts()
        bot.reply_to(
            message,
            "Stats:\n"
            f"- users: {c['users']}\n"
            f"- messages: {c['messages']}\n"
            f"- admins: {c['admins']}\n"
            f"- bans: {c['bans']}\n"
            f"- prompts: {c['prompts']}\n"
            f"- bot_enabled: {db.bot_enabled()}\n"
            f"- backup_enabled: {db.backup_enabled()}\n",
        )

    @bot.message_handler(commands=["export_db"])
    def cmd_export_db(message):
        if not require_admin(message):
            return
        date_str = datetime.now(TZ).strftime("%Y-%m-%d_%H-%M-%S")
        out_path = os.path.join("backups", f"manual_{date_str}.xlsx")
        export_db_to_xlsx(DB_PATH, out_path)
        with open(out_path, "rb") as f:
            bot.send_document(
                message.chat.id, f, caption=f"DB export: {os.path.basename(out_path)}"
            )

    @bot.message_handler(content_types=["document"])
    def handle_document(message):
        # Import: admin sends an xlsx with caption "/import_db"
        if not message.caption or not message.caption.strip().startswith("/import_db"):
            return
        if not require_admin(message):
            return
        doc = message.document
        if not doc.file_name.lower().endswith(".xlsx"):
            safe_reply(message, "Please send a .xlsx file.")
            return

        # Basic size guard (Telegram docs can be huge)
        if doc.file_size and doc.file_size > 25 * 1024 * 1024:
            safe_reply(message, "File too large (max 25MB).")
            return

        file_info = bot.get_file(doc.file_id)
        data = bot.download_file(file_info.file_path)
        tmp = "import.xlsx"
        with open(tmp, "wb") as f:
            f.write(data)
        try:
            import_xlsx_to_db(tmp, DB_PATH)
            db.reload()
            safe_reply(message, "Import completed (tables replaced).")
        except Exception as e:
            safe_reply(message, f"Import failed: {e}")
        finally:
            try:
                os.remove(tmp)
            except Exception:
                pass

    @bot.message_handler(content_types=["text"])
    def handle_text(message):
        user = message.from_user
        if not user:
            return

        # Track user + resolve pending admin/ban by username
        db.upsert_user(user.id, user.username, user.first_name, user.last_name)
        db.resolve_pending_admin(user.id, user.username)
        db.resolve_pending_ban(user.id, user.username)

        # Ignore commands here (they have their own handlers)
        txt = (message.text or "").strip()
        if txt.startswith("/"):
            return

        # Allow admins even if bot is disabled
        if not db.bot_enabled() and not db.is_admin(user.id):
            return

        # Deny banned users (admins bypass)
        if not db.is_admin(user.id) and db.is_banned(user.id, user.username):
            return

        # Rate limit
        if not db.is_admin(user.id) and rate_limited(user.id):
            return

        trigger_name = db.get_setting("trigger_name") or "daddygpt"

        cleaned = txt
        if message.chat.type in ("group", "supergroup"):
            ok, cleaned = should_respond_in_group(
                message, bot_id, bot_username, trigger_name
            )
            if not ok:
                return

        if not cleaned:
            return

        # Log incoming (use cleaned content)
        db.log_message(
            chat_id=message.chat.id,
            chat_type=message.chat.type,
            user_id=user.id,
            role="user",
            text=cleaned,
            tg_message_id=message.message_id,
            reply_to_tg_message_id=(
                message.reply_to_message.message_id
                if message.reply_to_message
                else None
            ),
        )

        # Build context for Ollama (per-user dialog in the chat)
        system_msg = build_system_message(db)
        recent = db.get_recent_dialog(message.chat.id, user.id, limit=20)

        ollama_messages = [{"role": "system", "content": system_msg}]
        for r in recent:
            ollama_messages.append({"role": r["role"], "content": r["text"]})

        try:
            bot.send_chat_action(message.chat.id, "typing")
            reply = ollama.chat(ollama_messages)
            if not reply:
                reply = "…"
        except Exception as e:
            reply = f"LLM error: {e}"

        # Send reply + log
        for i, part in enumerate(split_telegram(reply)):
            sent = (
                safe_reply(message, part)
                if i == 0
                else bot.send_message(message.chat.id, part)
            )
            db.log_message(
                chat_id=message.chat.id,
                chat_type=message.chat.type,
                user_id=user.id,  # tie assistant reply to the requesting user
                role="assistant",
                text=part,
                tg_message_id=sent.message_id if sent else None,
                reply_to_tg_message_id=message.message_id,
            )

    print(f"Bot started as @{bot_username} (id={bot_id})")
    bot.infinity_polling(skip_pending=True, timeout=30, long_polling_timeout=30)


if __name__ == "__main__":
    main()
