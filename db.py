import sqlite3
import time
import threading
from typing import Any, Iterable


class DB:
    """Thread-safe SQLite wrapper (telebot handlers are multi-threaded)."""

    def __init__(self, path: str = "database.db"):
        self.path = path
        self._lock = threading.RLock()
        self.conn = sqlite3.connect(self.path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        with self._lock:
            self._apply_pragmas()
            self._migrate()

    def close(self) -> None:
        with self._lock:
            try:
                self.conn.close()
            except Exception:
                pass

    def reload(self) -> None:
        """Re-open the SQLite connection (useful after a full import/replace)."""
        with self._lock:
            try:
                self.conn.close()
            except Exception:
                pass
            self.conn = sqlite3.connect(self.path, check_same_thread=False)
            self.conn.row_factory = sqlite3.Row
            self._apply_pragmas()
            # Do not drop; just ensure tables exist
            self._migrate()

    def _apply_pragmas(self) -> None:
        cur = self.conn.cursor()
        cur.execute("PRAGMA journal_mode=WAL;")
        cur.execute("PRAGMA synchronous=NORMAL;")
        cur.execute("PRAGMA foreign_keys=ON;")
        cur.execute("PRAGMA busy_timeout=5000;")
        cur.close()
        self.conn.commit()

    def _migrate(self) -> None:
        cur = self.conn.cursor()

        cur.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS features (
            name TEXT PRIMARY KEY,
            scope TEXT NOT NULL CHECK(scope IN ('user','admin')),
            description TEXT NOT NULL DEFAULT '',
            commands TEXT NOT NULL DEFAULT '',
            enabled INTEGER NOT NULL DEFAULT 1,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS admins (
            user_id INTEGER PRIMARY KEY,
            added_at INTEGER NOT NULL
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS admins_pending (
            username TEXT PRIMARY KEY,
            added_at INTEGER NOT NULL
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS bans (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            reason TEXT,
            banned_at INTEGER NOT NULL
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS bans_pending (
            username TEXT PRIMARY KEY,
            reason TEXT,
            banned_at INTEGER NOT NULL
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS system_prompts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            prompt TEXT NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1,
            created_at INTEGER NOT NULL
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            first_seen INTEGER NOT NULL,
            last_seen INTEGER NOT NULL
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS user_changes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            field TEXT NOT NULL,
            old_value TEXT,
            new_value TEXT,
            changed_at INTEGER NOT NULL,
            FOREIGN KEY(user_id) REFERENCES users(user_id) ON DELETE CASCADE
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER NOT NULL,
            chat_type TEXT NOT NULL,
            user_id INTEGER,
            role TEXT NOT NULL CHECK(role IN ('user','assistant','system')),
            text TEXT NOT NULL,
            tg_message_id INTEGER,
            reply_to_tg_message_id INTEGER,
            created_at INTEGER NOT NULL
        );
        """)

        cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_chat_time ON messages(chat_id, created_at);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_user_time ON messages(user_id, created_at);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);")

        # Defaults
        self._set_default("bot_enabled", "1")
        self._set_default("backup_enabled", "0")
        self._set_default("features_global_enabled", "1")
        self._set_default("trigger_name", "daddygpt")
        self._set_default("bot_display_name", "DaddyGPT")
        self._set_default("persona", "Helpful, safe, bilingual (Arabic/English) assistant.")

        self.conn.commit()
        cur.close()

    def _set_default(self, key: str, value: str) -> None:
        cur = self.conn.cursor()
        cur.execute("INSERT OR IGNORE INTO settings(key,value) VALUES(?,?)", (key, value))
        cur.close()

    # ---------- generic helpers ----------
    def q(self, sql: str, params: Iterable[Any] = ()) -> list[sqlite3.Row]:
        with self._lock:
            cur = self.conn.cursor()
            cur.execute(sql, tuple(params))
            rows = cur.fetchall()
            cur.close()
            return rows

    def e(self, sql: str, params: Iterable[Any] = ()) -> None:
        with self._lock:
            cur = self.conn.cursor()
            cur.execute(sql, tuple(params))
            cur.close()
            self.conn.commit()

    # ---------- settings ----------
    def get_setting(self, key: str) -> str | None:
        rows = self.q("SELECT value FROM settings WHERE key=?", (key,))
        return rows[0]["value"] if rows else None

    def set_setting(self, key: str, value: str) -> None:
        self.e(
            "INSERT INTO settings(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )

    def bot_enabled(self) -> bool:
        return self.get_setting("bot_enabled") == "1"

    def backup_enabled(self) -> bool:
        return self.get_setting("backup_enabled") == "1"

    # ---------- features ----------
    def features_global_enabled(self) -> bool:
        return self.get_setting("features_global_enabled") == "1"

    def set_features_global_enabled(self, enabled: bool) -> None:
        self.set_setting("features_global_enabled", "1" if enabled else "0")

    def ensure_feature(
        self,
        name: str,
        scope: str,
        description: str,
        commands: list[str] | None = None,
        enabled_default: bool = True,
    ) -> None:
        """Create the feature row if missing; keep existing enabled value if already present."""
        now = int(time.time())
        commands_s = ",".join([c.strip().lstrip("/") for c in (commands or []) if c and c.strip()])
        with self._lock:
            cur = self.conn.cursor()
            cur.execute(
                """
                INSERT OR IGNORE INTO features(name, scope, description, commands, enabled, created_at, updated_at)
                VALUES(?,?,?,?,?,?,?)
                """,
                (
                    name.strip().lower(),
                    scope,
                    description or "",
                    commands_s,
                    1 if enabled_default else 0,
                    now,
                    now,
                ),
            )
            # Update metadata (do not override enabled)
            cur.execute(
                """
                UPDATE features
                SET scope=?, description=?, commands=?, updated_at=?
                WHERE name=?
                """,
                (scope, description or "", commands_s, now, name.strip().lower()),
            )
            cur.close()
            self.conn.commit()

    def list_features(self) -> list[sqlite3.Row]:
        return self.q(
            "SELECT name, scope, description, commands, enabled, created_at, updated_at FROM features ORDER BY name ASC"
        )

    def list_features_by_enabled(self, enabled: bool) -> list[sqlite3.Row]:
        return self.q(
            "SELECT name, scope, description, commands, enabled FROM features WHERE enabled=? ORDER BY name ASC",
            (1 if enabled else 0,),
        )

    def get_feature(self, name: str) -> sqlite3.Row | None:
        rows = self.q(
            "SELECT name, scope, description, commands, enabled FROM features WHERE name=?",
            (name.strip().lower(),),
        )
        return rows[0] if rows else None

    def set_feature_enabled(self, name: str, enabled: bool) -> None:
        self.e(
            "UPDATE features SET enabled=?, updated_at=? WHERE name=?",
            (1 if enabled else 0, int(time.time()), name.strip().lower()),
        )

    def is_feature_enabled(self, name: str) -> bool:
        r = self.get_feature(name)
        return bool(r) and int(r["enabled"]) == 1

    def is_feature_active(self, name: str, *, bypass_global: bool = False) -> bool:
        """Feature enabled AND (global enabled unless bypass_global)."""
        if not bypass_global and not self.features_global_enabled():
            return False
        return self.is_feature_enabled(name)

    # ---------- admins ----------
    def admin_count(self) -> int:
        return int(self.q("SELECT COUNT(*) AS c FROM admins")[0]["c"])

    def is_admin(self, user_id: int) -> bool:
        return bool(self.q("SELECT 1 FROM admins WHERE user_id=?", (user_id,)))

    def list_admins(self) -> list[int]:
        return [int(r["user_id"]) for r in self.q("SELECT user_id FROM admins ORDER BY user_id")]

    def add_admin_id(self, user_id: int) -> None:
        self.e("INSERT OR IGNORE INTO admins(user_id, added_at) VALUES(?,?)", (user_id, int(time.time())))

    def remove_admin_id(self, user_id: int) -> None:
        self.e("DELETE FROM admins WHERE user_id=?", (user_id,))

    def add_admin_pending(self, username: str) -> None:
        username = username.lstrip("@").lower()
        self.e("INSERT OR IGNORE INTO admins_pending(username, added_at) VALUES(?,?)", (username, int(time.time())))

    def resolve_pending_admin(self, user_id: int, username: str | None) -> None:
        if not username:
            return
        u = username.lower()
        if self.q("SELECT 1 FROM admins_pending WHERE username=?", (u,)):
            self.e("DELETE FROM admins_pending WHERE username=?", (u,))
            self.add_admin_id(user_id)

    # ---------- bans ----------
    def is_banned(self, user_id: int, username: str | None) -> bool:
        if self.q("SELECT 1 FROM bans WHERE user_id=?", (user_id,)):
            return True
        if username:
            u = username.lower()
            return bool(self.q("SELECT 1 FROM bans_pending WHERE username=?", (u,)))
        return False

    def ban_id(self, user_id: int, username: str | None, reason: str) -> None:
        self.e(
            "INSERT OR REPLACE INTO bans(user_id, username, reason, banned_at) VALUES(?,?,?,?)",
            (user_id, (username or None), reason, int(time.time())),
        )

    def ban_pending(self, username: str, reason: str) -> None:
        u = username.lstrip("@").lower()
        self.e("INSERT OR REPLACE INTO bans_pending(username, reason, banned_at) VALUES(?,?,?)", (u, reason, int(time.time())))

    def unban_id(self, user_id: int) -> None:
        self.e("DELETE FROM bans WHERE user_id=?", (user_id,))

    def unban_pending(self, username: str) -> None:
        u = username.lstrip("@").lower()
        self.e("DELETE FROM bans_pending WHERE username=?", (u,))

    def resolve_pending_ban(self, user_id: int, username: str | None) -> None:
        if not username:
            return
        u = username.lower()
        rows = self.q("SELECT reason FROM bans_pending WHERE username=?", (u,))
        if rows:
            reason = rows[0]["reason"] or ""
            self.e("DELETE FROM bans_pending WHERE username=?", (u,))
            self.ban_id(user_id, username, reason)

    def list_bans(self, limit: int = 200) -> list[sqlite3.Row]:
        return self.q(
            "SELECT user_id, username, reason, banned_at FROM bans ORDER BY banned_at DESC LIMIT ?",
            (limit,),
        )

    def get_ban(self, user_id: int) -> sqlite3.Row | None:
        rows = self.q("SELECT user_id, username, reason, banned_at FROM bans WHERE user_id=?", (user_id,))
        return rows[0] if rows else None

    # ---------- prompts/persona ----------
    def get_system_prompts(self) -> list[str]:
        rows = self.q("SELECT prompt FROM system_prompts WHERE enabled=1 ORDER BY id ASC")
        return [r["prompt"] for r in rows]

    def list_prompts(self) -> list[sqlite3.Row]:
        return self.q("SELECT id, enabled, created_at, prompt FROM system_prompts ORDER BY id ASC")

    def add_prompt(self, prompt: str) -> None:
        self.e("INSERT INTO system_prompts(prompt, enabled, created_at) VALUES(?,?,?)", (prompt, 1, int(time.time())))

    def set_prompt(self, pid: int, prompt: str) -> None:
        self.e("UPDATE system_prompts SET prompt=? WHERE id=?", (prompt, pid))

    def toggle_prompt(self, pid: int, enabled: bool) -> None:
        self.e("UPDATE system_prompts SET enabled=? WHERE id=?", (1 if enabled else 0, pid))

    def delete_prompt(self, pid: int) -> None:
        self.e("DELETE FROM system_prompts WHERE id=?", (pid,))

    def clear_prompts(self) -> None:
        self.e("DELETE FROM system_prompts", ())

    # ---------- user tracking ----------
    def upsert_user(self, user_id: int, username: str | None, first_name: str | None, last_name: str | None) -> None:
        now = int(time.time())
        existing = self.q("SELECT username, first_name, last_name FROM users WHERE user_id=?", (user_id,))
        if not existing:
            self.e(
                "INSERT INTO users(user_id, username, first_name, last_name, first_seen, last_seen) VALUES(?,?,?,?,?,?)",
                (user_id, username, first_name, last_name, now, now),
            )
            return

        ex = existing[0]

        def track(field: str, old: str | None, new: str | None):
            if (old or "") != (new or ""):
                self.e(
                    "INSERT INTO user_changes(user_id, field, old_value, new_value, changed_at) VALUES(?,?,?,?,?)",
                    (user_id, field, old, new, now),
                )

        track("username", ex["username"], username)
        track("first_name", ex["first_name"], first_name)
        track("last_name", ex["last_name"], last_name)

        self.e("UPDATE users SET username=?, first_name=?, last_name=?, last_seen=? WHERE user_id=?", (username, first_name, last_name, now, user_id))

    def user_search(self, qtxt: str, limit: int = 30) -> list[sqlite3.Row]:
        like = f"%{qtxt.strip()}%"
        return self.q(
            """
            SELECT user_id, username, first_name, last_name, first_seen, last_seen
            FROM users
            WHERE CAST(user_id AS TEXT) LIKE ?
               OR IFNULL(username,'') LIKE ?
               OR IFNULL(first_name,'') LIKE ?
               OR IFNULL(last_name,'') LIKE ?
            ORDER BY last_seen DESC
            LIMIT ?
            """,
            (like, like, like, like, limit),
        )

    def user_changes(self, user_id: int, limit: int = 50) -> list[sqlite3.Row]:
        return self.q(
            """
            SELECT field, old_value, new_value, changed_at
            FROM user_changes
            WHERE user_id=?
            ORDER BY changed_at DESC
            LIMIT ?
            """,
            (user_id, limit),
        )

    def get_user_id_by_username(self, username: str) -> int | None:
        u = username.lstrip("@").lower().strip()
        if not u:
            return None
        rows = self.q("SELECT user_id FROM users WHERE LOWER(username)=? LIMIT 1", (u,))
        return int(rows[0]["user_id"]) if rows else None

    # ---------- messages ----------
    def log_message(
        self,
        chat_id: int,
        chat_type: str,
        user_id: int | None,
        role: str,
        text: str,
        tg_message_id: int | None,
        reply_to_tg_message_id: int | None,
    ) -> None:
        self.e(
            """
            INSERT INTO messages(chat_id, chat_type, user_id, role, text, tg_message_id, reply_to_tg_message_id, created_at)
            VALUES(?,?,?,?,?,?,?,?)
            """,
            (chat_id, chat_type, user_id, role, text, tg_message_id, reply_to_tg_message_id, int(time.time())),
        )

    def get_recent_dialog(self, chat_id: int, user_id: int, limit: int = 20) -> list[sqlite3.Row]:
        """
        Recent messages for one user within a chat, including assistant replies tied to that user_id.
        This avoids mixing multiple users in group chats.
        """
        return self.q(
            """
            SELECT role, text
            FROM messages
            WHERE chat_id=? AND user_id=? AND role IN ('user','assistant')
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (chat_id, user_id, limit),
        )[::-1]

    def get_user_conversation(self, user_id: int, limit: int = 200) -> list[sqlite3.Row]:
        return self.q(
            """
            SELECT chat_id, chat_type, role, text, created_at
            FROM messages
            WHERE user_id=?
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (user_id, limit),
        )[::-1]

    def search_user_messages(self, user_id: int, query: str, limit: int = 200) -> list[sqlite3.Row]:
        like = f"%{query}%"
        return self.q(
            """
            SELECT chat_id, chat_type, role, text, created_at
            FROM messages
            WHERE user_id=? AND text LIKE ?
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (user_id, like, limit),
        )[::-1]

    # ---------- stats ----------
    def counts(self) -> dict[str, int]:
        d = {}
        d["users"] = int(self.q("SELECT COUNT(*) AS c FROM users")[0]["c"])
        d["messages"] = int(self.q("SELECT COUNT(*) AS c FROM messages")[0]["c"])
        d["bans"] = int(self.q("SELECT COUNT(*) AS c FROM bans")[0]["c"])
        d["admins"] = int(self.q("SELECT COUNT(*) AS c FROM admins")[0]["c"])
        d["prompts"] = int(self.q("SELECT COUNT(*) AS c FROM system_prompts")[0]["c"])
        return d
