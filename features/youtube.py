import os
import re
import shutil
import tempfile
from typing import Any

from db import DB
from config import Config

try:
    from yt_dlp import YoutubeDL  # type: ignore
except Exception:  # pragma: no cover
    YoutubeDL = None  # type: ignore


FEATURE: dict[str, Any] = {
    "name": "youtube",
    "scope": "user",
    "description": "YouTube video/audio downloader",
    "commands": ["youtube", "yt"],
}


def _clean_filename(name: str, max_len: int = 120) -> str:
    name = re.sub(r"[\\/:*?\"<>|]", "_", name).strip()
    name = re.sub(r"\s+", " ", name).strip()
    if len(name) > max_len:
        name = name[:max_len].rstrip()
    return name or "file"


def register(bot, db: DB, cfg: Config, *, safe_reply, require_admin) -> None:
    """Registers /youtube (and /yt) commands."""

    @bot.message_handler(commands=["youtube", "yt"])
    def cmd_youtube(message):
        user = message.from_user
        if not user:
            return

        is_admin = db.is_admin(user.id)

        # Respect bot disable switch (admins bypass)
        if not is_admin and not db.bot_enabled():
            safe_reply(message, "Bot is currently disabled by admin.")
            return

        # Feature gating
        f = db.get_feature("youtube")
        if not f:
            # In case DB was imported without the features table row
            db.ensure_feature(
                FEATURE["name"],
                FEATURE["scope"],
                FEATURE["description"],
                FEATURE["commands"],
                enabled_default=True,
            )
            f = db.get_feature("youtube")

        # Scope enforcement
        if not is_admin and (f["scope"] or "user") == "admin":
            safe_reply(message, "Admin only.")
            return

        # Global & per-feature checks
        if not is_admin and not db.features_global_enabled():
            safe_reply(message, "All features are currently disabled by admin.")
            return
        if not db.is_feature_enabled("youtube"):
            safe_reply(message, "This feature is currently disabled.")
            return

        if YoutubeDL is None:
            safe_reply(
                message,
                "YouTube downloader is not installed. Install dependency: pip install yt-dlp",
            )
            return

        parts = (message.text or "").split(maxsplit=2)
        if len(parts) < 2:
            safe_reply(
                message,
                "Usage:\n"
                "/youtube <url> [audio|video]\n"
                "Examples:\n"
                "/youtube https://youtu.be/... audio\n"
                "/youtube https://youtu.be/... video",
            )
            return

        url = parts[1].strip()
        mode = (parts[2].strip().lower() if len(parts) >= 3 else cfg.youtube.default_mode)
        if mode not in ("audio", "video"):
            mode = cfg.youtube.default_mode

        # Telegram bots have upload limits; default to 45MB (configurable)
        max_bytes = int(cfg.youtube.max_file_mb) * 1024 * 1024

        os.makedirs(cfg.youtube.download_dir, exist_ok=True)
        tmpdir = tempfile.mkdtemp(prefix="yt_", dir=cfg.youtube.download_dir)

        try:
            safe_reply(message, f"Downloading ({mode})…")

            if mode == "audio":
                # Avoid ffmpeg dependency: download single audio file (m4a preferred)
                fmt = "bestaudio[ext=m4a]/bestaudio/best"
            else:
                # Avoid ffmpeg dependency: prefer progressive mp4 (audio+video in one file)
                fmt = "best[ext=mp4][height<=720]/best[height<=720]/best"

            ydl_opts: dict[str, Any] = {
                "outtmpl": os.path.join(tmpdir, "%(title).200s.%(ext)s"),
                "noplaylist": True,
                "quiet": True,
                "no_warnings": True,
                "format": fmt,
                "max_filesize": max_bytes,
                "retries": 3,
                "socket_timeout": 20,
            }

            with YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=True)

            # Handle rare cases where extract_info returns a playlist-like container
            if isinstance(info, dict) and info.get("entries"):
                entry = next((e for e in info["entries"] if e), None)
                if not entry:
                    safe_reply(message, "Nothing found to download.")
                    return
                info = entry

            title = _clean_filename(str(info.get("title") or "download"))
            ext = str(info.get("ext") or "bin")

            # Find the downloaded file inside tmpdir
            candidates = [
                os.path.join(tmpdir, f)
                for f in os.listdir(tmpdir)
                if os.path.isfile(os.path.join(tmpdir, f))
            ]
            if not candidates:
                safe_reply(message, "Download failed (no output file produced).")
                return
            # pick largest file
            out_path = max(candidates, key=lambda p: os.path.getsize(p))

            size = os.path.getsize(out_path)
            if size > max_bytes:
                safe_reply(
                    message,
                    f"File too large to send ({size / 1024 / 1024:.1f}MB). Limit is {cfg.youtube.max_file_mb}MB.",
                )
                return

            send_name = f"{title}.{ext}"
            send_path = os.path.join(tmpdir, send_name)
            if os.path.basename(out_path) != send_name:
                # rename for nicer filename
                try:
                    os.replace(out_path, send_path)
                except Exception:
                    send_path = out_path

            with open(send_path, "rb") as f:
                if mode == "audio":
                    bot.send_audio(message.chat.id, f, caption=title)
                else:
                    bot.send_video(message.chat.id, f, caption=title)

        except Exception as e:
            safe_reply(message, f"Download error: {e}")
        finally:
            try:
                shutil.rmtree(tmpdir, ignore_errors=True)
            except Exception:
                pass

