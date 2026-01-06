import os
import re
import shutil
import tempfile
import time
import secrets
from typing import Any, Optional, Tuple

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

# In-memory pending requests: token -> state
# NOTE: if you run multiple bot workers/processes, replace this with DB storage.
_YT_PENDING: dict[str, dict[str, Any]] = {}
_PENDING_TTL_SEC = 10 * 60  # 10 minutes

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def _strip_ansi(s: str) -> str:
    return _ANSI_RE.sub("", s)


def _clean_filename(name: str, max_len: int = 120) -> str:
    name = re.sub(r"[\\/:*?\"<>|]", "_", name).strip()
    name = re.sub(r"\s+", " ", name).strip()
    if len(name) > max_len:
        name = name[:max_len].rstrip()
    return name or "file"


def _apply_auth_opts(ydl_opts: dict[str, Any], cfg: Config) -> None:
    """
    Pass YouTube authentication cookies to yt-dlp if configured.

    Expected config fields (optional):
      - cfg.youtube.cookies_file: str path to Netscape cookies.txt
      - cfg.youtube.cookies_from_browser: str | tuple/list, e.g. "chrome" or ("chrome",) or ("chrome", "/path/profile")
    """
    cookies_file = getattr(cfg.youtube, "cookies_file", None)
    cookies_from_browser = getattr(cfg.youtube, "cookies_from_browser", None)

    if cookies_file:
        ydl_opts["cookiefile"] = str(cookies_file)
        return

    if cookies_from_browser:
        # yt-dlp expects cookiesfrombrowser as tuple: (browser,) or (browser, profile_path)
        if isinstance(cookies_from_browser, (list, tuple)):
            ydl_opts["cookiesfrombrowser"] = tuple(cookies_from_browser)
        else:
            ydl_opts["cookiesfrombrowser"] = (str(cookies_from_browser),)


def _get_filesize(f: dict[str, Any]) -> Optional[int]:
    for k in ("filesize", "filesize_approx"):
        v = f.get(k)
        if isinstance(v, int) and v > 0:
            return v
    return None


def _video_score(f: dict[str, Any], *, target_h: int) -> Tuple[int, int, int, int]:
    """
    Higher is better.
    Prefer:
      - height <= target, closer is better
      - mp4 ext
      - avc1 (H.264) vcodec
      - higher tbr
    """
    height = int(f.get("height") or 0)
    ext = str(f.get("ext") or "")
    vcodec = str(f.get("vcodec") or "")
    tbr = int(float(f.get("tbr") or 0))

    if height and height <= target_h:
        height_score = 10_000 + height
    else:
        height_score = height  # allow > target as fallback

    ext_score = 2 if ext == "mp4" else 1
    codec_score = 2 if vcodec.startswith("avc1") else 1

    return (height_score, ext_score, codec_score, tbr)


def _audio_score(f: dict[str, Any]) -> Tuple[int, int]:
    ext = str(f.get("ext") or "")
    abr = int(float(f.get("abr") or 0))
    ext_score = 2 if ext in ("m4a", "mp4") else 1
    return (ext_score, abr)


def _pick_best_audio_id(formats: list[dict[str, Any]]) -> Optional[str]:
    """
    Find the best audio-only format.
    Audio-only formats have vcodec == 'none' and either:
    - acodec is a valid string (not 'none')
    - acodec is None (common with HLS/m3u8 streams where codec isn't determined yet)
    """
    if not formats:
        return None

    # Find audio-only formats
    audio_only = []
    for f in formats:
        format_id = f.get("format_id")
        if not format_id:
            continue

        vcodec = f.get("vcodec")
        acodec = f.get("acodec")

        # Must be video-less (vcodec is 'none' or missing)
        if vcodec and vcodec != "none":
            continue

        # Must have audio:
        # - acodec can be None (HLS streams where codec isn't determined yet)
        # - OR acodec is a string that's not 'none'
        if acodec == "none":
            continue

        # If we get here, it's either:
        # 1. vcodec='none' and acodec=None (HLS audio stream)
        # 2. vcodec='none' and acodec='opus'/'mp4a' etc
        audio_only.append(f)

    if not audio_only:
        # Fallback: any format with audio codec that's not 'none'
        audio_only = [
            f
            for f in formats
            if f.get("format_id") and f.get("acodec") and f.get("acodec") != "none"
        ]

    if not audio_only:
        return None

    best = max(audio_only, key=_audio_score)
    return str(best["format_id"])


def _pick_video_options(
    formats: list[dict[str, Any]], *, max_height: int = 720
) -> list[tuple[int, str]]:
    """
    Returns list of (height, format_id) for video-only formats, one per height.
    Prefers avc1/mp4.
    """
    vids = [
        f
        for f in formats
        if str(f.get("acodec") or "none") == "none"
        and str(f.get("vcodec") or "none") != "none"
        and f.get("format_id")
    ]

    # group by height
    by_h: dict[int, list[dict[str, Any]]] = {}
    for f in vids:
        h = int(f.get("height") or 0)
        if h <= 0:
            continue
        if h > max_height:
            continue
        by_h.setdefault(h, []).append(f)

    options: list[tuple[int, str]] = []
    for h, lst in by_h.items():
        best = max(lst, key=lambda x: _video_score(x, target_h=max_height))
        options.append((h, str(best["format_id"])))

    options.sort(key=lambda x: x[0])
    return options


def _pick_output_file(info: dict[str, Any], tmpdir: str) -> Optional[str]:
    # Prefer yt-dlp reported final filepath(s)
    req = info.get("requested_downloads")
    if isinstance(req, list) and req:
        for item in req:
            fp = item.get("filepath")
            if isinstance(fp, str) and os.path.isfile(fp):
                return fp

    for k in ("filepath", "_filename"):
        fp = info.get(k)
        if isinstance(fp, str) and os.path.isfile(fp):
            return fp

    # Fallback scan
    candidates: list[str] = []
    for fn in os.listdir(tmpdir):
        p = os.path.join(tmpdir, fn)
        if os.path.isfile(p) and not fn.endswith((".part", ".ytdl", ".temp")):
            candidates.append(p)
    return max(candidates, key=os.path.getsize) if candidates else None


def _cleanup_pending() -> None:
    now = time.time()
    expired = [
        k
        for k, v in _YT_PENDING.items()
        if now - float(v.get("ts", 0)) > _PENDING_TTL_SEC
    ]
    for k in expired:
        _YT_PENDING.pop(k, None)


def register(bot, db: DB, cfg: Config, *, safe_reply, require_admin) -> None:
    """Registers /youtube (and /yt) commands."""

    try:
        from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton  # type: ignore
    except Exception:  # pragma: no cover
        InlineKeyboardMarkup = None  # type: ignore
        InlineKeyboardButton = None  # type: ignore

    def _need_ffmpeg_or_fail(message) -> bool:
        ffmpeg_ok = shutil.which("ffmpeg") is not None
        ffprobe_ok = shutil.which("ffprobe") is not None
        if not (ffmpeg_ok and ffprobe_ok):
            safe_reply(
                message,
                "ffmpeg/ffprobe not found. Install ffmpeg and ensure it's on PATH.\n"
                "Example (Ubuntu/Debian): apt-get install ffmpeg",
            )
            return False
        return True

    def _probe_info(url: str) -> dict[str, Any]:
        probe_opts: dict[str, Any] = {
            "quiet": True,
            "no_warnings": True,
            "no_color": True,
            "noplaylist": True,
            "skip_download": True,
            "socket_timeout": 20,
            # Helps with some YouTube edge cases / missing formats
            "extractor_args": {
                "youtube": {"player_client": ["android", "web", "tv_embedded", "ios"]}
            },
        }
        _apply_auth_opts(probe_opts, cfg)

        with YoutubeDL(probe_opts) as ydl:
            info = ydl.extract_info(url, download=False)

        # Handle rare playlist-like container
        if isinstance(info, dict) and info.get("entries"):
            entry = next((e for e in info["entries"] if e), None)
            if isinstance(entry, dict):
                return entry
        if not isinstance(info, dict):
            raise RuntimeError("Unexpected extractor result")
        return info

    def _send_picker(
        message,
        *,
        title: str,
        thumb_url: Optional[str],
        token: str,
        video_opts: list[tuple[int, str]],
    ) -> None:
        """Show options under the thumbnail as inline buttons."""
        if InlineKeyboardMarkup is None or InlineKeyboardButton is None:
            safe_reply(message, "Inline buttons not available in this bot setup.")
            return

        kb = InlineKeyboardMarkup(row_width=3)

        # Video resolution buttons
        rows: list[list[Any]] = []
        cur: list[Any] = []
        for h, fmt_id in video_opts:
            cur.append(
                InlineKeyboardButton(f"░ {h}p", callback_data=f"yt:{token}:v:{fmt_id}")
            )
            if len(cur) == 3:
                rows.append(cur)
                cur = []
        if cur:
            rows.append(cur)
        for r in rows:
            kb.row(*r)

        # Audio buttons (convert via ffmpeg)
        kb.row(
            InlineKeyboardButton("░ Audio (M4A)", callback_data=f"yt:{token}:a:m4a"),
            InlineKeyboardButton("░ Audio (MP3)", callback_data=f"yt:{token}:a:mp3"),
        )

        kb.row(InlineKeyboardButton("✖ Cancel", callback_data=f"yt:{token}:x"))

        caption = f"{title}\n\nChoose a format:"
        if thumb_url:
            bot.send_photo(message.chat.id, thumb_url, caption=caption, reply_markup=kb)
        else:
            bot.send_message(message.chat.id, caption, reply_markup=kb)

    def _friendly_auth_error(text: str) -> str:
        t = _strip_ansi(text)
        if "Please sign in" in t or ("cookies" in t and "authentication" in t):
            return (
                "This video requires YouTube login (age/region/restriction).\n"
                "Admin must configure cookies.\n\n"
                "Set one of:\n"
                "- cfg.youtube.cookies_file = '/path/to/cookies.txt'\n"
                "- cfg.youtube.cookies_from_browser = 'chrome'  (local dev)\n"
            )
        return t

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
            safe_reply(message, "Install dependency: pip install -U yt-dlp")
            return

        if not _need_ffmpeg_or_fail(message):
            return

        parts = (message.text or "").split(maxsplit=1)
        if len(parts) < 2:
            safe_reply(message, "Usage:\n/youtube <url>\n(or /yt <url>)")
            return

        url = parts[1].strip()
        if not url:
            safe_reply(message, "Usage:\n/youtube <url>")
            return

        _cleanup_pending()

        try:
            safe_reply(message, "Fetching available formats…")
            info = _probe_info(url)

            title = _clean_filename(str(info.get("title") or "YouTube"))
            thumb_url = (
                info.get("thumbnail")
                if isinstance(info.get("thumbnail"), str)
                else None
            )
            formats = info.get("formats")
            if not isinstance(formats, list) or not formats:
                safe_reply(message, "No formats found.")
                return

            video_opts = _pick_video_options(formats, max_height=720)
            if not video_opts:
                safe_reply(message, "No video formats found (<=720p).")
                return

            best_audio_id = _pick_best_audio_id(formats)
            if not best_audio_id:
                safe_reply(message, "No audio formats found.")
                return

            token = secrets.token_hex(4)
            _YT_PENDING[token] = {
                "ts": time.time(),
                "chat_id": int(message.chat.id),
                "user_id": int(user.id),
                "url": url,
                "title": title,
                "thumb": thumb_url,
                "video_opts": video_opts,
                "best_audio_id": best_audio_id,
                "max_bytes": int(cfg.youtube.max_file_mb) * 1024 * 1024,
                "download_dir": str(cfg.youtube.download_dir),
            }

            _send_picker(
                message,
                title=title,
                thumb_url=thumb_url,
                token=token,
                video_opts=video_opts,
            )

        except Exception as e:
            safe_reply(message, "Error: " + _friendly_auth_error(str(e)))

    @bot.callback_query_handler(
        func=lambda call: str(getattr(call, "data", "")).startswith("yt:")
    )
    def cb_youtube(call):
        try:
            data = str(call.data or "")
            parts = data.split(":")
            if len(parts) < 3:
                bot.answer_callback_query(call.id, "Invalid selection.")
                return

            token = parts[1]
            action = parts[2]
            value = parts[3] if len(parts) >= 4 else ""

            st = _YT_PENDING.get(token)
            if not st:
                bot.answer_callback_query(
                    call.id, "This request expired. Send /youtube again."
                )
                return

            if int(st.get("chat_id", -1)) != int(call.message.chat.id):
                bot.answer_callback_query(call.id, "Wrong chat.")
                return
            if int(st.get("user_id", -1)) != int(call.from_user.id):
                bot.answer_callback_query(
                    call.id, "Only the requester can use these buttons."
                )
                return
            if time.time() - float(st.get("ts", 0)) > _PENDING_TTL_SEC:
                _YT_PENDING.pop(token, None)
                bot.answer_callback_query(
                    call.id, "This request expired. Send /youtube again."
                )
                return

            if action == "x":
                _YT_PENDING.pop(token, None)
                bot.answer_callback_query(call.id, "Cancelled.")
                try:
                    bot.edit_message_caption(
                        chat_id=call.message.chat.id,
                        message_id=call.message.message_id,
                        caption="Cancelled.",
                    )
                except Exception:
                    pass
                return

            url = str(st["url"])
            title = str(st["title"])
            max_bytes = int(st["max_bytes"])
            download_root = str(st["download_dir"])
            best_audio_id = str(st["best_audio_id"])

            os.makedirs(download_root, exist_ok=True)
            tmpdir = tempfile.mkdtemp(prefix="yt_", dir=download_root)

            try:
                bot.answer_callback_query(call.id, "Starting download…")

                ydl_opts: dict[str, Any] = {
                    "outtmpl": os.path.join(tmpdir, "%(title).200s.%(ext)s"),
                    "noplaylist": True,
                    "quiet": True,
                    "no_warnings": True,
                    "no_color": True,
                    "socket_timeout": 20,
                    "retries": 5,
                    "fragment_retries": 10,
                    "max_filesize": max_bytes,
                    "extractor_args": {
                        "youtube": {
                            "player_client": ["android", "web", "tv_embedded", "ios"]
                        }
                    },
                }
                _apply_auth_opts(ydl_opts, cfg)

                send_mode = "video"
                out_ext = "mp4"

                if action == "v":
                    video_id = value
                    if not video_id:
                        raise RuntimeError("Missing video format id")

                    # video-only + best-audio, merge/remux/convert to mp4
                    ydl_opts["format"] = f"{video_id}+{best_audio_id}"
                    ydl_opts["merge_output_format"] = "mp4"
                    ydl_opts["postprocessors"] = [
                        {"key": "FFmpegVideoRemuxer", "preferedformat": "mp4"},
                        {"key": "FFmpegVideoConvertor", "preferedformat": "mp4"},
                    ]
                    send_mode = "video"
                    out_ext = "mp4"

                elif action == "a":
                    choice = value.lower().strip()
                    ydl_opts["format"] = best_audio_id
                    send_mode = "audio"

                    if choice == "mp3":
                        out_ext = "mp3"
                        ydl_opts["postprocessors"] = [
                            {
                                "key": "FFmpegExtractAudio",
                                "preferredcodec": "mp3",
                                "preferredquality": "192",
                            }
                        ]
                    else:
                        out_ext = "m4a"
                        ydl_opts["postprocessors"] = [
                            {
                                "key": "FFmpegExtractAudio",
                                "preferredcodec": "m4a",
                                "preferredquality": "0",
                            }
                        ]
                    ydl_opts["prefer_ffmpeg"] = True
                else:
                    raise RuntimeError("Unknown action")

                with YoutubeDL(ydl_opts) as ydl:
                    info2 = ydl.extract_info(url, download=True)

                if isinstance(info2, dict) and info2.get("entries"):
                    entry2 = next((e for e in info2["entries"] if e), None)
                    if isinstance(entry2, dict):
                        info2 = entry2

                if not isinstance(info2, dict):
                    raise RuntimeError("Unexpected downloader result")

                out_path = _pick_output_file(info2, tmpdir)
                if not out_path or not os.path.isfile(out_path):
                    raise RuntimeError("No output file produced")

                size = os.path.getsize(out_path)
                if size <= 0:
                    raise RuntimeError("Empty output file")
                if size > max_bytes:
                    raise RuntimeError(
                        f"File too large to send ({size / 1024 / 1024:.1f}MB). "
                        f"Limit is {max_bytes / 1024 / 1024:.0f}MB."
                    )

                base_title = _clean_filename(str(info2.get("title") or title))
                send_name = f"{base_title}.{out_ext}"
                send_path = os.path.join(tmpdir, send_name)

                if os.path.abspath(out_path) != os.path.abspath(send_path):
                    try:
                        os.replace(out_path, send_path)
                    except Exception:
                        send_path = out_path

                with open(send_path, "rb") as f:
                    if send_mode == "audio":
                        bot.send_audio(call.message.chat.id, f, caption=base_title)
                    else:
                        bot.send_video(call.message.chat.id, f, caption=base_title)

                try:
                    bot.delete_message(call.message.chat.id, call.message.message_id)
                except Exception:
                    pass

                _YT_PENDING.pop(token, None)

            except Exception as e:
                bot.send_message(
                    call.message.chat.id,
                    "Download error: " + _friendly_auth_error(str(e)),
                )
            finally:
                try:
                    shutil.rmtree(tmpdir, ignore_errors=True)
                except Exception:
                    pass

        except Exception:
            try:
                bot.answer_callback_query(call.id, "Error.")
            except Exception:
                pass
