import json
import os
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class OllamaConfig:
    url: str = "http://127.0.0.1:11434"
    model: str = "gemma3:1b"
    timeout_s: int = 180


@dataclass(frozen=True)
class YouTubeConfig:
    download_dir: str = "downloads"
    max_file_mb: int = 45
    default_mode: str = "audio"  # 'audio' or 'video'


@dataclass(frozen=True)
class BotConfig:
    # Cooldown for normal users (admins bypass)
    rate_limit_seconds: float = 1.5


@dataclass(frozen=True)
class Config:
    ollama: OllamaConfig = OllamaConfig()
    youtube: YouTubeConfig = YouTubeConfig()
    bot: BotConfig = BotConfig()


def _deep_get(d: dict[str, Any], path: list[str], default: Any = None) -> Any:
    cur: Any = d
    for p in path:
        if not isinstance(cur, dict) or p not in cur:
            return default
        cur = cur[p]
    return cur


def load_config(path: str = "config.json") -> Config:
    """
    Loads settings from config.json (if present) and allows env var overrides.

    Env overrides (optional):
      - OLLAMA_URL
      - OLLAMA_MODEL
      - OLLAMA_TIMEOUT_S
    """
    data: dict[str, Any] = {}
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f) or {}
        except Exception:
            data = {}

    ollama = OllamaConfig(
        url=str(_deep_get(data, ["ollama", "url"], OllamaConfig.url)),
        model=str(_deep_get(data, ["ollama", "model"], OllamaConfig.model)),
        timeout_s=int(_deep_get(data, ["ollama", "timeout_s"], OllamaConfig.timeout_s)),
    )
    youtube = YouTubeConfig(
        download_dir=str(
            _deep_get(data, ["youtube", "download_dir"], YouTubeConfig.download_dir)
        ),
        max_file_mb=int(_deep_get(data, ["youtube", "max_file_mb"], YouTubeConfig.max_file_mb)),
        default_mode=str(_deep_get(data, ["youtube", "default_mode"], YouTubeConfig.default_mode)),
    )
    bot = BotConfig(
        rate_limit_seconds=float(
            _deep_get(data, ["bot", "rate_limit_seconds"], BotConfig.rate_limit_seconds)
        )
    )

    # Env overrides (keep existing JSON defaults)
    ollama = OllamaConfig(
        url=os.getenv("OLLAMA_URL", ollama.url),
        model=os.getenv("OLLAMA_MODEL", ollama.model),
        timeout_s=int(os.getenv("OLLAMA_TIMEOUT_S", str(ollama.timeout_s))),
    )

    return Config(ollama=ollama, youtube=youtube, bot=bot)
