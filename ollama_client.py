import os
import requests
from typing import Any


class OllamaClient:
    """Minimal Ollama /api/chat client with sane defaults."""

    def __init__(
        self,
        base_url: str | None = None,
        model: str | None = None,
        timeout_s: int | None = None,
    ):
        self.base_url = (base_url or os.getenv("OLLAMA_URL", "http://127.0.0.1:11434")).rstrip(
            "/"
        )
        self.model = model or os.getenv("OLLAMA_MODEL", "gemma3:1b")
        self.timeout_s = int(timeout_s or os.getenv("OLLAMA_TIMEOUT_S", "180"))
        self.session = requests.Session()

    def chat(self, messages: list[dict[str, Any]], timeout_s: int | None = None) -> str:
        """
        Calls Ollama /api/chat with stream=false.
        """
        url = f"{self.base_url}/api/chat"
        payload = {"model": self.model, "messages": messages, "stream": False}
        # connect timeout 5s, read timeout timeout_s
        read_timeout = int(timeout_s or self.timeout_s)
        r = self.session.post(url, json=payload, timeout=(5, read_timeout))
        r.raise_for_status()
        data = r.json()
        return (data.get("message") or {}).get("content", "").strip()
