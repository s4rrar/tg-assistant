import os
import requests
from typing import Any


class OllamaClient:
    """Minimal Ollama /api/chat client with sane defaults."""

    def __init__(self):
        self.base_url = os.getenv("OLLAMA_URL", "http://127.0.0.1:11434").rstrip("/")
        self.model = os.getenv("OLLAMA_MODEL", "gemma3:1b")
        self.session = requests.Session()

    def chat(self, messages: list[dict[str, Any]], timeout_s: int = 180) -> str:
        """
        Calls Ollama /api/chat with stream=false.
        """
        url = f"{self.base_url}/api/chat"
        payload = {"model": self.model, "messages": messages, "stream": False}
        # connect timeout 5s, read timeout timeout_s
        r = self.session.post(url, json=payload, timeout=(5, timeout_s))
        r.raise_for_status()
        data = r.json()
        return (data.get("message") or {}).get("content", "").strip()
