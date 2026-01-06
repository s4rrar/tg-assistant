# DaddyGPT Telegram Bot (Ollama + gemma3:1b)

A security- and performance-focused Telegram bot built with **pyTelegramBotAPI** and **Ollama**.

## Features
- Works in **private chats** and **group/supergroup** chats.
  - In groups: the bot responds only if you **prefix with the trigger name** (default: `daddygpt`) or **@mention** it, or **reply to the bot**.
- Uses Ollama model (default: **gemma3:1b**).
- **Arabic + English** (responds in the user's language when possible).
- First run prompts:
  - Telegram **Bot Token** (stored encrypted in `token.txt`)
  - Initial **Admin ID(s)** (stored in `database.db`)
- Admin panel (Telegram commands):
  - Add/remove admins by **id** or **@username** (username becomes active after the user messages the bot once)
  - Enable/disable bot
  - Enable/disable daily 2AM backups (Asia/Hebron)
  - Ban/unban users by id or @username
  - Search users, view username/name change history
  - Export user chats (messages + bot replies)
  - Export/Import the whole SQLite DB to/from Excel
- Daily backup at **2:00 AM** (can be toggled by admins):
  - Saved to `backups/YYYY-MM-DD.xlsx`
  - Sent to all admins via Telegram

## Quick start

### 1) Install & run Ollama
- Install Ollama, then pull the model:
  - `ollama pull gemma3:1b`
- Make sure Ollama is running (default URL: `http://127.0.0.1:11434`).

Optional env vars:
- `OLLAMA_URL` (default `http://127.0.0.1:11434`)
- `OLLAMA_MODEL` (default `gemma3:1b`)
- `BOT_TOKEN_KEY` (recommended): Fernet key for token encryption.

### 2) Install Python deps
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 3) Run the bot
```bash
python main.py
```
On the first run it will ask for:
- Telegram bot token
- Initial admin user id(s) (comma-separated)

## Security notes
- `token.txt` is encrypted using **Fernet authenticated encryption**.
- For best security across restarts/machines, set `BOT_TOKEN_KEY` (a Fernet key):
  - Generate one in Python:
    ```python
    from cryptography.fernet import Fernet
    print(Fernet.generate_key().decode())
    ```
  - Export it in your shell:
    - `export BOT_TOKEN_KEY='...'`

## File layout
- `main.py` – Telegram bot entrypoint
- `db.py` – SQLite schema + thread-safe data access
- `security.py` – token encryption/decryption
- `ollama_client.py` – Ollama chat client
- `excel_io.py` – export/import DB as Excel
- `database.db` – SQLite database (auto-created if missing)
- `backups/` – scheduled + manual Excel exports

