import os
import stat
from cryptography.fernet import Fernet  # Authenticated encryption (AES in CBC + HMAC, via Fernet)

KEY_FILE = "token.key"
TOKEN_FILE = "token.txt"


def _chmod_600(path: str) -> None:
    try:
        os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)  # 0o600
    except Exception:
        # On Windows or restricted FS, chmod may fail; ignore but keep running.
        pass


def get_or_create_fernet() -> Fernet:
    """
    Best practice: supply BOT_TOKEN_KEY as a Fernet key (base64 urlsafe 32-byte).
    If absent, we create token.key locally (chmod 600).
    """
    env_key = os.getenv("BOT_TOKEN_KEY")
    if env_key:
        return Fernet(env_key.encode("utf-8"))

    if os.path.exists(KEY_FILE):
        key = open(KEY_FILE, "rb").read().strip()
        return Fernet(key)

    key = Fernet.generate_key()
    with open(KEY_FILE, "wb") as f:
        f.write(key)
    _chmod_600(KEY_FILE)
    return Fernet(key)


def save_encrypted_token(token: str) -> None:
    fernet = get_or_create_fernet()
    ct = fernet.encrypt(token.encode("utf-8"))
    with open(TOKEN_FILE, "wb") as f:
        f.write(ct)
    _chmod_600(TOKEN_FILE)


def load_decrypted_token() -> str | None:
    if not os.path.exists(TOKEN_FILE):
        return None
    fernet = get_or_create_fernet()
    ct = open(TOKEN_FILE, "rb").read().strip()
    try:
        return fernet.decrypt(ct).decode("utf-8")
    except Exception as e:
        raise RuntimeError(
            "Failed to decrypt token.txt. If you changed BOT_TOKEN_KEY or token.key, restore the original."
        ) from e
