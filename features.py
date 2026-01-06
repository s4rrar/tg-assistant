from __future__ import annotations

import importlib.util
import os
from typing import Any

from db import DB
from config import Config

# ------------------------------------------------------------
# Feature modules live in ./features/<name>.py
#
# When adding a new feature:
#   1) Create ./features/<name>.py
#   2) Implement FEATURE = {...} and a register(...) function
#   3) Add "<name>" to FEATURE_MODULE_NAMES below
#   4) Admin can /reload to pick up the new feature code

FEATURE_MODULE_NAMES = [
    "youtube",
]

FEATURES_DIR = os.path.join(os.path.dirname(__file__), "features")


def _load_feature_module(name: str):
    path = os.path.join(FEATURES_DIR, f"{name}.py")
    if not os.path.exists(path):
        raise FileNotFoundError(f"Feature file not found: {path}")

    spec = importlib.util.spec_from_file_location(f"feature_{name}", path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load feature: {name}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_feature_modules() -> tuple[list[Any], list[str]]:
    modules: list[Any] = []
    errors: list[str] = []
    for name in FEATURE_MODULE_NAMES:
        try:
            modules.append(_load_feature_module(str(name).strip()))
        except Exception as e:
            errors.append(f"{name}: {e}")
    return modules, errors


def _fmt_feature_row(r) -> str:
    name = str(r["name"])
    scope = str(r["scope"])
    enabled = int(r["enabled"]) == 1
    desc = (r["description"] or "").strip()
    commands = (r["commands"] or "").strip()
    cmd_show = ""
    if commands:
        first = commands.split(",")[0].strip()
        if first:
            cmd_show = f" /{first}"
    status = "ENABLED" if enabled else "disabled"
    tail = []
    if scope:
        tail.append(scope)
    if desc:
        tail.append(desc)
    return f"• {name}{cmd_show} — {status} ({'; '.join(tail)})".strip()


def init_features(db: DB, modules: list[Any]) -> None:
    """Ensure all known features exist in DB (keeps enabled values if already set)."""
    for m in modules:
        feature = getattr(m, "FEATURE", None) or {}
        name = str(feature.get("name") or "").strip().lower()
        if not name:
            continue
        db.ensure_feature(
            name=name,
            scope=str(feature.get("scope") or "user").strip().lower(),
            description=str(feature.get("description") or "").strip(),
            commands=[str(c) for c in (feature.get("commands") or [])],
            enabled_default=True,
        )


def register_features(
    bot,
    db: DB,
    cfg: Config,
    *,
    safe_reply,
    require_admin,
) -> None:
    """Register core feature admin/user commands + feature modules."""

    modules, load_errors = load_feature_modules()

    # Ensure feature rows exist
    init_features(db, modules)

    # ---------------- user/admin: /features ----------------
    @bot.message_handler(commands=["features"])
    def cmd_features(message):
        user = message.from_user
        if not user:
            return
        is_admin = db.is_admin(user.id)

        if is_admin:
            rows = db.list_features()
            if not rows:
                safe_reply(message, "No features registered.")
                return

            hdr = [
                f"Features (global_enabled={db.features_global_enabled()}):",
                "",
            ]
            if load_errors:
                hdr.append("Load errors:")
                hdr.extend([f"• {e}" for e in load_errors])
                hdr.append("")

            safe_reply(
                message,
                ("\n".join(hdr + [_fmt_feature_row(r) for r in rows]))[:3800],
            )
            return

        # Normal user: show only enabled user features (and respect global)
        if not db.features_global_enabled():
            safe_reply(message, "All features are currently disabled by admin.")
            return

        rows = [
            r
            for r in db.list_features()
            if str(r["scope"]) == "user" and int(r["enabled"]) == 1
        ]
        if not rows:
            safe_reply(message, "No enabled features.")
            return

        lines = [
            "Enabled features:",
            "",
        ]
        for r in rows:
            # show name + command + description (no status for users)
            name = str(r["name"])
            desc = (r["description"] or "").strip()
            commands = (r["commands"] or "").strip()
            cmd_show = ""
            if commands:
                first = commands.split(",")[0].strip()
                if first:
                    cmd_show = f"/{first}"
            if desc:
                lines.append(f"• {name} ({cmd_show}) — {desc}".strip())
            else:
                lines.append(f"• {name} ({cmd_show})".strip())

        safe_reply(message, ("\n".join(lines))[:3800])

    # ---------------- admin: /enabled_features, /disabled_features ----------------
    @bot.message_handler(commands=["enabled_features"])
    def cmd_enabled_features(message):
        if not require_admin(message):
            return
        rows = db.list_features_by_enabled(True)
        if not rows:
            safe_reply(message, "No enabled features.")
            return
        safe_reply(
            message,
            ("Enabled features:\n\n" + "\n".join(_fmt_feature_row(r) for r in rows))[
                :3800
            ],
        )

    @bot.message_handler(commands=["disabled_features"])
    def cmd_disabled_features(message):
        if not require_admin(message):
            return
        rows = db.list_features_by_enabled(False)
        if not rows:
            safe_reply(message, "No disabled features.")
            return
        safe_reply(
            message,
            ("Disabled features:\n\n" + "\n".join(_fmt_feature_row(r) for r in rows))[
                :3800
            ],
        )

    # ---------------- admin: toggle per-feature ----------------
    @bot.message_handler(commands=["feature_enable", "feature_disable"])
    def cmd_feature_toggle(message):
        if not require_admin(message):
            return
        parts = (message.text or "").split(maxsplit=1)
        if len(parts) < 2:
            safe_reply(
                message,
                "Usage: /feature_enable <name> OR /feature_disable <name>",
            )
            return
        name = parts[1].strip().lower()
        r = db.get_feature(name)
        if not r:
            safe_reply(message, f"Unknown feature: {name}")
            return
        enabled = message.text.strip().startswith("/feature_enable")
        db.set_feature_enabled(name, enabled)
        safe_reply(message, f"Feature '{name}' enabled={enabled}")

    # ---------------- admin: global toggle ----------------
    @bot.message_handler(commands=["features_enable_all", "features_disable_all"])
    def cmd_features_global_toggle(message):
        if not require_admin(message):
            return
        enabled = message.text.strip().startswith("/features_enable_all")
        db.set_features_global_enabled(enabled)
        safe_reply(message, f"Global features enabled={db.features_global_enabled()}")

    # Register individual feature modules
    for m in modules:
        register_fn = getattr(m, "register", None)
        if callable(register_fn):
            register_fn(
                bot,
                db,
                cfg,
                safe_reply=safe_reply,
                require_admin=require_admin,
            )
