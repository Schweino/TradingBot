from __future__ import annotations

import os
from pathlib import Path

HERE = Path(__file__).resolve().parent
DEFAULT_OUTPUT_ROOT = Path(r"C:\xampp\htdocs\BotOutput\Claude")


def output_root() -> str:
    configured = os.environ.get("TRADING_BOT_OUTPUT_ROOT")
    root = Path(configured) if configured else DEFAULT_OUTPUT_ROOT
    root.mkdir(parents=True, exist_ok=True)
    return str(root)


def output_path(*parts: object) -> str:
    path = Path(output_root()).joinpath(*(str(part) for part in parts))
    parent = path if not path.suffix else path.parent
    parent.mkdir(parents=True, exist_ok=True)
    return str(path)


def repo_path(*parts: object) -> str:
    return str(HERE.joinpath(*(str(part) for part in parts)))
