"""Client for the resident Step 2 scorer service."""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import step2_manifest_resolver
import step2_score_cache


HERE = Path(__file__).resolve().parent


class ResidentScorerClient:
    def __init__(
        self,
        *,
        manifest_path: str = "",
        start: str = step2_manifest_resolver.DEFAULT_START,
        end: str = step2_manifest_resolver.DEFAULT_END,
        tickers: list[str] | None = None,
        cache_db: str = str(step2_score_cache.DEFAULT_CACHE_DB),
        start_balance: float = 100000.0,
        day_subset: list[str] | None = None,
    ):
        cmd = [
            sys.executable,
            str(HERE / "step2_scorer_service.py"),
            "--start", start,
            "--end", end,
            "--tickers", *(tickers or step2_manifest_resolver.DEFAULT_TICKERS),
            "--cache-db", cache_db,
            "--start-balance", str(float(start_balance)),
        ]
        if manifest_path:
            cmd.extend(["--compiled-manifest", manifest_path])
        if day_subset:
            cmd.extend(["--day-subset", *[str(day) for day in day_subset]])
        self.cmd = [str(part) for part in cmd]
        self.proc = subprocess.Popen(
            self.cmd,
            cwd=HERE,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        self.ready = self._read_json_line()
        if not self.ready.get("ok"):
            self.close()
            raise RuntimeError(f"resident scorer failed to start: {self.ready}")

    def _read_json_line(self) -> dict[str, Any]:
        if self.proc.stdout is None:
            raise RuntimeError("resident scorer stdout unavailable")
        line = self.proc.stdout.readline()
        if not line:
            stderr = self.proc.stderr.read() if self.proc.stderr else ""
            raise RuntimeError(f"resident scorer exited before response: {stderr[-2000:]}")
        return json.loads(line)

    def request(self, payload: dict[str, Any]) -> dict[str, Any]:
        if self.proc.stdin is None:
            raise RuntimeError("resident scorer stdin unavailable")
        self.proc.stdin.write(json.dumps(payload, sort_keys=True, default=str) + "\n")
        self.proc.stdin.flush()
        return self._read_json_line()

    def score(
        self,
        variants: list[dict[str, Any]],
        *,
        raw: bool = False,
        summary: bool = False,
        preserve_order: bool = False,
    ) -> dict[str, Any]:
        return self.request({
            "cmd": "score",
            "variants": variants,
            "raw": raw,
            "summary": summary,
            "preserve_order": preserve_order,
        })

    def active(self) -> dict[str, Any]:
        return self.request({"cmd": "active"})

    def stats(self) -> dict[str, Any]:
        return self.request({"cmd": "stats"})

    def close(self) -> None:
        try:
            if self.proc.poll() is None:
                try:
                    self.request({"cmd": "shutdown"})
                except Exception:
                    pass
                self.proc.terminate()
        finally:
            try:
                self.proc.wait(timeout=5)
            except Exception:
                self.proc.kill()

    def __enter__(self) -> "ResidentScorerClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


def _read_variants(path: str) -> list[dict[str, Any]]:
    payload = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    rows = payload.get("variants") or payload.get("candidates") or payload.get("leaderboard") or payload.get("rows") or []
    return [row for row in rows if isinstance(row, dict)]


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Send requests to a resident Step 2 scorer service.")
    ap.add_argument("--compiled-manifest", default="")
    ap.add_argument("--start", default=step2_manifest_resolver.DEFAULT_START)
    ap.add_argument("--end", default=step2_manifest_resolver.DEFAULT_END)
    ap.add_argument("--tickers", nargs="*", default=step2_manifest_resolver.DEFAULT_TICKERS)
    ap.add_argument("--cache-db", default=str(step2_score_cache.DEFAULT_CACHE_DB))
    ap.add_argument("--start-balance", type=float, default=100000.0)
    ap.add_argument("--day-subset", nargs="*", default=[])
    ap.add_argument("--variants-json", default="")
    ap.add_argument("--active", action="store_true")
    ap.add_argument("--stats", action="store_true")
    ap.add_argument("--summary", action="store_true")
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    with ResidentScorerClient(
        manifest_path=args.compiled_manifest,
        start=args.start,
        end=args.end,
        tickers=args.tickers,
        cache_db=args.cache_db,
        start_balance=args.start_balance,
        day_subset=args.day_subset or None,
    ) as client:
        if args.active:
            payload = client.active()
        elif args.stats:
            payload = client.stats()
        else:
            payload = client.score(_read_variants(args.variants_json), summary=bool(args.summary), preserve_order=bool(args.summary))
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    return 0 if payload.get("ok") else 2


if __name__ == "__main__":
    raise SystemExit(main())
