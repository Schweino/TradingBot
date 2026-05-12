"""Pre-hunt Step 2 cache gate.

This is a thin CLI wrapper around step2_manifest_resolver. It exists so long
hunt jobs can run a single explicit cache check before doing any scoring work.
"""
from __future__ import annotations

import argparse
import json

import step2_manifest_resolver


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Verify the fastest safe certified Step 2 cache before hunting.")
    ap.add_argument("--start", default=step2_manifest_resolver.DEFAULT_START)
    ap.add_argument("--end", default=step2_manifest_resolver.DEFAULT_END)
    ap.add_argument("--tickers", nargs="*", default=step2_manifest_resolver.DEFAULT_TICKERS)
    ap.add_argument("--compiled-manifest", default="")
    ap.add_argument("--compiled-dir", default=str(step2_manifest_resolver.COMPILED_DIR))
    ap.add_argument("--name", default="")
    ap.add_argument("--linked-name", default="")
    ap.add_argument("--label", default="pre_hunt")
    ap.add_argument("--no-link", dest="allow_link", action="store_false")
    ap.set_defaults(allow_link=True)
    ap.add_argument("--no-prefer-linked", dest="prefer_linked", action="store_false")
    ap.set_defaults(prefer_linked=True)
    ap.add_argument("--no-recertify", dest="allow_recertify", action="store_false")
    ap.set_defaults(allow_recertify=True)
    ap.add_argument("--feed", default="sip")
    ap.add_argument("--quote-mode", default="per-second")
    ap.add_argument("--btc-mode", default="bars")
    ap.add_argument("--indicator-mode", default="live")
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--json", action="store_true")
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    payload = step2_manifest_resolver.resolve_best_manifest(
        start=args.start,
        end=args.end,
        tickers=args.tickers,
        compiled_manifest=args.compiled_manifest or None,
        compiled_dir=args.compiled_dir,
        name=args.name,
        linked_manifest_name=args.linked_name,
        prefer_linked=args.prefer_linked,
        allow_link=args.allow_link,
        allow_recertify=args.allow_recertify,
        require_certified=True,
        write_certification=True,
        write_receipt=True,
        label=args.label,
        feed=args.feed,
        quote_mode=args.quote_mode,
        btc_mode=args.btc_mode,
        indicator_mode=args.indicator_mode,
        workers=args.workers,
    )
    print(json.dumps(payload if args.json else step2_manifest_resolver.compact_resolution(payload), indent=2, sort_keys=True, default=str))
    return 0 if payload.get("ok") and (payload.get("pre_hunt_gate") or {}).get("ok") else 2


if __name__ == "__main__":
    raise SystemExit(main())
