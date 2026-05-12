from __future__ import annotations

import argparse
import json

from scoring_profiles import write_profile_from_lab_result


def main() -> int:
    ap = argparse.ArgumentParser(description='Extract a replay scoring profile from scoring_variant_lab_fast output.')
    ap.add_argument('--lab-json', required=True)
    ap.add_argument('--variant', default=None, help='Variant name. Defaults to top result in the lab JSON.')
    ap.add_argument('--out', required=True)
    args = ap.parse_args()
    profile = write_profile_from_lab_result(args.lab_json, args.out, args.variant)
    print(json.dumps({'out': args.out, 'profile': profile.get('name')}, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
