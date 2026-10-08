"""Stateful reference run for a reproducible two-microbatch synthetic incident."""
import argparse
import json
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from temporalx.stateful import demo  # noqa: E402


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="artifacts/stateful-s03.db")
    parser.add_argument("--fixtures", default=str(ROOT / "fixtures" / "streaming"))
    args = parser.parse_args()
    print(json.dumps(demo(args.db, args.fixtures), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
