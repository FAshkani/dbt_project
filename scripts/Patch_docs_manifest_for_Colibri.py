#!/usr/bin/env python3
import json, sys
from pathlib import Path

SRC_CANDIDATES = [
    Path("target/manifest.patched.json"),
    Path("target/manifest.json"),
]
DEST = Path("target/manifest.patched.json")
FORCED = "duckdb"   # or "sqlserver" if you prefer

def main() -> int:
    src = next((p for p in SRC_CANDIDATES if p.exists()), None)
    if not src:
        print("[ERROR] No manifest found in target/: expected manifest.patched.json or manifest.json", file=sys.stderr)
        return 1

    try:
        data = json.loads(src.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"[ERROR] Failed to parse JSON {src}: {e}", file=sys.stderr)
        return 1

    md = data.setdefault("metadata", {})
    before_type = md.get("adapter_type")
    before_legacy = md.get("adapter")

    # Force to supported adapter for colibri
    md["adapter_type"] = FORCED
    if "adapter" in md:
        md["adapter"] = FORCED
    md["adapter_note"] = f"patched-from-{before_type or before_legacy}-for-colibri"

    try:
        DEST.parent.mkdir(parents=True, exist_ok=True)
        # Compact JSON to keep file size down
        DEST.write_text(json.dumps(data, separators=(",", ":")), encoding="utf-8")
    except Exception as e:
        print(f"[ERROR] Failed to write {DEST}: {e}", file=sys.stderr)
        return 1

    print(f"[OK] Patched {src.name} -> {DEST.name} (adapter_type='{FORCED}')")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
