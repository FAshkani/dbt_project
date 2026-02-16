#!/usr/bin/env python3
"""
Patch dbt manifest adapter for dbt-colibri on Fabric projects.

Usage (PowerShell example):
  python tools\patch_manifest_adapter.py `
    --src target\manifest.json `
    --out target\manifest.patched.json `
    --from fabric `
    --to sqlserver `
    --backup target\manifest.fabric.json

Follow with:
  colibri generate --manifest target\manifest.patched.json --catalog target\catalog.json --output-dir dist\
"""

from __future__ import annotations
import argparse
import json
import sys
from pathlib import Path

def main() -> int:
    ap = argparse.ArgumentParser(description="Patch dbt manifest adapter_type for dbt-colibri.")
    ap.add_argument("--src", default="target/manifest.json", help="Path to the original manifest.json")
    ap.add_argument("--out", default="target/manifest.patched.json", help="Path to write the patched manifest")
    ap.add_argument("--backup", default="", help="Optional backup path for the original manifest")
    ap.add_argument("--from", dest="from_adapter", default="fabric", help="Current adapter name to replace")
    ap.add_argument("--to", dest="to_adapter", default="sqlserver", help="New adapter name to set")
    ap.add_argument("--pretty", action="store_true", help="Pretty-print JSON output")
    args = ap.parse_args()

    src_path = Path(args.src)
    out_path = Path(args.out)
    backup_path = Path(args.backup) if args.backup else None

    if not src_path.exists():
        print(f"[ERROR] Source manifest not found: {src_path}", file=sys.stderr)
        return 1

    try:
        data = json.loads(src_path.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"[ERROR] Failed to read/parse JSON: {src_path}\n{e}", file=sys.stderr)
        return 1

    meta = data.get("metadata", {})
    before_adapter_type = meta.get("adapter_type")
    before_adapter_legacy = meta.get("adapter")  # older manifests

    changed = False
    if before_adapter_type == args.from_adapter:
        meta["adapter_type"] = args.to_adapter
        changed = True
    if before_adapter_legacy == args.from_adapter:
        meta["adapter"] = args.to_adapter
        changed = True
    if not data.get("metadata"):
        data["metadata"] = {"adapter_type": args.to_adapter}
        changed = True

    if changed:
        print(f"[INFO] Patched: adapter_type '{before_adapter_type}' -> '{meta.get('adapter_type')}', "
              f"adapter '{before_adapter_legacy}' -> '{meta.get('adapter')}'")
    else:
        print(f"[INFO] No changes needed. adapter_type='{before_adapter_type}', adapter='{before_adapter_legacy}'")

    try:
        if backup_path:
            backup_path.parent.mkdir(parents=True, exist_ok=True)
            backup_path.write_text(json.dumps(data, indent=2 if args.pretty else None), encoding="utf-8")
            print(f"[INFO] Backup written: {backup_path}")
    except Exception as e:
        print(f"[WARN] Failed to write backup: {e}", file=sys.stderr)

    try:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        content = json.dumps(data, indent=2) if args.pretty else json.dumps(data, separators=(",", ":"))
        out_path.write_text(content, encoding="utf-8")
        print(f"[OK] Patched manifest written: {out_path}")
        return 0
    except Exception as e:
        print(f"[ERROR] Failed to write patched manifest: {e}", file=sys.stderr)
        return 1

if __name__ == "__main__":
    raise SystemExit(main())
