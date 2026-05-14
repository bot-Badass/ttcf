#!/usr/bin/env python3
"""
TTCF Dashboard — Web UI for managing the content factory.
Usage: python dashboard.py [--port 8000] [--host 0.0.0.0]
"""
import os
import sys
from pathlib import Path

# Load .env
env_path = Path(__file__).parent / ".env"
if env_path.is_file():
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            key, _, value = line.partition("=")
            value = value.strip().strip('"').strip("'")
            os.environ.setdefault(key.strip(), value)

sys.path.insert(0, str(Path(__file__).parent))

import argparse
import uvicorn

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TTCF Dashboard")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--reload", action="store_true")
    args = parser.parse_args()

    print(f"\n🚀 TTCF Dashboard → http://{args.host}:{args.port}\n")
    uvicorn.run(
        "src.dashboard.app:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
    )
