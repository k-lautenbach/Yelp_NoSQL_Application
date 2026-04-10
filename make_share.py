"""
make_share.py
-------------
Copies a clean shareable subset of hw6 into ../hw6_share/

Included:
  - *.py files from the hw6 root
  - README.md and .gitignore (if present)
  - Yelp-JSON/filtered_indianapolis/*.json

Excluded:
  - Yelp-JSON/Yelp JSON/  (full raw dataset)
  - *.tar, *.zip
  - __MACOSX/
  - .idea/, .vscode/, __pycache__/, *.pyc
"""

import os
import shutil

SRC  = os.path.dirname(os.path.abspath(__file__))           # hw6/
DEST = os.path.join(os.path.dirname(SRC), "hw6_share")     # ../hw6_share/

# ── Wipe and recreate destination ─────────────────────────────────────────────
if os.path.exists(DEST):
    shutil.rmtree(DEST)
os.makedirs(DEST)

copied = []

# ── 1. Python scripts, README.md, .gitignore from hw6 root ───────────────────
for name in os.listdir(SRC):
    src_path = os.path.join(SRC, name)
    if not os.path.isfile(src_path):
        continue
    if name.endswith(".py") or name in ("README.md", ".gitignore"):
        dst_path = os.path.join(DEST, name)
        shutil.copy2(src_path, dst_path)
        copied.append(os.path.relpath(dst_path, DEST))

# ── 2. Yelp-JSON/filtered_indianapolis/*.json ─────────────────────────────────
filtered_src = os.path.join(SRC, "Yelp-JSON", "filtered_indianapolis")
filtered_dst = os.path.join(DEST, "Yelp-JSON", "filtered_indianapolis")
os.makedirs(filtered_dst)

for name in sorted(os.listdir(filtered_src)):
    if name.endswith(".json"):
        shutil.copy2(
            os.path.join(filtered_src, name),
            os.path.join(filtered_dst, name),
        )
        copied.append(os.path.join("Yelp-JSON", "filtered_indianapolis", name))

# ── Print result tree ─────────────────────────────────────────────────────────
print(f"hw6_share/  ({DEST})")
for root, dirs, files in os.walk(DEST):
    # Sort for deterministic output
    dirs.sort()
    files.sort()
    depth  = root.replace(DEST, "").count(os.sep)
    indent = "    " * depth
    folder = os.path.basename(root)
    if root != DEST:
        print(f"{indent}{folder}/")
    file_indent = "    " * (depth + 1)
    for f in files:
        size = os.path.getsize(os.path.join(root, f))
        size_str = (f"{size / 1_048_576:.1f} MB" if size >= 1_048_576
                    else f"{size / 1024:.1f} KB" if size >= 1024
                    else f"{size} B")
        print(f"{file_indent}{f}  ({size_str})")

print(f"\n{len(copied)} files copied.")
