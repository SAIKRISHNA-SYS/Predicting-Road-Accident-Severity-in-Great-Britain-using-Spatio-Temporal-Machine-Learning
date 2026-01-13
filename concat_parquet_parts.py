"""
Concatenate Parquet part files (train_parts/test_parts) into single train.parquet and test.parquet,
and save them directly to the user's Downloads folder.

🪟 Designed for Windows + VS Code
Run normally with Ctrl+Alt+N or the green Run button.
"""

import pyarrow.parquet as pq
from pathlib import Path
import shutil
import sys

# ------------------------------
# AUTO-DETECT DOWNLOADS FOLDER
# ------------------------------
downloads_dir = Path.home() / "Downloads"
downloads_dir.mkdir(exist_ok=True)
print(f"\n Output files will be saved to: {downloads_dir}\n")

# ------------------------------
# Utility functions
# ------------------------------
def get_parts(dirpath: Path):
    """Return sorted list of parquet part files in directory."""
    if not dirpath.exists() or not dirpath.is_dir():
        return []
    return sorted(dirpath.glob("*.parquet"))

def bytes_to_readable(n: int) -> str:
    """Convert bytes to human-readable string."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if n < 1024:
            return f"{n:3.1f}{unit}"
        n /= 1024
    return f"{n:.1f}PB"

def concat_parts(parts, out_file: Path):
    """Merge multiple parquet parts into a single parquet file."""
    if not parts:
        print("  No part files found.")
        return False

    total_bytes = sum(p.stat().st_size for p in parts)
    free_bytes = shutil.disk_usage(out_file.parent).free
    print(f" Found {len(parts)} parts")
    print(f"Total size: {bytes_to_readable(total_bytes)} | Free space: {bytes_to_readable(free_bytes)}")

    if free_bytes < total_bytes * 0.5:
        print("  Warning: low disk space — merging may fail if space runs out.")

    # Read first part for schema
    first = parts[0]
    print(f"Reading schema from: {first.name}")
    first_table = pq.read_table(first)
    schema = first_table.schema

    writer = pq.ParquetWriter(out_file, schema, compression="snappy")

    try:
        print(f"Writing {first.name} → {out_file.name}")
        writer.write_table(first_table)

        for p in parts[1:]:
            print(f"Appending {p.name} ...")
            table = pq.read_table(p)
            if not table.schema.equals(schema):
                try:
                    table = table.cast(schema)
                except Exception:
                    print(f"⚠️  Schema mismatch for {p.name}, aligning columns...")
                    common = [n for n in schema.names if n in table.column_names]
                    table = table.select(common)
            writer.write_table(table)
    finally:
        writer.close()

    print(f" Done: {out_file} ({bytes_to_readable(out_file.stat().st_size)})")
    return True

# ------------------------------
# Main function
# ------------------------------
def main():
    print(" Enter the full path to your folder containing 'train_parts' and 'test_parts':")
    input_dir = Path(input("Path: ").strip('"').strip())

    if not input_dir.exists():
        print(f" Directory not found: {input_dir}")
        sys.exit(1)

    splits = {
        "train_parts": downloads_dir / "train.parquet",
        "test_parts": downloads_dir / "test.parquet"
    }

    for split_dir, out_path in splits.items():
        dir_path = input_dir / split_dir
        print(f"\n=== Processing '{split_dir}' from {dir_path} ===")
        parts = get_parts(dir_path)
        if not parts:
            print(f"⚠️  No parquet parts found in {dir_path}, skipping.")
            continue
        concat_parts(parts, out_path)

    print("\n All merges complete! Files saved to your Downloads folder:")
    print(f"   • {downloads_dir / 'train.parquet'}")
    print(f"   • {downloads_dir / 'test.parquet'}")

# ------------------------------
# Run
# ------------------------------
if __name__ == "__main__":
    main()
