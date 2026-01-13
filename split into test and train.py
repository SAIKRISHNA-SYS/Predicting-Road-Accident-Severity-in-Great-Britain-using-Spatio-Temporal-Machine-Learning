"""
Concatenate Parquet part files (train_parts/test_parts) into single train.parquet and test.parquet,
and save them directly to the user's Downloads folder.

🪟 Non-interactive version for VS Code on Windows
"""

import pyarrow.parquet as pq
from pathlib import Path
import shutil
import sys

# ------------------------------
# 1️⃣ Define input and output paths
# ------------------------------
input_dir = Path(r"C:\Users\saikr\Downloads\filtered_parquet")
downloads_dir = Path.home() / "Downloads"
downloads_dir.mkdir(exist_ok=True)
print(f"\nOutput files will be saved to: {downloads_dir}\n")

# ------------------------------
# 2️⃣ Helper functions
# ------------------------------
def get_parts(dirpath: Path):
    if not dirpath.exists() or not dirpath.is_dir():
        return []
    return sorted(dirpath.glob("*.parquet"))

def bytes_to_readable(n: int) -> str:
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if n < 1024:
            return f"{n:3.1f}{unit}"
        n /= 1024
    return f"{n:.1f}PB"

def concat_parts(parts, out_file: Path):
    if not parts:
        print(f"Warning: No part files found for {out_file.name}.")
        return False

    total_bytes = sum(p.stat().st_size for p in parts)
    free_bytes = shutil.disk_usage(out_file.parent).free
    print(f"Found {len(parts)} parts for {out_file.name}")
    print(f"Total size: {bytes_to_readable(total_bytes)} | Free space: {bytes_to_readable(free_bytes)}")

    if free_bytes < total_bytes * 0.5:
        print("Warning: low disk space — merging may fail if space runs out.")

    first = parts[0]
    print(f"Reading schema from: {first.name}")
    first_table = pq.read_table(first)
    schema = first_table.schema

    writer = pq.ParquetWriter(out_file, schema, compression="snappy")

    try:
        print(f"Writing {first.name} -> {out_file.name}")
        writer.write_table(first_table)

        for p in parts[1:]:
            print(f"Appending {p.name} ...")
            table = pq.read_table(p)
            if not table.schema.equals(schema):
                try:
                    table = table.cast(schema)
                except Exception:
                    print(f"Warning: Schema mismatch for {p.name}, aligning columns...")
                    common = [n for n in schema.names if n in table.column_names]
                    table = table.select(common)
            writer.write_table(table)
    finally:
        writer.close()

    print(f"Done: {out_file} ({bytes_to_readable(out_file.stat().st_size)})")
    return True

# ------------------------------
# 3️⃣ Main
# ------------------------------
def main():
    if not input_dir.exists():
        print(f"Error: Input directory not found: {input_dir}")
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
            print(f"Warning: No parquet parts found in {dir_path}, skipping.")
            continue
        concat_parts(parts, out_path)

    print("\nAll merges complete! Files saved to your Downloads folder:")
    print(f"   - {downloads_dir / 'train.parquet'}")
    print(f"   - {downloads_dir / 'test.parquet'}")

# ------------------------------
# 4️⃣ Run
# ------------------------------
if __name__ == "__main__":
    main()
