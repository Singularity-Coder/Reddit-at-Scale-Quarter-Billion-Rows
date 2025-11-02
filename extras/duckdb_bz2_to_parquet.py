"""
duckdb_dir_to_parquet.py

Convert all top-level CSV or JSONL files in a directory (any of: .gz, .zst, .bz2, or
uncompressed) into ONE Parquet file using DuckDB.

- Install: pip install duckdb zstandard
- Input: directory path (no recursion)
- Supports formats: csv, jsonl (or auto-detect)
- For .bz2 (including plain *.bz2 without .csv/.jsonl in the name),
  transcodes to .zst in a temp dir, then DuckDB reads .zst in parallel.
- Output Parquet uses ZSTD with configurable COMPRESSION_LEVEL (default 22)

Examples
--------
Auto-detect (CSV or JSONL), use max zstd:

    python duckdb_bz2_to_parquet.py \
        -i "/data/top" \
        -o "/data/out/all.parquet"

Explicit JSONL with 8 threads and bigger row groups:

    python duckdb_bz2_to_parquet.py \
        -i "/Volumes/alienHD/5_files_2TB_reddit_comments" \
        -o "/Volumes/alienHD/test2/out.parquet" \
        --format jsonl \
        --threads 8 \
        --row-group-size 1000000 \
        --parquet-compression zstd \
        --compression-level 22

"""


from __future__ import annotations
import argparse
import sys
from pathlib import Path
import tempfile
import shutil
import bz2
import duckdb
import zstandard as zstd

# ---------- args ----------

def parse_args():
    p = argparse.ArgumentParser(
        description="Convert top-level CSV/JSONL files (gz/zst/bz2/uncompressed) into one Parquet via DuckDB."
    )
    p.add_argument("-i", "--input-dir", required=True, metavar="DIR",
                   help="Directory containing files (top level only; no recursion).")
    p.add_argument("-o", "--output-parquet", required=True, metavar="FILE",
                   help="Output Parquet file.")
    p.add_argument("--format", choices=["auto", "csv", "jsonl"], default="auto",
                   help="Input format (default: auto).")
    p.add_argument("--threads", type=int, default=0,
                   help="DuckDB threads (0=auto).")
    p.add_argument("--row-group-size", type=int, default=1_000_000,
                   help="Parquet ROW_GROUP_SIZE (rows).")
    p.add_argument("--parquet-compression", choices=["zstd", "snappy", "gzip"], default="zstd",
                   help="Parquet compression codec (default: zstd).")
    p.add_argument("--compression-level", type=int, default=22,
                   help="Parquet compression level (default: 22 for zstd).")
    p.add_argument("--keep-temp", action="store_true",
                   help="Keep temporary .zst files created from .bz2.")
    return p.parse_args()

# ---------- helpers ----------

def list_top_level(dirpath: Path):
    return sorted([p for p in dirpath.iterdir() if p.is_file()])

def sniff_bz2_is_jsonl(path: Path) -> bool:
    try:
        with bz2.open(path, "rt", encoding="utf-8", errors="ignore") as fh:
            for line in fh:
                s = line.strip()
                if not s:
                    continue
                return s.startswith("{") or s.startswith("[")
    except Exception:
        pass
    # default to CSV if unsure
    return False

def detect_format(files) -> str:
    # Extensions first
    for f in files:
        n = f.name.lower()
        if n.endswith((".jsonl", ".ndjson", ".jsonl.gz", ".ndjson.gz", ".jsonl.zst", ".ndjson.zst")):
            return "jsonl"
    for f in files:
        n = f.name.lower()
        if n.endswith((".csv", ".csv.gz", ".csv.zst")):
            return "csv"
    # If bz2 exists, sniff one
    bz2s = [f for f in files if f.name.lower().endswith(".bz2")]
    if bz2s:
        return "jsonl" if sniff_bz2_is_jsonl(bz2s[0]) else "csv"
    return "csv"

def transcode_bz2_to_zst(src: Path, dst: Path, level: int = 9, threads: int = 0):
    """
    Stream-transcode bz2 -> zst. Use a moderate level for intermediates (fast to create/read).
    IMPORTANT FIX: construct ZstdCompressor with keywords, not a positional params object.
    """
    # threads=0 lets zstd decide; many-core systems will parallelize
    cctx = zstd.ZstdCompressor(level=level, threads=max(0, threads))
    with bz2.open(src, "rb") as fin, open(dst, "wb") as fout, cctx.stream_writer(fout) as zf:
        shutil.copyfileobj(fin, zf)

def build_inputs(dirpath: Path, fmt: str):
    """Return (direct_globs, bz2_files_to_transcode)."""
    files = list_top_level(dirpath)
    direct_globs = []
    bz2_files = []

    if fmt == "csv":
        if any(f.name.lower().endswith(".csv") for f in files):
            direct_globs.append(str(dirpath / "*.csv"))
        if any(f.name.lower().endswith(".csv.gz") for f in files):
            direct_globs.append(str(dirpath / "*.csv.gz"))
        if any(f.name.lower().endswith(".csv.zst") for f in files):
            direct_globs.append(str(dirpath / "*.csv.zst"))
        # treat both .csv.bz2 and plain .bz2 as CSV when fmt=csv
        bz2_files = [f for f in files if f.name.lower().endswith((".csv.bz2", ".bz2"))]
    else:  # jsonl
        if any(f.name.lower().endswith(".jsonl") for f in files):
            direct_globs.append(str(dirpath / "*.jsonl"))
        if any(f.name.lower().endswith(".ndjson") for f in files):
            direct_globs.append(str(dirpath / "*.ndjson"))
        if any(f.name.lower().endswith(".jsonl.gz") for f in files):
            direct_globs.append(str(dirpath / "*.jsonl.gz"))
        if any(f.name.lower().endswith(".ndjson.gz") for f in files):
            direct_globs.append(str(dirpath / "*.ndjson.gz"))
        if any(f.name.lower().endswith(".jsonl.zst") for f in files):
            direct_globs.append(str(dirpath / "*.jsonl.zst"))
        if any(f.name.lower().endswith(".ndjson.zst") for f in files):
            direct_globs.append(str(dirpath / "*.ndjson.zst"))
        # treat both .jsonl.bz2/.ndjson.bz2 and plain .bz2 as JSONL when fmt=jsonl
        bz2_files = [f for f in files if f.name.lower().endswith((".jsonl.bz2", ".ndjson.bz2", ".bz2"))]

    return direct_globs, bz2_files

def make_union_query(fmt: str, globs: list[str]) -> str:
    scans = []
    if fmt == "csv":
        for g in globs:
            scans.append(f"SELECT * FROM read_csv_auto('{g}', HEADER=TRUE)")
    else:
        for g in globs:
            scans.append(f"SELECT * FROM read_json_auto('{g}')")
    return "\nUNION ALL\n".join(scans) if scans else "SELECT * FROM (SELECT 1) WHERE 1=0"

# ---------- main ----------

def main():
    args = parse_args()
    in_dir = Path(args.input_dir)
    if not in_dir.exists() or not in_dir.is_dir():
        print(f"Input is not a directory: {in_dir}", file=sys.stderr)
        sys.exit(1)

    files = list_top_level(in_dir)
    if not files:
        print(f"No files found in: {in_dir}", file=sys.stderr)
        sys.exit(1)

    fmt = args.format if args.format != "auto" else detect_format(files)
    if fmt not in ("csv", "jsonl"):
        print("Could not determine format (csv/jsonl). Use --format explicitly.", file=sys.stderr)
        sys.exit(1)

    direct_globs, bz2_files = build_inputs(in_dir, fmt)

    tempdir = None
    if bz2_files:
        tempdir = Path(tempfile.mkdtemp(prefix="duckdb_bz2_to_zst_"))
        print(f"Transcoding {len(bz2_files)} .bz2 files to .zst in: {tempdir}")
        # Moderate intermediate .zst level for speed; Parquet level is controlled separately.
        inter_level = 9
        inter_threads = max(args.threads, 0)
        for src in bz2_files:
            base = src.name[:-4]  # drop .bz2
            # keep any existing suffix; extension doesn't affect DuckDB parsing
            dst = tempdir / f"{base}.zst"
            transcode_bz2_to_zst(src, dst, level=inter_level, threads=inter_threads)
        direct_globs.append(str(tempdir / "*.zst"))

    if not direct_globs:
        print("No readable inputs found (expecting .csv/.jsonl [optionally .gz/.zst] or .bz2).", file=sys.stderr)
        if tempdir and not args.keep_temp:
            shutil.rmtree(tempdir, ignore_errors=True)
        sys.exit(1)

    con = duckdb.connect()
    con.execute(f"PRAGMA threads={args.threads};") if args.threads > 0 else con.execute("PRAGMA threads=0;")
    if fmt == "jsonl":
        con.execute("INSTALL json; LOAD json;")

    union_sql = make_union_query(fmt, direct_globs)

    out_path = Path(args.output_parquet)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_sql_path = str(out_path).replace("'", "''")

    copy_sql = f"""
    COPY (
      {union_sql}
    )
    TO '{out_sql_path}'
    (FORMAT PARQUET,
     COMPRESSION {args.parquet_compression},
     COMPRESSION_LEVEL {args.compression_level},
     ROW_GROUP_SIZE {args.row_group_size});
    """

    print("Running DuckDB COPY with:")
    print(f"  format={fmt}, codec={args.parquet_compression}, level={args.compression_level}, row_group_size={args.row_group_size}")
    for g in direct_globs:
        print(f"  scan: {g}")

    con.execute(copy_sql)

    if tempdir and not args.keep_temp:
        shutil.rmtree(tempdir, ignore_errors=True)

    print(f"Done → {out_path}")

if __name__ == "__main__":
    main()
