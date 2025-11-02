"""
bz2_to_parquet.py

Convert all top-level *.bz2 (CSV or JSONL) in a directory into ONE Parquet file.

- Install: pip install pandas pyarrow
- No recursion
- Large, streaming chunks
- ZSTD compression w/ configurable level (e.g., 22)
- Dictionary encoding for strings
- Optional type coercion to tighten schema

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
from pathlib import Path
import sys
import bz2
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq



# ---------- CLI ----------

def parse_args():
    p = argparse.ArgumentParser(
        description="Convert top-level .bz2 files (CSV/JSONL) in a directory into one Parquet (PyArrow)."
    )
    p.add_argument("-i", "--input-dir", required=True, metavar="DIR",
                   help="Directory with top-level .bz2 files (no recursion).")
    p.add_argument("-o", "--output-parquet", required=True, metavar="FILE",
                   help="Output Parquet file.")
    p.add_argument("--format", choices=["auto", "csv", "jsonl"], default="auto",
                   help="Content format inside .bz2 (default: auto).")
    p.add_argument("--chunksize", type=int, default=1_000_000,
                   help="Rows per chunk to stream (default: 1,000,000).")

    # CSV knobs (only used when --format csv or auto-detected csv)
    p.add_argument("--csv-sep", default=",", help='CSV delimiter (default: ",")')
    p.add_argument("--has-header", action="store_true",
                   help="Set if CSVs have a header row (default: False)")
    p.add_argument("--csv-encoding", default="utf-8", help="CSV encoding (default: utf-8)")

    # Parquet / perf knobs
    p.add_argument("--parquet-compression", choices=["zstd", "snappy", "gzip", "brotli", "none"],
                   default="zstd", help="Parquet compression codec (default: zstd)")
    p.add_argument("--compression-level", type=int, default=22,
                   help="Compression level for zstd/gzip/brotli (e.g., 22 for zstd).")
    p.add_argument("--row-group-size", type=int, default=1_000_000,
                   help="Target Parquet row group size (default: 1,000,000).")
    p.add_argument("--threads", type=int, default=0,
                   help="Hint for PyArrow CPU threads (0 = default).")
    p.add_argument("--no-dict", action="store_true", help="Disable dictionary encoding for strings.")
    p.add_argument("--no-coerce", action="store_true",
                   help="Disable type coercion heuristics (leave strings as-is).")
    return p.parse_args()


# ---------- Helpers ----------

def list_bz2_top_level(d: Path):
    return sorted([p for p in d.glob("*.bz2") if p.is_file()])

def detect_format(files) -> str:
    # Filename hints first
    for f in files:
        n = f.name.lower()
        if n.endswith((".jsonl.bz2", ".ndjson.bz2")):
            return "jsonl"
    for f in files:
        n = f.name.lower()
        if n.endswith(".csv.bz2"):
            return "csv"
    # Sniff first non-empty line of a bz2 file
    try:
        with bz2.open(files[0], "rt", encoding="utf-8", errors="ignore") as fh:
            for line in fh:
                s = line.strip()
                if not s:
                    continue
                return "jsonl" if s[:1] in ("{", "[") else "csv"
    except Exception:
        pass
    return "csv"  # safe fallback

def iter_chunks_csv(path: Path, chunksize: int, sep: str, header: bool, encoding: str):
    header_arg = 0 if header else None
    try:
        rdr = pd.read_csv(
            path,
            compression="infer",  # bz2
            sep=sep,
            header=header_arg,
            encoding=encoding,
            chunksize=chunksize,
            low_memory=False,
        )
        for chunk in rdr:
            yield chunk
    except pd.errors.EmptyDataError:
        return

def iter_chunks_jsonl(path: Path, chunksize: int):
    try:
        rdr = pd.read_json(
            path,
            compression="infer",
            lines=True,
            chunksize=chunksize,
        )
        for chunk in rdr:
            yield chunk
    except ValueError:
        return

def coerce_chunk(df: pd.DataFrame) -> pd.DataFrame:
    """
    Tighten types safely:
      - integer strings -> Int64[pyarrow]; if overflow and non-negative -> UInt64[pyarrow]; else keep as string
      - decimal/scientific numbers -> Float64[pyarrow]
      - 'true'/'false' -> boolean[pyarrow]
    Falls back to string when overflow or mixed signs would lose precision.
    """
    df = df.convert_dtypes(dtype_backend="pyarrow")

    for col in df.columns:
        s = df[col]
        if not (pd.api.types.is_string_dtype(s) or pd.api.types.is_object_dtype(s)):
            continue

        s_str = s.astype("string[pyarrow]")

        # 1) Integer-like? (no dots/exponent)
        int_mask = s_str.str.match(r'^[+-]?\d+$')
        frac_int = float(int_mask.mean()) if len(s_str) else 0.0
        if frac_int > 0.98:
            try:
                # First try signed 64-bit
                df[col] = s_str.astype("Int64[pyarrow]")
            except Exception:
                # If overflowed, try unsigned only if no negatives are present
                try:
                    has_negative = bool(s_str[int_mask].str.contains('-', regex=False).any())
                except Exception:
                    has_negative = True
                if not has_negative:
                    try:
                        df[col] = s_str.astype("UInt64[pyarrow]")
                    except Exception:
                        df[col] = s_str  # keep exact as string
                else:
                    df[col] = s_str
            continue

        # 2) Float-like (decimals, exponents, or mixed)
        num = pd.to_numeric(s_str, errors="coerce")
        if num.notna().mean() > 0.98:
            df[col] = num.astype("Float64[pyarrow]")
            continue

        # 3) Boolean-like
        low = s_str.str.lower()
        if low.isin(["true", "false"]).mean() > 0.98:
            df[col] = low.map({"true": True, "false": False}).astype("boolean[pyarrow]")

    return df


def ensure_columns(df: pd.DataFrame, schema: pa.Schema) -> pd.DataFrame:
    for name in schema.names:
        if name not in df.columns:
            df[name] = pd.NA
    return df[schema.names]


# ---------- Main ----------

def main():
    args = parse_args()

    # Optional thread hint (ignored by older PyArrow)
    try:
        if args.threads and args.threads > 0:
            pa.set_cpu_count(args.threads)
    except Exception:
        pass

    in_dir = Path(args.input_dir)
    if not in_dir.exists() or not in_dir.is_dir():
        print(f"Input is not a directory: {in_dir}", file=sys.stderr)
        sys.exit(1)

    files = list_bz2_top_level(in_dir)
    if not files:
        print(f"No .bz2 files found in: {in_dir}", file=sys.stderr)
        sys.exit(1)

    fmt = args.format if args.format != "auto" else detect_format(files)
    if fmt not in ("csv", "jsonl"):
        print("Could not determine format (csv/jsonl). Use --format explicitly.", file=sys.stderr)
        sys.exit(1)

    out_path = Path(args.output_parquet)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    codec = None if args.parquet_compression == "none" else args.parquet_compression
    use_dict = not args.no_dict
    do_coerce = not args.no_coerce
    rg_size = int(args.row_group_size) if args.row_group_size and args.row_group_size > 0 else None

    writer = None
    schema = None
    total_rows = 0
    files_done = 0

    try:
        for f in files:
            if fmt == "csv":
                chunks = iter_chunks_csv(
                    f, chunksize=args.chunksize, sep=args.csv_sep,
                    header=args.has_header, encoding=args.csv_encoding
                )
            else:
                chunks = iter_chunks_jsonl(f, chunksize=args.chunksize)

            file_rows = 0
            for chunk in chunks:
                if chunk is None or chunk.empty:
                    continue
                if do_coerce:
                    chunk = coerce_chunk(chunk)

                if schema is None:
                    table = pa.Table.from_pandas(chunk, preserve_index=False)
                    schema = table.schema
                    # Create writer (compression level may not be supported on old PyArrow)
                    try:
                        writer = pq.ParquetWriter(
                            str(out_path),
                            schema=schema,
                            compression=codec,
                            compression_level=args.compression_level,
                            use_dictionary=use_dict,
                            write_statistics=True,
                        )
                    except TypeError:
                        writer = pq.ParquetWriter(
                            str(out_path),
                            schema=schema,
                            compression=codec,
                            use_dictionary=use_dict,
                            write_statistics=True,
                        )
                    # write first chunk
                    try:
                        writer.write_table(table, row_group_size=rg_size)
                    except TypeError:
                        # older pyarrow: split manually if needed
                        if rg_size and rg_size > 0 and table.num_rows > rg_size:
                            for start in range(0, table.num_rows, rg_size):
                                writer.write_table(table.slice(start, rg_size))
                        else:
                            writer.write_table(table)
                else:
                    chunk = ensure_columns(chunk, schema)
                    table = pa.Table.from_pandas(chunk, schema=schema, preserve_index=False)
                    try:
                        writer.write_table(table, row_group_size=rg_size)
                    except TypeError:
                        if rg_size and rg_size > 0 and table.num_rows > rg_size:
                            for start in range(0, table.num_rows, rg_size):
                                writer.write_table(table.slice(start, rg_size))
                        else:
                            writer.write_table(table)

                r = len(chunk)
                file_rows += r
                total_rows += r

            if file_rows > 0:
                files_done += 1
                print(f"✔ Wrote {file_rows:,} rows from {f.name}")
            else:
                print(f"– Skipped empty or unreadable: {f.name}")

        if writer is None:
            print("All files were empty; nothing to write.", file=sys.stderr)
            sys.exit(2)

    finally:
        if writer is not None:
            writer.close()

    print(f"\nDone. Files processed: {files_done}/{len(files)} | "
          f"Total rows: {total_rows:,} | Output: {out_path}")


if __name__ == "__main__":
    main()
