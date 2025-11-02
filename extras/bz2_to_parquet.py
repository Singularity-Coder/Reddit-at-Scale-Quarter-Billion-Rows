"""
bz2_to_parquet.py
Convert multiple .bz2 files (CSV or JSON Lines) into a single Parquet file.

Examples
--------
CSV (comma, header row):
    python bz2_to_parquet.py \
        --i "/data/logs/*.bz2" \
        --o "out/all.parquet" \
        --format csv --chunksize 200000 --csv-sep "," --has-header

JSON Lines (.jsonl) compressed with bz2:
    python bz2_to_parquet.py \
        --i "/data/events/*.bz2" \
        --o "out/all.parquet" \
        --format jsonl --chunksize 100000
"""

import argparse
import sys
from pathlib import Path
import glob
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def parse_args():
    import argparse
    p = argparse.ArgumentParser(
        description="Convert many .bz2 files (CSV or JSONL) into one Parquet file (streaming)."
    )

    # support both --in_dest and --in-dest (and a short -i)
    p.add_argument("--i",
                   dest="input_glob", required=True, metavar="GLOB",
                   help='Glob for input files, e.g. "/path/to/*.bz2"')

    # support both --out_dest and --out-dest (and a short -o)
    p.add_argument("--o",
                   dest="output_parquet", required=True, metavar="FILE",
                   help="Output Parquet file path")

    p.add_argument("--format",
                   choices=["csv", "jsonl"], default="csv",
                   help="Input content format inside the .bz2 file (default: csv)")

    p.add_argument("--chunksize", type=int, default=200_000,
                   help="Rows per chunk to stream (default: 200k)")

    # CSV-specific
    p.add_argument("--csv-sep", default=",",
                   help='CSV delimiter (default: ",")')
    p.add_argument("--has-header", action="store_true",
                   help="Set if CSVs have a header row (default: False = no header)")
    p.add_argument("--csv-encoding", default="utf-8",
                   help="CSV encoding (default: utf-8)")

    # Parquet
    p.add_argument("--parquet-compression",
                   choices=["snappy", "zstd", "gzip", "brotli", "none"],
                   default="snappy",
                   help="Parquet compression codec (default: snappy)")

    return p.parse_args()


def list_files(pattern: str):
    files = sorted(glob.glob(pattern))
    return [Path(f) for f in files if Path(f).is_file()]


def iter_chunks_csv(path: Path, chunksize: int, sep: str, header: bool, encoding: str):
    header_arg = 0 if header else None
    try:
        reader = pd.read_csv(
            path,
            compression="infer",   # .bz2
            sep=sep,
            header=header_arg,
            encoding=encoding,
            chunksize=chunksize,
            low_memory=False,
            # engine left as default; pandas chooses python/c/pyarrow depending on env
        )
        for chunk in reader:
            yield chunk
    except pd.errors.EmptyDataError:
        return


def iter_chunks_jsonl(path: Path, chunksize: int):
    # JSON Lines (one JSON object per line)
    try:
        reader = pd.read_json(
            path,
            compression="infer",   # .bz2
            lines=True,
            chunksize=chunksize,
        )
        for chunk in reader:
            yield chunk
    except ValueError:
        # Raised if file is empty or malformed
        return


def ensure_columns(df: pd.DataFrame, schema: pa.Schema) -> pd.DataFrame:
    # Add any missing columns, then reorder to schema order
    missing = [name for name in schema.names if name not in df.columns]
    for col in missing:
        df[col] = pd.NA
    # Extra columns (unexpected) are dropped to keep schema fixed
    df = df[schema.names]
    return df


def main():
    args = parse_args()

    files = list_files(args.input_glob)
    if not files:
        print(f"No files matched pattern: {args.input_glob}", file=sys.stderr)
        sys.exit(1)

    out_path = Path(args.output_parquet)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    codec = None if args.parquet_compression == "none" else args.parquet_compression

    writer = None
    schema = None
    total_rows = 0
    files_processed = 0

    try:
        for f in files:
            # Pick the right chunk iterator
            if args.format == "csv":
                chunk_iter = iter_chunks_csv(
                    f,
                    chunksize=args.chunksize,
                    sep=args.csv_sep,
                    header=args.has_header,
                    encoding=args.csv_encoding,
                )
            else:
                chunk_iter = iter_chunks_jsonl(f, chunksize=args.chunksize)

            file_rows = 0
            for chunk in chunk_iter:
                if chunk is None or chunk.empty:
                    continue

                if schema is None:
                    # First non-empty chunk defines the schema
                    table = pa.Table.from_pandas(chunk, preserve_index=False)
                    schema = table.schema
                    writer = pq.ParquetWriter(
                        str(out_path), schema=schema, compression=codec
                    )
                    writer.write_table(table)
                else:
                    # Conform to the fixed schema
                    chunk = ensure_columns(chunk, schema)
                    table = pa.Table.from_pandas(
                        chunk, schema=schema, preserve_index=False
                    )
                    writer.write_table(table)

                r = len(chunk)
                file_rows += r
                total_rows += r

            if file_rows > 0:
                files_processed += 1
                print(f"✔ Wrote {file_rows:,} rows from {f.name}")
            else:
                print(f"– Skipped empty or unreadable: {f.name}")

        if writer is None:
            print("All files were empty; nothing to write.", file=sys.stderr)
            sys.exit(2)

    finally:
        if writer is not None:
            writer.close()

    print(
        f"\nDone. Files processed: {files_processed}/{len(files)} | "
        f"Total rows: {total_rows:,} | Output: {out_path}"
    )


if __name__ == "__main__":
    main()
