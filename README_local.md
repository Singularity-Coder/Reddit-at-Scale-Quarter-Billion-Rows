# Quarter Billion Records EDA On MacBook Air (Work In-Progress)
Exploratory Data Analysis on 1/4 Billion records of Reddit data done on MacBook Air with 16GB RAM.

## External Storage
I am using a MacBook with m1 chip, 16GB RAM, 500GB internal storage, I obviously cannot load terra bytes of data and do EDA. So I used 10TB external storage given below:
* [GEONIX Refurbished 10 TB SATA Hard Drive for Desktop/Surveillance–8.89 cm(3.5 Inch), 6 Gb/s 7200 RPM High Speed Data Transfer, Heavy Duty Hard Disk with 256 MB Cache for Computer PC, 2 Years Warranty](https://www.amazon.in/dp/B0DQ5M168L?ref=ppx_yo2ov_dt_b_fed_asin_title)
* [SABRENT USB 3.0 to SATA External Hard Drive Lay-Flat Docking Station for 2.5 or 3.5in HDD, SSD [Support UASP] (EC-DFLT)](https://www.amazon.in/dp/B00LS5NFQ2?ref=ppx_yo2ov_dt_b_fed_asin_title)

## Goals
* Stream files (not load all into RAM)
* Auto-detect format (JSON array vs JSONL)
* Write merged output incrementally
* Minimize temp file and memory use
* Safe for external drive I/O

Best Approach: Line-by-Line Streaming Merge with Format Detection
Here’s a memory-safe script that:
* Opens each file.
* Peeks at the first non-whitespace character:
    * [ → JSON Array (use streaming with ijson)
    * { → JSONL or single object per line
* Merges all entries line by line to avoid RAM bloat.
* Uses tqdm for progress.
* Handles I/O safely on large external drives.

## Install Required Package
```python
# If you dont want to see install logs other than critical messages like errors use --quiet
# !pip install --quiet polars

# Try installing directly inside the current environment using %pip (Jupyter magic command):
# %pip install ijson tqdm

!pip install ijson tqdm
!pip install ijson
!pip install polars

# Restart Kernal after this
```

## Merge all 5 JSON files
Merge 2 files at a time. file1 & file2. Then the merged output of file1 & file2 with file3, etc. Merging all 5 at once did not work for me for some reason. The total file size of merged.json is 160.07 GB.
```python
import os
import json
import ijson
from tqdm import tqdm

input_dir = '/Volumes/TenTB/five_files_130GB_reddit_comments'
output_path = '/Volumes/TenTB/merged_output/merged.jsonl'

files = [f for f in os.listdir(input_dir) if f.endswith('.json') or f.endswith('.jsonl')]
print(f"Found {len(files)} files to process.\n")

count = 0

with open(output_path, 'w', encoding='utf-8') as outfile:
    for filename in tqdm(files, desc="Merging files"):
        file_path = os.path.join(input_dir, filename)

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                # Peek at first character
                first_char = ''
                while True:
                    c = f.read(1)
                    if not c:
                        break
                    if not c.isspace():
                        first_char = c
                        break
                f.seek(0)  # rewind

                if first_char == '[':
                    # Stream JSON array with ijson
                    for item in ijson.items(f, 'item'):
                        json.dump(item, outfile)
                        outfile.write('\n')
                        count += 1
                elif first_char == '{':
                    # Assume NDJSON / JSONL
                    for line in f:
                        outfile.write(line)
                        count += 1
                else:
                    print(f"[!] Skipped {filename}: Unknown format")

        except Exception as e:
            print(f"[!] Error reading {filename}: {e}")

print(f"\n✅ Merged {count} total JSON objects into:\n{output_path}")
```

## Convert to Valid JSON
The merged JSON is not in a valid JSON format. You will get ```IncompleteJSONError: parse error: trailing garbage```. This typically means:
- Your JSON file is not a valid single JSON object or array.
- You're trying to parse a file that contains multiple JSON objects without being in a list.
- Example of bad JSON:
```json
{"a": 1}
{"b": 2}
```
This is not valid if parsed as a single JSON object. We should correct it by wrapping it in an array and separate the objects by a comma. To convert it into a single object do as shown below. The total file size of correct.json is 160.33 GB.
```python
with open("/Volumes/TenTB/merged_output/merged.json", "r", encoding="utf-8") as infile, \
     open("/Volumes/TenTB/correct_json/correct.json", "w", encoding="utf-8") as outfile:

    outfile.write("[\n")
    first = True
    for line in infile:
        line = line.strip()
        if not line:
            continue
        if not first:
            outfile.write(",\n")
        outfile.write(line)
        first = False
    outfile.write("\n]")
    print(f"JSON Array file saved to: {output_csv_path}")
```

## JSON to CSV

Use ijson — a streaming JSON parser that reads the file incrementally, so it doesn’t blow up your memory. ```json.load(json_file)```  tries to load the entire 160 GB JSON file into RAM, which exceeds your 16 GB RAM and causes Jupyter (and possibly your entire system) to hang or crash. The total file size of 2tb_reddit_comments_2015.csv is 85.07 GB.

```python
import ijson
import csv
import os

# Paths
input_json_path = '/Volumes/TenTB/correct_json/correct.json'
output_dir = '/Volumes/TenTB/csv_output/'
output_csv_path = os.path.join(output_dir, 'reddit_comments_2015.csv')

# Ensure output directory exists
os.makedirs(output_dir, exist_ok=True)

# First pass: collect all keys from all rows
print("Scanning all keys...")
all_keys = set()
with open(input_json_path, 'rb') as json_file:
    items = ijson.items(json_file, 'item')
    for item in items:
        all_keys.update(item.keys())

fieldnames = sorted(all_keys)

# Second pass: write to CSV
print("Writing CSV...")
with open(input_json_path, 'rb') as json_file, open(output_csv_path, 'w', newline='', encoding='utf-8') as csv_file:
    items = ijson.items(json_file, 'item')
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames, extrasaction='ignore')
    writer.writeheader()
    for item in items:
        writer.writerow(item)

print(f"✅ CSV file saved to: {output_csv_path}")
```

## Get Total Records
* Do start file names with digits. Shell commands will cause problems.
* Do "jupyter notebook" by navigating to external storage directory in terminal
* Running ```wc -l /Volumes/TenTB/csv_output/reddit_comments_2015.csv``` in terminal will not give the correct number of records unless each record is exactly 1 line. This counts the number of lines which is inaccurate. It gave 529,610,375 records.
* This will give the correct number of records. 266,268,920 which is about a quarter billion records:

```python
import polars as pl # Super fast compared to csvstat

lf = pl.scan_csv('/Volumes/alienHD/csv_output/reddit_comments_2015.csv', infer_schema_length=1000)
row_count = lf.select(pl.len()).collect()[0, 0]
print(f"Total rows: {row_count}")
```