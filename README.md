# EDA-2TB-Reddit (Work In-Progress)
Exploratory Data Analysis on 2 Terra Bytes of Reddit data. 

## Dataset 5 files
* [Reddit Comments 2015 dataset](https://archive.org/download/2015_reddit_comments_corpus/reddit_data/2015/)

## Sample JSON reddit comments 2015
```json
{
  "score_hidden": false,
  "name": "t1_cnas8zv",
  "link_id": "t3_2qyrla",
  "body": "Most of us have some family members like this. *Most* of my family is like this.",
  "downs": 0,
  "created_utc": "1420070400",
  "score": 14,
  "author": "YoungModern",
  "distinguished": null,
  "id": "cnas8zv",
  "archived": false,
  "parent_id": "t3_2qyrla",
  "subreddit": "exmormon",
  "author_flair_css_class": null,
  "author_flair_text": null,
  "gilded": 0,
  "retrieved_on": 1425124282,
  "ups": 14,
  "controversiality": 0,
  "subreddit_id": "t5_2r0gj",
  "edited": false
}
```

## External Storage
I am using a MacBook with m1 chip, 16GB RAM, 500GB internal storage, I obviously cannot load terra bytes of data and do EDA. So I used 10TB external storage given below:
* [GEONIX Refurbished 10 TB SATA Hard Drive for Desktop/Surveillance–8.89 cm(3.5 Inch), 6 Gb/s 7200 RPM High Speed Data Transfer, Heavy Duty Hard Disk with 256 MB Cache for Computer PC, 2 Years Warranty](https://www.amazon.in/dp/B0DQ5M168L?ref=ppx_yo2ov_dt_b_fed_asin_title)
* [SABRENT USB 3.0 to SATA External Hard Drive Lay-Flat Docking Station for 2.5 or 3.5in HDD, SSD [Support UASP] (EC-DFLT)](https://www.amazon.in/dp/B00LS5NFQ2?ref=ppx_yo2ov_dt_b_fed_asin_title)


## Merge all 5 JSON files
Merge 2 files at a time. 1 & 2. Then the merged output of 1 & 2 with 3. Merging all 5 at once did not work for me for some reason.
```python
import os
import json
import ijson
from tqdm import tqdm

input_dir = '/Volumes/10TB/5_files_2TB_reddit_comments'
output_path = '/Volumes/10TB/merged_output/merged.jsonl'

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

## Clean JSON
The merged JSON, since its reddit data, is in the form {} {} ... object object and is not a proper JSON format. We should correct it by wrapping it in an array and separate the objects by a comma.
```python
with open("/Volumes/10TB/merged_output/merged.json", "r", encoding="utf-8") as infile, \
     open("/Volumes/10TB/correct_json/correct.json", "w", encoding="utf-8") as outfile:

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

We need ijson lib to not load all the data into memory at once. If you have only 16GB ram then 38GB wont fit.

```python
pip install ijson

import ijson
import csv
import os

# Paths
input_json_path = '/Volumes/10TB/correct_json/correct.json'
output_dir = '/Volumes/10TB/csv_output/'
output_csv_path = os.path.join(output_dir, '2tb_reddit_comments_2015.csv')

# Ensure output directory exists
os.makedirs(output_dir, exist_ok=True)

with open(input_json_path, 'rb') as json_file, open(output_csv_path, 'w', newline='', encoding='utf-8') as csv_file:
    # Stream each item in the top-level JSON array
    items = ijson.items(json_file, 'item')
    first_item = next(items)

    # Retain all columns
    all_keys = set()
    for item in items:
        all_keys.update(item.keys())
    fieldnames = sorted(all_keys)  # consistent column order

    # Initialize CSV writer
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerow(first_item)

    for item in items:
        writer.writerow(item)

print(f"CSV file saved to: {output_csv_path}")
```

