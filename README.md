![alt text](https://github.com/Singularity-Coder/Quarter-Billion-EDA/blob/main/assets/banner.png)

# Quarter Billion Records EDA On MacBook Air
Exploratory Data Analysis on 1/4 Billion records of Reddit data done on MacBook Air with 16GB RAM.


## Dataset 5 files
* Check [Reddit Comments 2015 dataset](https://archive.org/download/2015_reddit_comments_corpus/reddit_data/2015/)
* The data is in the JSONL or single object per line format. 
* Total records: 266,268,920 which is about 266 million or ~1/4 billion records.


## Sample JSONL reddit comments 2015
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

## Prepare Data
* To perform EDA, we need to convert the 5 bz2 Gzip files into a single parquet file. Use `Reddit-at-Scale-Quarter-Billion-Rows/extras/bz2_to_parquet.py` [file](https://github.com/Singularity-Coder/Reddit-at-Scale-Quarter-Billion-Rows/blob/main/extras/bz2_to_parquet.py) to get the file. Put all the bz2 files in a single directory and run this script. Then perform EDA as shown below.


## Exploratory Data Analysis
Check EDA here - [reddit_eda.ipynb]()