# Quarter Billion Records EDA On Google Cloud (BigQuery)
Exploratory Data Analysis on 1/4 Billion records of Reddit data done using Google Cloud (BigQuery).


## Uploading large files to Google Cloud Storage without overloading Laptop RAM
* **Avoid Browser Upload for Huge Files**: If you're using the browser to upload large files, it can be inefficient. Chrome especially can cause high RAM usage when handling big files. Use `gsutil` or the `gcloud storage cp` command-line tools. Google Cloud upload CLI tools (like `gsutil` or the browser uploader) **do not load the full file into RAM** during upload. They **stream** the file in chunks — meaning only small portions are read into memory at a time.
* **`gsutil` Parallelism**: If you're using `gsutil cp` with parallelism (`-m` flag), it can use more memory by opening multiple threads. 
* **Other Apps**: Other apps (Chrome tabs, IDEs, background apps) may be taking memory while the upload is running, making it look like the upload is the cause.
* **macOS Caching**: macOS aggressively caches disk reads in memory. So when you upload, macOS may cache parts of the file in RAM, but this is temporary and not necessary for the upload itself.
* **Restart Before Upload**: Close unnecessary apps to free up memory before uploading.


### Step 1: Install Google Cloud SDK on macOS
* [Google Cloud SDK Install Guide](https://cloud.google.com/sdk/docs/install)

```bash
brew install --cask google-cloud-sdk
```

Then initialize:

```bash
gcloud init
gcloud auth login
```

This will open a browser to log in to your Google account and set the project.

### Step 2: Upload File with Optimized Settings

```bash
gcloud storage cp /path/to/yourfile gs://your-bucket-name/
```

**By default:**

* Files over **8 MB** are **resumable**.
* If upload fails (network disconnect), you **re-run the same command**, it **resumes automatically**.

**Optional Control:**

* **Chunk Size** (RAM control):

```bash
gcloud storage cp --chunk-size=32M /path/to/yourfile gs://your-bucket-name/
```

* **Parallelism**:

  * `gcloud storage cp` **does not use parallelism** like `gsutil`.
  * **It favors stability over speed**.

**Summary:**

| Feature            | `gcloud storage cp`        |
| ------------------ | -------------------------- |
| Resumable Upload   | ✅ **Always on by default** |
| Parallel Upload    | ❌ No                       |
| RAM Efficient      | ✅ Yes                      |
| Speed              | ⚠️ Medium                  |
| Chunk Size Control | ✅ Yes via `--chunk-size`   |

If you want **parallel fast upload** → use `gsutil`.
If you want **RAM-efficient, fail-safe upload** → use `gcloud storage cp` (no `--resumable` needed).

___

This command ensures large files are **split into smaller chunks** and uploaded **efficiently**, **without overloading RAM**.

```bash
gsutil -o "GSUtil:parallel_composite_upload_threshold=150M" cp -r -D /path/to/yourfile gs://your-bucket-name/
```
* `-o "GSUtil:parallel_composite_upload_threshold=150M"`: Files **larger than 150MB** are split into smaller components for upload, making the process **faster and more memory-efficient**.
* `cp`: Copy the file.
* `-D` shows upload progress
* `/path/to/yourfile`: Full path to your file (you can drag-drop in terminal).
* `gs://your-bucket-name/`: Your target Google Cloud Storage bucket.

(Optional) Limit Parallelism to Save RAM

If RAM is still high, you can **reduce concurrency**:

```bash
gsutil -o "GSUtil:parallel_composite_upload_threshold=150M" \
       -o "GSUtil:parallel_process_count=1" \
       -o "GSUtil:parallel_thread_count=1" \
       cp /path/to/yourfile gs://your-bucket-name/
```

This forces **single-threaded uploads**, which will be slower but minimal on RAM.