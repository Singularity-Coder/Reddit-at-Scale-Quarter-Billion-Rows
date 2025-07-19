# Quarter Billion Records EDA On Google Cloud (BigQuery)
Exploratory Data Analysis on 1/4 Billion records of Reddit data done using Google Cloud (BigQuery).

### Step 1: Open Google Cloud
* Open [Google Cloud](https://console.cloud.google.com/welcome).
* Upload the 2TB file to [Google Cloud Storage](https://console.cloud.google.com/storage) as max capacity for a BigQuery data set seems to be 100MB as of 20 Apr, 2025.
* Check [Billing](https://console.cloud.google.com/home/dashboard) details on Dashboard.
* Check [Pricing](https://cloud.google.com/storage/pricing).
* EDA will be done on [Bigquery](https://console.cloud.google.com/bigquery).


### Step 2: Create a Bucket
If this is disabled then you don't have billing enabled or your account is not verified.
![alt text](https://github.com/Singularity-Coder/Quarter-Billion-Records-EDA-On-MacBook-Air/blob/main/assets/ss1.png)


### Step 3: Install Google Cloud SDK on macOS
Uploading large files to Google Cloud Storage without overloading Laptop RAM:
* **Avoid Browser Upload for Huge Files**: If you're using the browser to upload large files, it can be inefficient. Chrome especially can cause high RAM usage when handling big files. Use `gsutil` or the `gcloud storage cp` command-line tools. Google Cloud upload CLI tools (like `gsutil` or the browser uploader) **do not load the full file into RAM** during upload. They **stream** the file in chunks — meaning only small portions are read into memory at a time.
* **`gsutil` Parallelism**: If you're using `gsutil cp` with parallelism (`-m` flag), it can use more memory by opening multiple threads. 
* **Other Apps**: Other apps (Chrome tabs, IDEs, background apps) may be taking memory while the upload is running, making it look like the upload is the cause.
* **macOS Caching**: macOS aggressively caches disk reads in memory. So when you upload, macOS may cache parts of the file in RAM, but this is temporary and not necessary for the upload itself.
* **Restart Before Upload**: Close unnecessary apps to free up memory before uploading.
* [Google Cloud SDK Install Guide](https://cloud.google.com/sdk/docs/install)

```bash
brew install --cask google-cloud-sdk
```

Google Cloud help command (Optional):
```bash
gcloud cheat-sheet
gcloud --help
```

### Step 4: Authenticate

Then initialize:

```bash
gcloud init
gcloud auth login
```

This will open a browser to log in to your Google account and set the project.

After authenticating from CLI you will see this page:
https://cloud.google.com/sdk/auth_success 

### Step 5: Upload file
This command ensures large files are **split into smaller chunks** and uploaded **efficiently**, **without overloading RAM**.

```bash
gsutil -o "GSUtil:parallel_composite_upload_threshold=150M" \
       -o "Boto:num_retries=20" \
       -o "Boto:max_retry_delay=300" \
       cp -r -D \
       /path/to/yourfile \
       gs://your-bucket-name/
```
* `-o "GSUtil:parallel_composite_upload_threshold=150M"`: Files **larger than 150MB** are split into smaller components for upload, making the process **faster and more memory-efficient**.
* `-o "Boto:num_retries"`: Total retries (default: 23). This will retry up to 23 times automatically before failing.
* `-o "Boto:max_retry_delay"`: Max backoff time (default: 32s). 300s = 5 minutes backoff.
* `-o "Boto:retry_delay_multiplier"`: Exponential backoff growth (default: 2.0). It means after every retry the delay gets twice as long if its 2.0. 1st retry 2 sec, 2nd try 4 sec, 3rd try 8 sec, etc. To reduce load on your computer and the server during failures (e.g., network blips, server throttling). Avoids hammering the server with constant retries, helps stabilize uploads.
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
       cp /path/to/yourfile \
       gs://your-bucket-name/
```

This forces **single-threaded uploads**, which will be slower but minimal on RAM.

**By default:**
* If upload fails (network disconnect or you manually cancel it ctrl+c), you **re-run the same command**, it **resumes automatically**. Enabled for parallel composite uploads (files split into parts). After Interruption, just rerun, resumes chunks (composite). Session Lifetime is ~7 days or until components expire. Partial chunks resume for massive files.
* **If you need speed, retries, parallel tuning, or are uploading really huge files (multi-hundred GB)** → **`gsutil cp`** gives you **finer control**.
* **For programmatic scripting (bash scripts, cron jobs)** → Use `gsutil`.
* **Resumable Upload**: Only via parallel composite (partial chunks retry)
* **Chunk Size Control**: Threshold splits into parallel chunks.
* **RAM Efficiency**: Medium (parallel threads = higher RAM).
* **Parallel Upload**: Yes via `parallel_composite_upload_threshold`. It controls when gsutil splits files into multiple chunks for parallel upload.
* **Default = 150MB** → if your file is **larger than 150MB**, it gets split into multiple chunks (composite upload). You CAN increase this value to say 500MB.
    * Files under 500MB will upload normally (single stream).
    * Files over 500MB will be split into **multiple parts** and uploaded in parallel.