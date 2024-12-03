# README
## Subreddit Extraction

* Info about .zstd files from [https://files.pushshift.io/reddit/submissions/README.txt]()
* **UPDATE 17.08.2023**: pushshift not available anymore! Use torrents instead (search for "reddit torrents 2005 2023).


**WARNING: 24 threads are too much --> memory error --> fixed: readline() instead of readlines()**

**WARNING: 24 threads are too much --> HDD out of space --> use 12 threads --> set variable in extract_subreddit.py**


1. Docker with Ubuntu:

	`docker run --rm -it --name container_name -v /path/to/Reddit-Extractor_python3:/path/to/Reddit-Extractor_python3 -v /media/nas_datasets/datasets/Reddit:/media/nas_datasets/datasets/Reddit ubuntu:22.04`

2. Install zstd (v1.4.1)

	`apt update`
	
	`apt install -y zstd`

	`cd /path/to/Reddit-Extractor_python3/`


3. Install Python (3.8)

	`apt install -y python3 python3-pip`
	
	Install python packages with pip

4. Run scripts:

	Edit both scripts first: add subreddits to extract (variable "subreddits")

	`time python3 extract_subreddit.py`

	`time python3 prettify_output.py`
