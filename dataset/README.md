# Dataset
This module will sanitize raw data and partition them based on requirements for batch and incremental processing.

## Prerequisite
- Python 3.11

## Download Raw Data
```bash
curl -fsSL "https://snap.stanford.edu/data/cit-HepPh.txt.gz" | gunzip -d > cit-HepPh.txt
curl -fsSL "https://snap.stanford.edu/data/cit-HepPh-dates.txt.gz" | gunzip -d > cit-HepPh-dates.txt
```

## Partition Dataset
```bash
python3 partition.py
```

This script will create two types of partitioned dataset, one is for batch processing, such as Spark, another is for incremental processing, such as Timely.

For batch type, each file will include all citations till that year. For incremental type, each file will include new citations since last year.