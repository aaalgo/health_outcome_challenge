# AAA CMS Code

# Preprocessing

1. Use `patch_2009.py` to patch the 2009 data.
2. `./generate_meta.py` scans data file and collect meta data, e.g.
   columns of each file.
3. `./generate_mapping.py` builds maps pre-2010 data fields to new
   fields.
4. `./merge_helper_2009.py` sort and merge data by patient ID.
5. `./filter_*.py` filter merged data by various criterion.


# Services

## Commands


```
docker-compose up
/opt/hadoop/sbin/start-all.sh
/opt/spark/sbin/start-all.sh
celery -A inf_server worker --loglevel=info
celery flower -A inf_server --address=0.0.0.0 --port=5555 --url_prefix
flower
```

## Ports

- Redis: 6379.
  * DB 0 is for web server
  * DB 1 is for Celery broker
  * DB 2 is for Celery results


# Pre-Test Data

- merge_test_good.py
- merge_test_good_minus_1_year.py

# Test Data

Test data:

- Raw data encrypted: /shared/data/CMS/AI_Challenge_2012_SAF_5
- Raw data: /shared/data/CMS/AI_Challenge_2012_SAF_5/data/dec/
- Raw data: hadoop:/user/wdong/cms_raw_2012 
- Merged: hadoop:cms_merged_2012
- Merged minus 1 year: cms_merged_2012_minus_1_year

Script:

- Merge: merge_helper_test_data.py  merge_test_data.py
- Merge minus 1 year: merge_helper_test_data_minus_1_year.py merge_test_good_minus_1_year.py

