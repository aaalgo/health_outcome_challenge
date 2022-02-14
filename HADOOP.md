1. merge_stage2.py
2. filter_pids.py	generate test set

./filter_pids.py -r hadoop hdfs:///user/wdong/cms_stage2 --output-dir cms_test/ --python-bin /opt/pypy3/bin/pypy3 --cmdenv CMS_HOME=/home/wdong/shared/cms2

3. sample_mortality.py

- sample train

./sample_mortality.py -r hadoop hdfs:///user/wdong/cms_stage2 --output-dir mortality_train/ --python-bin /opt/pypy3/bin/pypy3 --cmdenv CMS_HOME=/home/wdong/shared/cms2 --black meta/split4

./sample_mortality.py -r hadoop hdfs:///user/wdong/cms_stage2 --output-dir mortality_train_25/ --python-bin /opt/pypy3/bin/pypy3 --cmdenv CMS_HOME=/home/wdong/shared/cms2 --black meta/split4 --bg_ratio 25

- sample test

./sample_mortality.py -r hadoop hdfs:///user/wdong/cms_test --output-dir mortality_test_pre/ --python-bin /opt/pypy3/bin/pypy3 --cmdenv CMS_HOME=/home/wdong/shared/cms2

hadoop fs -get mortality_test_pre mortality/
./gen_mortality_test_labels.py


./filter_mortality_cutoff.py -r hadoop hdfs:///user/wdong/mortality_test_pre --output-dir mortality_test/ --python-bin /opt/pypy3/bin/pypy3 --cmdenv CMS_HOME=/home/wdong/shared/cms2 --gs meta/mortality/test_gs

-- remove claims past cutoff
-- remove death flag & date in demos


# file list

cms_raw_stage2	-- raw data
cms_stage2		-- merged data
cms_test		-- 1/5 data for test	(pids in meta/split4)
mortality_train	
mortality_test
