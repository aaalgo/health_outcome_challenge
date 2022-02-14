#!/usr/bin/env python3
#from pyspark.sql import SparkSession
import os
import sys
import pickle
import random
from glob import glob
import subprocess as sp
import cms

if __name__ == '__main__':
    sp.call('hdfs dfs -rm -r cms_merged_2012', shell=True)
    sp.call('rm -rf output', shell=True)
    sp.call('rm -rf /opt/hadoop/logs/userlogs/*', shell=True)
    #sp.check_call(f'./merge_helper.py cms_sample --output-dir output/ --meta  {meta_name}', shell=True)
    sp.check_call(f'./merge_helper_test_data.py -r hadoop hdfs:///user/wdong/cms_raw_2012 --output-dir cms_merged_2012/ --python-bin /opt/pypy3/bin/pypy3', shell=True)
    pass

