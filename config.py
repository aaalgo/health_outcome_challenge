#!/usr/bin/env python3

S3_SERVER = 'barton:9000'
REDUCE_TASKS = 500
S3_ACCESS_KEY = 'minioadmin'
S3_SECRET_KEY = 'minioadmin'
S3_BUCKET = 'local'
USE_FAKE_DATA = False

CASE_SERVER_PORT = 9100
WEB_SERVER = 'http://barton:16666'

#CELERY_BROKER = 'redis://localhost:6379/1'
#CELERY_BACKEND = 'redis://localhost:6379/2'

HADOOP_FILES = ['setup_mrjob.py'] #'cms.py', 'meta2.pkl', 'config.py', 'mapping2.pkl', 'olap.py', 'cms_spark.py', 'target.py']

SERVED_DATA = ['/data2/CMS/cms_stage2/*']

try:
    from local_config import *
    HADOOP_FILES.append('local_config.py')
except:
    pass


def s3_key (v):
    v = str(int(v))
    l1 = v[-3:]
    l2 = v[-6:-3]
    return '%s/%s/%s' % (l1, l2, v)

if __name__ == '__main__':
    print(s3_key(498266385))
    pass
