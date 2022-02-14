#!/usr/bin/env python3
import sys
import os
import traceback
import io
import mrjob
from mrjob.job import MRJob
from minio import Minio
import setup_mrjob
import config

class CopyS3Job (MRJob):

    INPUT_PROTOCOL = mrjob.protocol.BytesProtocol
    INTERNAL_PROTOCOL = mrjob.protocol.BytesProtocol
    OUTPUT_PROTOCOL = mrjob.protocol.BytesProtocol

    FILES = config.HADOOP_FILES

    def mapper_init (self):
        self.s3 = Minio(config.S3_SERVER,
                  access_key=config.S3_ACCESS_KEY,
                  secret_key=config.S3_SECRET_KEY,
                  secure=False)
        pass

    def mapper (self, key, value):
        self.s3.put_object(config.S3_BUCKET, config.s3_key(key), io.BytesIO(value), len(value))
        pass
    pass

# ./copy_s3_helper.py -r hadoop hdfs:///user/wdong/cms_stage2 --output-dir dummy --python-bin /opt/pypy3/bin/pypy3 --cmdenv CMS_HOME=/home/wdong/shared/cms2
if __name__ == '__main__':
    s3 = Minio(config.S3_SERVER,
          access_key=config.S3_ACCESS_KEY,
          secret_key=config.S3_SECRET_KEY,
          secure=False)
    s3.make_bucket(config.S3_BUCKET)
    CopyS3Job.run()
    pass

