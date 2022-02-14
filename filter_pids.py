#!/opt/pypy3/bin/pypy3
import sys
import os
import traceback
import pickle
from datetime import datetime, timedelta
import mrjob
from mrjob.job import MRJob
import setup_mrjob
import cms
import config

class MergeJob (MRJob):

    INPUT_PROTOCOL = mrjob.protocol.BytesProtocol
    INTERNAL_PROTOCOL = mrjob.protocol.BytesProtocol
    OUTPUT_PROTOCOL = mrjob.protocol.BytesProtocol
    FILES = config.HADOOP_FILES
    JOBCONF = {'mapred.reduce.tasks': 0, 'mapred.child.renice': 10}

    def configure_args(self):
       super(MergeJob, self).configure_args()
       self.add_file_arg('--list', default=os.path.join(cms.CMS_HOME, 'meta/split4'), help='')
       pass

    def mapper_init (self):
        self.filter = {}
        with open(self.options.list, 'r') as f:
            for l in f:
                fs = l.strip().split('\t')
                pid = fs[0]
                self.filter[pid] = 1
                pass
            pass
        pass

    def mapper (self, key, value):
        pid = key.decode('ascii')
        if pid in self.filter:
            yield key, value
    pass


if __name__ == '__main__':
    MergeJob.run()
    # ./filter_pids.py -r hadoop hdfs:///user/wdong/cms_stage2 --output-dir cms_test/ --python-bin /opt/pypy3/bin/pypy3 --cmdenv CMS_HOME=/home/wdong/shared/cms2
    pass

