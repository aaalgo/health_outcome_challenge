#!/opt/pypy3/bin/pypy3
import sys
import os
import random
import mrjob
from mrjob.job import MRJob
import setup_mrjob
import cms
import config

def is_dead (case):
    for demo in case.demos():
        if not demo.DATE_OF_DEATH is None:
            return True
    return False

class MergeJob (MRJob):

    INPUT_PROTOCOL = mrjob.protocol.BytesProtocol
    INTERNAL_PROTOCOL = mrjob.protocol.BytesProtocol
    OUTPUT_PROTOCOL = mrjob.protocol.BytesProtocol
    FILES = config.HADOOP_FILES
    JOBCONF = {'mapred.reduce.tasks': 0, 'mapred.child.renice': 10}

    def configure_args(self):
       super(MergeJob, self).configure_args()
       self.add_file_arg('--black', default=None, help='')
       self.add_passthru_arg('--bg_ratio', default=5, type=int, help='1/5')
       pass

    def mapper_init (self):
        self.black = {}
        self.bg_ratio = self.options.bg_ratio
        if not self.options.black is None:
            with open(self.options.black, 'r') as f:
                for l in f:
                    fs = l.strip().split('\t')
                    pid = int(fs[0])
                    self.black[pid] = 1
                    pass
                pass
        self.loader = cms.CoreLoader(cms.loader)
        pass

    def mapper (self, key, value):
        assert type(value) is bytes
        case = self.loader.load(value, True)
        if case.pid in self.black:
            return
        use = False
        if is_dead(case):
            use = True
        elif random.randint(1, self.bg_ratio) == 1:
            use = True
            pass
        if use:
            yield key, value
        pass
    pass


if __name__ == '__main__':
    MergeJob.run()
    # ./filter_pids.py -r hadoop hdfs:///user/wdong/cms_stage2 --output-dir cms_test/ --python-bin /opt/pypy3/bin/pypy3 --cmdenv CMS_HOME=/home/wdong/shared/cms2
    pass

