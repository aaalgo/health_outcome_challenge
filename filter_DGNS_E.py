#!/opt/pypy3/bin/pypy3
#!/usr/bin/env python3
import sys
import os
import traceback
import pickle
from datetime import datetime, timedelta
import mrjob
from mrjob.job import MRJob
import cms
import config
import target

def has_positive (case):
    # case
    # if has_unplanned(case.inp) or has_unplanned(case.snf):
    if target.has_dgns(case.inp):
        return True
    return False

class MergeJob (MRJob):

    INPUT_PROTOCOL = mrjob.protocol.BytesProtocol
    INTERNAL_PROTOCOL = mrjob.protocol.BytesProtocol
    OUTPUT_PROTOCOL = mrjob.protocol.BytesProtocol
    FILES = config.HADOOP_FILES
    JOBCONF = {'mapred.reduce.tasks': 0, 'mapred.child.renice': 10}

    def mapper_init (self):
        self.loader = cms.CaseLoader()
        pass

    def mapper (self, key, value):
        case = self.loader.load(value.decode('ascii'))
        if has_positive(case):
            yield key, value
    pass


if __name__ == '__main__':
    MergeJob.run()
    pass

