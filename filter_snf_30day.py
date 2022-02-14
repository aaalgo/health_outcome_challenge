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

def load_dt (dt):
    day = dt % 100 
    dt = dt // 100 
    month = dt % 100 
    year = dt // 100 
    return datetime(year, month, day)

TH = timedelta(days=30)

def has_unplanned (rows):
    rows.sort(key=lambda x: x.THRU_DT)
    for i in range(1, len(rows)):
        pre = rows[i-1]
        cur = rows[i]
        a=str(pre.THRU_DT)
        if (('2008' in a) or ('2009' in a)):
            pass
        else:
            if load_dt(cur.ADMSN_DT) - load_dt(pre.THRU_DT) < TH:
                return True
        pass
    return False

def has_positive (case):
    # case
    # if has_unplanned(case.inp) or has_unplanned(case.snf):
    if has_unplanned(case.snf):
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

