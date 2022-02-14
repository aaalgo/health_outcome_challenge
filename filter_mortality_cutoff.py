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
       self.add_file_arg('--gs', help='')
       pass

    def mapper_init (self):
        self.filter = {}
        with open(self.options.gs, 'r') as f:
            for l in f:
                label, pid, observe, cutoff = l.strip().split('\t')
                pid = int(pid)
                cutoff = int(cutoff)
                self.filter[pid] = cutoff
                pass
            pass

        fmts = [] # (is_den, ref year | thru_dt)
        for _, _, ctype, sub, _, cols, _ in cms.loader.loader.formats:
            lookup = {}
            for i, f in enumerate(cols):
                lookup[f.long_name] = i
                pass
            offset = None
            if ctype == 'den':
                is_den = True
                offset = lookup['REFERENCE_YEAR']
                f1 = lookup['VALID_DATE_OF_DEATH_SWITCH']
                f2 = lookup['DATE_OF_DEATH']
            else:
                is_den = False
                offset = lookup['CLM_THRU_DT']
                f1 = None
                f2 = None
                pass
            fmts.append((is_den, offset, f1, f2))
            pass
        self.fmts = fmts
        pass

    def mapper (self, key, value):
        pid = int(key.decode('ascii'))
        cutoff = self.filter.get(pid, None)
        if cutoff is None:
            return
        rows = []
        for row in value.split(b'|'):
            fs = row.split(b',')
            fid = int(fs[0])
            is_den, offset, remove_f1, remove_f2 = self.fmts[fid]
            v = int(fs[1 + offset])
            if is_den:
                assert v >= 10 and v <= 30
                year_begin = (2000 + v) * 10000;
                if year_begin < cutoff:
                    fs[1 + remove_f1] = b''
                    fs[1 + remove_f2] = b''
                    #rows.append(row)
                    rows.append(b','.join(fs))
                    pass
            else:
                thru_dt = v
                assert v >= 10000101 and v <= 30000101
                if thru_dt < cutoff:
                    rows.append(row)
                    pass
                pass
            pass
        yield key, b'|'.join(rows)
    pass


if __name__ == '__main__':
    MergeJob.run()
    pass

