#!/usr/bin/env python3
import sys
import os
import traceback
from operator import add
import pickle
import mrjob
from mrjob.job import MRJob
import cms
import config

SPECIAL_DATES = ['CLM_HOSPC_START_DT_ID', 'NCH_BENE_MDCR_BNFTS_EXHTD_DT_I']

def extract_key (line, cols, ind):
    key = ind[3]
    assert not b'|' in line
    return line.split(b',', key+1)[key]

def construct_2011_name (path):
    bname = os.path.basename(path)
    # 2011: hha_instcond_lds_5_2011.csv
    #       inp_instval_lds_2012_sample.csv
    return bname.replace('2012', '2011')


class MergeJob (MRJob):

    INPUT_PROTOCOL = mrjob.protocol.BytesValueProtocol
    INTERNAL_PROTOCOL = mrjob.protocol.BytesProtocol
    OUTPUT_PROTOCOL = mrjob.protocol.BytesProtocol

    FILES = config.HADOOP_FILES
    JOBCONF = {'mapred.reduce.tasks': 500}

    def mapper_init (self):
        with open('meta.pkl', 'rb') as f:
            formats, lookup = pickle.load(f)
        fname = os.environ['mapreduce_map_input_file']
        bname2011 = construct_2011_name(fname)
        self.fid = lookup[bname2011]
        self.ind, self.cols = formats[self.fid]

        to_patch = []
        for i, col in enumerate(self.cols):
            if not col.type is int:
                continue
            v = col.long_name.find('_DT')
            if v < 0:
                continue
            suff = col.long_name[(v + 3):]
            if len(suff) == 0 or suff.isnumeric() or col.long_name in SPECIAL_DATES:
                to_patch.append((i, col))
                pass
            pass
        self.to_patch = to_patch
        pass

    def mapper (self, _, line):
        code = line
        try:
            line = line.decode('ascii')
            line = line.strip()
            fs = line.split(',')
            assert len(fs) == len(self.cols)
            key = fs[0]
            assert len(key) == 9
            for i, col in self.to_patch:
                if len(fs[i]) == 0:
                    continue
                assert fs[i].isnumeric(), 'xxx:%s' % fs[i]
                x = int(fs[i])
                if col.long_name == 'DOB_DT' and  x < 100:
                    continue
                assert x > 18000000
                x = x - 10000
                if x == 20110229:
                    x = 20110228
                fs[i] = str(x)
                pass
            fs.insert(0, str(self.fid))
            key = key.encode('ascii')
            line = ','.join(fs).encode('ascii')
            self.increment_counter('cmr', 'record_count', 1)
            yield key, line
        except:
            traceback.print_exc(None, sys.stderr)
            self.increment_counter('cmr', 'bad_lines', 1)
            yield b'error', code
            pass
        pass

    def reducer (self, key, values):
        yield key, b'|'.join(values)
    pass


if __name__ == '__main__':
    MergeJob.run()
    pass

