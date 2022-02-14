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

def extract_key (line, cols, ind):
    key = ind[3]
    assert not b'|' in line
    return line.split(b',', key+1)[key]

class MergeJob (MRJob):

    INPUT_PROTOCOL = mrjob.protocol.BytesValueProtocol
    INTERNAL_PROTOCOL = mrjob.protocol.BytesProtocol
    OUTPUT_PROTOCOL = mrjob.protocol.BytesProtocol

    FILES = config.HADOOP_FILES
    JOBCONF = {'mapred.reduce.tasks': 500}

    def mapper_init (self):
        with open('meta2.pkl', 'rb') as f:
            version, formats, lookup = pickle.load(f)
            assert version == cms.META_VERSION
        fname = os.environ['mapreduce_map_input_file']
        bname = os.path.basename(fname)
        self.fid = lookup[bname]
        self.ind, self.cols = formats[self.fid]
        pass

    def mapper (self, _, line):
        code = line
        try:
            k = extract_key(line, self.cols, self.ind)
            self.increment_counter('cmr', 'record_count', 1)
            yield k, ('%d,' % self.fid).encode('ascii') + code
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

