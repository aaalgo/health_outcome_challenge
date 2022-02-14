#!/usr/bin/env python3
import sys
import os
import traceback
import pickle
import mrjob
from mrjob.job import MRJob
import cms
import olap
import config

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
        yield key, olap.encode(olap.Case(case))
    pass


if __name__ == '__main__':
    MergeJob.run()
    pass

