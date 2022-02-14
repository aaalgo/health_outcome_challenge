#!/opt/pypy3/bin/pypy3
import sys
import os
import traceback
import pickle
import mrjob
from mrjob.job import MRJob
import cms
import config
import target

class MergeJob (MRJob):

    INPUT_PROTOCOL = mrjob.protocol.TextProtocol
    FILES = config.HADOOP_FILES
    JOBCONF = {'mapred.reduce.tasks': 10}

    def mapper_init (self):
        self.loader = cms.CaseLoader()
        pass

    def mapper (self, key, value):
        case = self.loader.load(value)
        # 统计DEN人数
        for den in case.den:
            year = '20%02d' % int(den.RFRNC_YR)
            yield 'den_%s' % year, 1
            yield 'den_%s_sex_%d' % (year,  int(den.SEX)), 1
            yield 'den_%s_state_%d' % (year,  int(den.STATE_CD)), 1
            #yield 'den_%s_cnty_%d' % (year,  int(den.CNTY_CD)), 1
            yield 'den_%s_race_%d' % (year,  int(den.RACE)), 1
            yield 'den_%s_age_%d' % (year,  int(den.AGE)), 1
            pass

        if len(case.den) > 0:
            den = case.den[-1]
            yield 'den', 1
            yield 'den_sex_%d' % int(den.SEX), 1
            yield 'den_state_%d' % int(den.STATE_CD), 1
            #yield 'den_cnty_%d' % int(den.CNTY_CD), 1
            yield 'den_race_%d' % int(den.RACE), 1
            yield 'den_age_%d' % int(den.AGE), 1

        for ctype in cms.CLAIM_TYPES:
            rows = getattr(case, ctype, [])
            labels = target.label_records(rows, ctype == 'inp', False)

            for row, labels in zip(rows, labels): #getattr(case, ctype):
                year = str((int(row.THRU_DT) // 10000))
                yield 'claim', 1
                yield 'claim_%s' % year, 1
                yield 'claim_state_%d' % int(row.STATE_CD), 1
                yield 'claim_%s_state_%d' % (year, int(row.STATE_CD)), 1
                yield 'pmt', row.PMT_AMT
                yield 'pmt_%s' % year, row.PMT_AMT
                yield ctype, 1
                yield '%s_%s' % (ctype, year), 1
                yield '%s_pmt' % ctype, row.PMT_AMT
                yield '%s_pmt_%s' % (ctype, year), row.PMT_AMT
                if labels[0]:
                    yield 'E', 1
                    yield 'E_%s' % year, 1
                    yield 'E_%s_%s' % (year, ctype), 1
                if labels[1]:
                    yield 'POA', 1
                    yield 'POA_%s' % year, 1
                    yield 'POA_%s_%s' % (year, ctype), 1
                pass
        pass

    def reducer (self, key, values):
        yield key, sum(values)
    pass


if __name__ == '__main__':
    MergeJob.run()
    pass

