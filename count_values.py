#!/opt/pypy3/bin/pypy3
import sys
import os
import pickle
import mrjob
from mrjob.job import MRJob
import cms
import config


def is_numeric (v):
    try:
        float(v)
        return True
    except ValueError:
        return False
    pass


FIELDS = ['STATE_CD',
          'CNTY_CD',
          'SEX',
          'RACE',
          'AGE',
          'OREC',
          'CREC',
          'MS_CD',
          'A_TRM_CD',
          'B_TRM_CD',
          'RFRNC_YR',
          'V_DOD_SW',
          'PMT_AMT',
          'DOB_DT',
          'GNDR_CD',
          'RACE_CD',
          'CNTY_CD',
          'STATE_CD',
          'ICD_DGNS_CD',
          'CLM_POA_IND_SW',
          'ICD_DGNS_E_CD',
          'PRVSTATE',
          'ICD_PRCDR_CD']

def filter_specs (specs):
    lookup = {}
    for ctype, _, fields, _ in specs: #cms.loader.normer.specs:
        fs = []
        for field_name, subfields, _, _, _ in fields:
            add = False
            if subfields is None:
                if field_name in FIELDS:
                    add = True
            else:
                sfs = []
                for sf in subfields:
                    if sf in FIELDS:
                        sfs.append(sf)
                        add = True
                        pass
                    pass
                subfields = sfs
                pass
            if add:
                fs.append([field_name, subfields])
                pass
            pass
        lookup[ctype] = fs
        #print(ctype, fs)
        pass
    return lookup


class MergeJob (MRJob):

    INPUT_PROTOCOL = mrjob.protocol.BytesProtocol
    FILES = config.HADOOP_FILES
    JOBCONF = {'mapred.reduce.tasks': 100, 'mapred.child.renice': 10}

    def mapper_init (self):
        self.loader = cms.CaseLoader()
        self.fields_lookup = filter_specs(self.loader.normer.specs)
        pass

    def mapper (self, key, value):
        case = self.loader.load(value.decode('ascii'))
        for ctype in cms.CLAIM_TYPES + ['den']:
            fields = self.fields_lookup[ctype]
            rows = getattr(case, ctype)
            for row in rows:
                for field_name, subfields in fields:
                    col = getattr(row, field_name)
                    #if col is None:
                    #    continue
                    if subfields is None:
                        #yield '%s_%s_total' % (ctype, field_name), 1
                        #if not is_numeric(col):
                        # this column is string
                        if col is None:
                            col = '[]'
                        yield '%s|%s||%s' % (ctype, field_name, col), 1
                        pass
                    else:   # sub table
                        for r in col:
                            for subfield in subfields:
                                c = getattr(r, subfield)
                                if c is None:
                                    c = '[]'
                                #yield '%s_%s_%s_total' % (ctype, field_name, subfield), 1
                                #if not is_numeric(c):
                                yield '%s|%s|%s|%s' % (ctype, field_name, subfield, c), 1
                            pass
                        pass
                    pass
                pass
            pass
        pass

    def combiner (self, key, values):
        yield key, sum(values)

    def reducer (self, key, values):
        yield key, sum(values)

    pass

if __name__ == '__main__':
    MergeJob.run()
    pass

