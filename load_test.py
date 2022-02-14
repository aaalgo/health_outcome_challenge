#!/usr/bin/env python3
import sys
import cms

loader = cms.CaseLoader()
icd = cms.ICD()

input_path = '/shared/data/CMS/tests/tests_2012'

if len(sys.argv) > 1:
    input_path = sys.argv[1]

with open(input_path, 'r') as f:
    c = 0
    for l in f:
        l = l.strip()
        if len(l) == 0:
            break
        c += 1
        k, v = l.split('\t')

        case = loader.load(v)
        print(case.pid)
        for c in case.car:
            print(c.THRU_DT)
            for d in c.dgns:
                print('\t', d.ICD_DGNS_CD, icd.explain(d.ICD_DGNS_CD))
                pass
        pass
    pass

print(c)
