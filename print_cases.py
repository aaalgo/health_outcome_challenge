#!/usr/bin/env python3
import sys
import cms

loader = cms.CoreLoader(cms.loader)

for line in sys.stdin:
    c = loader.load(line.encode('ascii'), False)
    print('===', c.pid)
    for demo in c.demos():
        print('DEMO', demo.REFERENCE_YEAR, demo.VALID_DATE_OF_DEATH_SWITCH, demo.DATE_OF_DEATH)
        pass
    for claim in c.claims():
        print('CLAIM', claim.CLM_THRU_DT)
        pass
    pass

