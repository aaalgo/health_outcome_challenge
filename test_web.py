#!/usr/bin/env python3
import os
import requests
import config

def load_raw_case (pid):
    resp = requests.get(config.WEB_SERVER + '/api/raw/?pid=%d' % pid)
    return resp.content


if __name__ == '__main__':
    import cms
    import sys
    loader = cms.CoreLoader(cms.loader)
    pid = 100036065
    if len(sys.argv) > 1:
        pid = int(sys.argv[1])
        pass
    buf = load_raw_case(pid)
    case = loader.load(buf, True)
    for demo in case.demos():
        print('DEMO', demo.REFERENCE_YEAR)
        pass
    for c in case.claims():
        print('CLAIM', c.CLM_THRU_DT, c.ctype(), )
        pass
    pass

