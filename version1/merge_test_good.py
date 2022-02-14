#!/usr/bin/env python3
import os
import sys
import pickle
from glob import glob
from collections import defaultdict

CNTS = {
"car_claimsj_lds_5_2012.csv":	286822,
"car_linej_lds_5_2012.csv":	577809,
"den_saf_lds_5_2012.csv":	14586,
"dme_claimsj_lds_5_2012.csv":	24206,
"dme_linej_lds_5_2012.csv":	43115,
"hha_claimsj_lds_5_2012.csv":	2406,
"hha_instcond_lds_5_2012.csv":	328,
"hha_instoccr_lds_5_2012.csv":	181,
"hha_instval_lds_5_2012.csv":	9210,
"hha_revenuej_lds_5_2012.csv":	49080,
"hosp_claimsj_lds_5_2012.csv":	1199,
"hosp_instcond_lds_5_2012.csv":	379,
"hosp_instoccr_lds_5_2012.csv":	1162,
"hosp_instval_lds_5_2012.csv":	2220,
"hosp_revenuej_lds_5_2012.csv":	19787,
"inp_claimsj_lds_5_2012.csv":	5050,
"inp_instcond_lds_5_2012.csv":	6613,
"inp_instoccr_lds_5_2012.csv":	7734,
"inp_instval_lds_5_2012.csv":	18270,
"inp_revenuej_lds_5_2012.csv":	86029,
"out_claimsj_lds_5_2012.csv":	52086,
"out_instcond_lds_5_2012.csv":	25525,
"out_instoccr_lds_5_2012.csv":	62827,
"out_instval_lds_5_2012.csv":	119141,
"out_revenuej_lds_5_2012.csv":	484649,
"snf_claimsj_lds_5_2012.csv":	1839,
"snf_instcond_lds_5_2012.csv":	763,
"snf_instoccr_lds_5_2012.csv":	2913,
"snf_instval_lds_5_2012.csv":	3478,
"snf_revenuej_lds_5_2012.csv":	14509
}


def construct_2011_name (path):
    bname = os.path.basename(path)
    # 2011: hha_instcond_lds_5_2011.csv
    #       inp_instval_lds_2012_sample.csv
    return bname.replace('2012', '2011')


with open('meta.pkl', 'rb') as f:
    formats, lookup = pickle.load(f)
    pass

'''
def verify_header (path, header, cols):
    fs = header.strip().split(',')
    assert len(fs) >= len(cols), '%d %d' % (len(fs), len(cols))
    assert fs[0] == 'DESY_SORT_KEY'
    for h, c in zip(fs, cols):
        assert h == c.long_name
    if len(fs) > len(cols):
        for h in fs[len(cols):]:
            print("COLUMN NOT MAPPED: ", h, path)
    pass
    '''

def process_file (path):
    #global lookup
    #global formats
    bname = os.path.basename(path)
    bname2011 = construct_2011_name(path)
    fid = lookup[bname2011]
    #print(path, lookup[bname2011])
    ind, cols = formats[fid]

    with open(path, 'r') as f:
        for row, line in enumerate(f):
            line = line.strip()
            fs = line.split(',')
            assert len(fs) == len(cols)
            key = fs[0]
            assert len(key) == 9
            fs.insert(0, str(fid))
            yield key, ('%s,' % fid) + line
        pass
    pass

merged = defaultdict(lambda: [])

KEYS_2012 = defaultdict(lambda: [])
for path in ['tests/den_saf_lds_5_2012.csv']:
    for key, value in process_file(path):
        key1 = key[:6]
        KEYS_2012[key1].append(key)
        pass
    pass

print(len(KEYS_2012), 'keys in 2012 loaded.')

C = 0
#for path in glob('tests/*2012*.csv'):
for path in glob('tests/*.csv'):
    c = 0
    check_cnts = CNTS[os.path.basename(path)]
    del CNTS[os.path.basename(path)]
    for key, value in process_file(path):
        merged[key].append(value)
        c += 1
        pass
    print(c, 'rows loaded from ', path)
    assert (c == check_cnts)
    C += 1
    pass
assert len(CNTS) == 0


with open('/shared/data/CMS/tests/tests_2012', 'w') as f:
    for k, v in merged.items():
        f.write(k)
        f.write('\t')
        f.write('|'.join(v))
        f.write('\n')
        pass
    pass

