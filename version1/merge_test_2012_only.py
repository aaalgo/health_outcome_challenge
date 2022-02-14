#!/usr/bin/env python3
import os
import sys
import pickle
from glob import glob
from collections import defaultdict


def construct_2011_name (path):
    bname = os.path.basename(path)
    # 2011: hha_instcond_lds_5_2011.csv
    #       inp_instval_lds_2012_sample.csv
    if bname == 'den_saf_lds_5_2012_sample.csv':
        return  'den_saf_lds_5_2011.csv'
    return bname.replace('_lds_2012_sample.csv', '_lds_5_2011.csv')


with open('meta.pkl', 'rb') as f:
    formats, lookup = pickle.load(f)
    pass

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

MONTH_LOOKUP = {
"JAN":"01",
"FEB":"02",
"MAR":"03",
"APR":"04",
"MAY":"05",
"JUN":"06",
"JUL":"07",
"AUG":"08",
"SEP":"09",
"OCT":"10",
"NOV":"11",
"DEC":"12"
}


def patch_date (f):
    day = f[:2]
    v = int(day)
    assert v >= 1 and v <= 31
    month = f[2:5]
    year = f[5:]
    v = int(year)
    return year + MONTH_LOOKUP[month] + day, v

SPECIAL_DATES = ['CLM_HOSPC_START_DT_ID', 'NCH_BENE_MDCR_BNFTS_EXHTD_DT_I']

def process_file (path, fid, cols, log_path):
    cnt_patch = 0
    cnt_good = 0
    cnt_bad = 0
    cnt_empty = 0
    #with open(path, 'r') as f, \
    #     open(log_path, 'w') as log:
    with open(path, 'r') as f:
        header = next(f)
        verify_header(path, header, cols)
        to_patch = []
        for i, col in enumerate(cols):
            if not col.type is int:
                continue
            v = col.long_name.find('_DT')
            if v < 0:
                continue
            suff = col.long_name[(v + 3):]
            if len(suff) == 0 or suff.isnumeric() or col.long_name in SPECIAL_DATES:
                to_patch.append((i, col))
                pass

        for row, line in enumerate(f):
            fs = line.strip().split(',')
            key = fs[0]
            for i, col in to_patch:
                f = fs[i]
                if f.isnumeric():
                    cnt_good += 1
                else:
                    try:
                        fs[i], year = patch_date(f)
                        if year < 1950 or year > 2012:
                            print('BAD DATE row_%d col_%d %s: %s' % (row, i, col.long_name, f))
                        cnt_patch += 1
                    except:
                        if len(f.strip()) == 0:
                            cnt_empty += 1
                        else:
                            raise
                            assert False, f
                            #log.write('row_%d col_%d %s: %s\n' % (row, i, col.long_name, f))
                            cnt_bad += 1
                        fs[i] = ''      # EMPTY VALUES
                    pass
            fs.insert(0, str(fid))
            yield key, ','.join(fs)
        #log.write('good dates: %d\n' % cnt_good)
        #log.write('fixed dates: %d\n' % cnt_patch)
        #log.write('bad dates: %d\n' % cnt_bad)
        #log.write('empty dates: %d\n' % cnt_empty)
        pass
    #if cnt_bad == 0:
    #    os.remove(log_path)
    pass


merged = defaultdict(lambda: [])

C = 0
for path in glob('tests/*2012*.csv'):
    bname = os.path.basename(path)
    bname2011 = construct_2011_name(path)
    fid = lookup[bname2011]
    #print(path, lookup[bname2011])
    ind, cols = formats[fid]
    c = 0
    for key, value in process_file(path, fid, cols, os.path.join('merge_test_log', bname)):
        merged[key].append(value)
        c += 1
        pass
    print(c, 'rows loaded from ', path)
    C += 1
    pass


with open('/shared/data/CMS/tests/tests_2012', 'w') as f:
    for k, v in merged.items():
        f.write(k)
        f.write('\t')
        f.write('|'.join(v))
        f.write('\n')
        pass
    pass

