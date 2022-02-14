#!/usr/bin/env python3
import os
import sys
import json
import cms
from glob import glob

def remove_jk (sub, ch):
    return sub
    ###################
    if sub[-1] == ch:
        sub = sub[:-1]
        pass
    return sub

def scan_files ():
    tasks = []
    '''
    for year in [2008, 2009, 2010]:
        tasks.append(('den_saf_lds_5_%d' % year, (year, 'den', None)))
        for ctype in cms.CLAIM_TYPES:
            if ctype == 'car' and year == 2010:
                tasks.append(('%s_clm_saf_lds_5_%da' % (ctype, year), (year, ctype, None)))
                tasks.append(('%s_clm_saf_lds_5_%db' % (ctype, year), (year, ctype, None)))
            else:
                tasks.append(('%s_clm_saf_lds_5_%d' % (ctype, year), (year, ctype, None)))
            pass
        pass
    '''
    for year in [2011, 2012, 2013, 2014, 2015]:
        tasks.append(('den_saf_lds_5_%d' % year, (year, 'den', None)))
        for ctype in ['car', 'dme']:
            for sub in ['claimsj', 'linej']:
                tasks.append(('%s_%s_lds_5_%d' % (ctype, sub, year), (year, ctype, remove_jk(sub, 'j'))))
            pass
        for ctype in ['inp', 'out', 'hha', 'snf', 'hosp']:
            for sub in ['claimsj', 'instcond', 'instoccr', 'instval', 'revenuej']:
                tasks.append(('%s_%s_lds_5_%d' % (ctype, sub, year), (year, ctype, remove_jk(sub, 'j'))))
            pass
        pass
    for year in [2016, 2017]:
        tasks.append(('den_saf_lds_5_%d' % year, (year, 'den', None)))
        for ctype in ['car', 'dme']:
            for sub in ['claimsk', 'linek', 'demo']:
                tasks.append(('%s_%s_lds_5_%d' % (ctype, sub, year), (year, ctype, remove_jk(sub, 'k'))))
            pass
        for ctype in ['inp', 'out', 'hha', 'snf', 'hosp']:
            for sub in ['claimsk', 'instcond', 'instoccr', 'instval', 'revenuek', 'demo']:
                tasks.append(('%s_%s_lds_5_%d' % (ctype, sub, year), (year, ctype, remove_jk(sub, 'k'))))
            pass
        pass
    # 只能递增，上面已有的不能删掉
    #cnt = len(cms.deep_glob('*.csv'))
    #assert cnt == len(tasks)
    return tasks

if __name__ == '__main__':

    tasks = scan_files()
    lookup = {}
    formats = []
    fts_files = {}
    csv_files = {}
    for path in glob('data/*.fts'):
        fts_files[os.path.basename(path)] = 1
        pass
    for path in glob('data/*.csv'):
        csv_files[os.path.basename(path)] = 1
        pass
    for bname, ind in tasks:
        fid = len(formats)
        year = ind[0]
        # sanity check files
        if year >= 2013:
            del csv_files[bname + '.csv']
            pass
        if '2010a' in bname:
            continue
        elif '2010b' in bname:
            del fts_files[bname.replace('2010b', '2010') + '.fts']
        else:
            del fts_files[bname + '.fts']
            pass

        cols = cms.load_columns_bname(bname)
        key = None
        for i, col in enumerate(cols):
            if col.long_name == 'DESY_SORT_KEY':
                key = i
                break
            pass
        assert not key is None
        lookup[bname + '.csv'] = fid
        formats.append((ind + (key, ), cols))
        pass

    meta_name = 'meta2.json'
    assert not os.path.exists(meta_name), "meta2.pkl exists"
    with open(meta_name, 'w') as f:
        json.dump([cms.META_VERSION, formats, lookup], f)
        pass

    print("UNUSED CSV:")
    for key, _ in csv_files.items():
        print(key)
    print("UNUSED FTS:")
    for key, _ in fts_files.items():
        print(key)
    pass


