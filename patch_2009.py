#!/usr/bin/env python3
import cms
import subprocess as sp

hostname = sp.check_output('hostname', shell=True)
hostname = hostname.decode('ascii').strip()
assert hostname == 'gpu1', hostname

ROOT = '/home/wdong/cms_raw/'

for dtype in cms.CLAIM_TYPES:
    print("Patching", dtype)
    old_path = ROOT + dtype + "_clm_saf_lds_5_2009.csv"
    patch_path = ROOT + "patch/" + dtype + "_clm_saf_lds_fd_5_2009.csv"
    new_path = ROOT + "new/" + dtype + "_clm_saf_lds_5_2009.csv"

    old_fts = "data/" + dtype + "_clm_saf_lds_5_2009.fts"
    patch_fts = "data/" + dtype + "_clm_saf_lds_fd_5_2009.fts"

    old_columns = cms.load_columns(old_fts)
    patch_columns = cms.load_columns(patch_fts)
    print(len(patch_columns))
    #for col in patch_columns:
    #    print('\t', col.long_name)
    old_lookup = {f.long_name: i for i, f in enumerate(old_columns)}
    replaces = []
    for i, f in enumerate(patch_columns):
        assert f.long_name in old_lookup
        patch_f, old_f = i, old_lookup[f.long_name]
        if f.long_name == 'CLAIM_NO':
            patch_claim_no = patch_f
            old_claim_no = old_f
        elif f.long_name == 'DESY_SORT_KEY':
            patch_dsysrtky = patch_f
            old_dsysrtky = old_f
        else:
            replaces.append((patch_f, old_f))
            pass
        pass
    patch_mapping = 'tmp/%s.patch_mapping' % dtype
    with open(patch_mapping, 'w') as f:
        f.write('%d %d %d %d %d %d\n' % (len(patch_columns), len(old_columns), patch_claim_no, old_claim_no, patch_dsysrtky, old_dsysrtky))
        for p, o in replaces:
            f.write('%d %d\n' % (p, o))
            pass
        pass

    sp.check_call('./patch_2009_helper %s %s %s %s' % (old_path, patch_path, new_path, patch_mapping), shell=True)
    pass

