#!/usr/bin/env python3
import os
import sys
import json
import cms

SUB_LOOKUP = {
'den': [
    ('buyin', ['ENTITLEMENT_BUY_IN_IND']),
    ('hmoind', ['HMO_INDICATOR']),
],
'inp': [
    ('dgns', ['ICD_DGNS_CD', 'CLM_POA_IND_SW']),
    ('dgns_e', ['ICD_DGNS_E_CD', 'CLM_E_POA_IND_SW']),
    ('prcdr', ['ICD_PRCDR_CD', 'PRCDR_DT']),
 ],
'out': [
    ('dgns', ['ICD_DGNS_CD']),
    ('dgns_e', ['ICD_DGNS_E_CD']),
    ('prcdr', ['ICD_PRCDR_CD', 'PRCDR_DT']),
],
'snf':[
    ('dgns', ['ICD_DGNS_CD']),
    ('dgns_e', ['ICD_DGNS_E_CD']),
    ('prcdr', ['ICD_PRCDR_CD', 'PRCDR_DT']),
],
'hosp':[
    ('dgns', ['ICD_DGNS_CD']),
    ('dgns_e', ['ICD_DGNS_E_CD']),
],
'hha': [
    ('dgns', ['ICD_DGNS_CD']),
    ('dgns_e', ['ICD_DGNS_E_CD']),
],
'car': [
    ('dgns', ['ICD_DGNS_CD']),
],
'dme': [
    ('dgns', ['ICD_DGNS_CD']),
]
}

IGNORE = [
          'PRNCPAL_DGNS_VRSN_CD',
          'ICD_DGNS_VRSN_CD',
          'FST_DGNS_E_VRSN_CD',
          'ICD_DGNS_E_VRSN_CD',
          'ADMTG_DGNS_VRSN_CD',
          'ICD_PRCDR_VRSN_CD',
          'RSN_VISIT_VRSN_CD',
          ]

SUB_IGNORE = [  # 这些在claimsj里都有，不需要重复
        'DESY_SORT_KEY',
        'CLAIM_NO',
        'CLM_THRU_DT',
        'NCH_CLM_TYPE_CD'
        ]

def split_column_stem_suffix (orig_fname):
    # 如果没有suffix，返回suffix=0
    fname = orig_fname
    while fname[-1].isnumeric():
        fname = fname[:-1]
    suffix = orig_fname[len(fname):]
    if len(suffix) == 0:
        return fname, 0
    return fname, int(suffix)


# 确认2008-2010年的格式是一样的
def compare_format (f1, f2):
    assert len(f1) == len(f2)
    for a, b in zip(f1, f2):
        assert a.no == b.no
        assert a.long_name == b.long_name
        assert a.short_name == b.short_name
    pass

def generate_master_mapping_den (sub_lookups, fields, output_field_spec, ignore):
    # append to output_field_spec
    subs = [[] for _ in sub_lookups] # 每张子表格的字段
    for f in fields:
        stem, suffix = split_column_stem_suffix(f.long_name)
        if stem in ignore:
            continue
        used = False
        for (_, lookup), sub in zip(sub_lookups, subs):
            if stem in lookup:
                if suffix == 1:
                    sub.append((stem, f))
                used = True
        if not used:
            output_field_spec.append([f.long_name, None, [(f.long_name, None)], f.line])
            pass
        pass
    for (name, _), sub in zip(sub_lookups, subs):
        names = [x for x, _ in sub]
        lines = [f.line for _, f in sub]
        output_field_spec.append([name, names, [(None, names)], lines])
        pass
    pass

def generate_master_mapping (sub_lookups, fields, output_field_spec, ignore):
    # append to output_field_spec
    subs = [[] for _ in sub_lookups] # 每张子表格的字段
    for f in fields:
        stem, suffix = split_column_stem_suffix(f.long_name)
        if stem in ignore:
            continue
        used = False
        for (_, lookup), sub in zip(sub_lookups, subs):
            if stem in lookup:
                if suffix == 1:
                    sub.append((stem, f))
                used = True
        if not used:
            output_field_spec.append([f.long_name, None, [(f.long_name, None), (f.long_name, None)], f.line])
            pass
        pass
    for (name, _), sub in zip(sub_lookups, subs):
        names = [x for x, _ in sub]
        lines = [f.line for _, f in sub]
        output_field_spec.append([name, names, [(None, names), (None, names)], lines])
        pass
    pass


def remove_jk (name):
    if name[-1] == 'j':
        return name[:-1]
    return name

def name_j (name):
    return name

def name_k (name):
    if name[-1] == 'j':
        return name[:-1] + 'k'
    return name

def generate_slave_mapping (ctype, table_name, fields, output_field_spec, ignore):
    # append to output_field_spec
    names = []
    lines = []
    for f in fields:
        if f.long_name in ignore:
            continue
        names.append(f.long_name)
        lines.append(f.line)
        pass
    #print(ctype)
    #print('\t', name_j(table_name), name_k(table_name))
    output_field_spec.append([remove_jk(table_name), names, [('%s_%s' % (ctype, name_j(table_name)), names), ('%s_%s' % (ctype, name_k(table_name)), names)], lines])
    pass

# 分析DEN格式
def analyze_denom ():
    fs1 = None
    for path in cms.deep_glob('den_saf_lds_5_*.fts'):
        fs = cms.load_columns(path)
        if fs1 is None:
            fs1 = fs
            continue
        compare_format(fs1, fs)
        pass
    fspec = []
    generate_master_mapping_den(SUB_LOOKUP['den'], fs1, fspec, [])
    return ['den', ['den'], fspec]

def analyze_claim_type (ctype):

    fspec = []

    claim_paths = cms.deep_glob('%s_claims[jk]_lds_5_2015.fts' % ctype) + cms.deep_glob('%s_[^c]*_lds_5_2015.fts' % ctype)
    # 先看claims (is_master)

    for claim_path in claim_paths:
        fs = cms.load_columns(claim_path)
        table_name = claim_path.split('_')[1]
        #if table_name[-1] == 'j' or table_name[-1] == 'k':
        #    table_name = table_name[:-1]
        #    pass
        is_master = (table_name == 'claimsj')
        #print(claim_path, table_name, is_master)
        # 'claimsj'是主表, 别的是附表

        if is_master:
            generate_master_mapping(SUB_LOOKUP[ctype], fs, fspec, IGNORE)
        else:
            generate_slave_mapping(ctype, table_name, fs, fspec, SUB_IGNORE)
        pass

    return [ctype, ['%s_claimsj' % ctype, '%s_claimsk' % ctype], fspec]


specs = [analyze_denom()]
for ctype in cms.CLAIM_TYPES:
    spec = analyze_claim_type(ctype)
    specs.append(spec)
    pass

with open('mapping2.json', 'w') as f:
    json.dump(specs, f)
    pass



