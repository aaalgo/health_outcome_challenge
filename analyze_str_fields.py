#!/usr/bin/env python3
import sys
import os
import traceback
import math
import pickle
import config
import json
from glob import glob

ROOT = os.path.abspath(os.path.dirname(__file__))

tables = {}
for path in glob(ROOT + '/str_fields/*'):
    with open(path, 'r') as f:
        for l in f:
            k, v = l.strip().split('\t')
            k = k.strip('"')
            fs = k.split('|')
            t = fs[0]
            if not t in tables:
                tables[t] = {}
                pass
            table = tables[t]
            f = fs[1]
            if len(fs) == 2:
                table[f] = [[]]
            elif len(fs) == 3:
                if not f in table:
                    table[f] = {}
                table[f][fs[2]] = [[]]
            else:
                print(l)
                assert False
                pass
            pass


for path in glob(ROOT + '/str_values/*'):
    with open(path, 'r') as f:
        for l in f:
            k, c = l.strip().split('\t')
            k = k.strip('"')
            fs = k.split('|')
            c = int(c)
            if len(fs) == 3:
                t, f, v = fs
                sf = None

            elif len(fs) == 4:
                t, f, sf, v = fs
            else:
                assert False
                pass
            field = tables[t][f]
            if sf is None:
                field[0].append((v, c))
            else:
                field[sf][0].append((v, c))
            pass
        pass
    pass

def analyze_values (v):
    values = v[0]
    v.pop()
    values.sort(key=lambda k: k[1], reverse=True)
    total = sum([x for _, x in values])
    cnt_th = math.ceil(total * 0.8)
    r = []
    c = 0
    n = 0
    lookup = {}
    for x in values:
        if c >= cnt_th:
            break
        lookup[x[0]] = n
        n += 1
        pass
    if len(lookup) <= 1 or len(lookup) > 200:
        v.append(False)
        v.append(None)
        v.append(None)
    else:
        v.append(True)
        v.append(c >= total)
        v.append(lookup)
    #if len(r) > 20:
    #    print(v)
    #    pass
    #return r

for _, table in tables.items():
    for f, v in table.items():
        if type(v) is list:
            analyze_values(v)
        else:
            for sf, vv in v.items():
                analyze_values(vv)

print(json.dumps(tables, indent=4*' '))
with open('extractor.pkl', 'wb') as f:
    pickle.dump(tables, f)
    pass
