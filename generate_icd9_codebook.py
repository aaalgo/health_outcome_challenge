#!/usr/bin/env python3
import bz2
import sys
from collections import defaultdict
import pickle
from cms import Tree
from icd import *

def load_icd_stats ():
    with bz2.open('meta/icd9_stat.bz2', 'rt') as f:
        for l in f:
            a, cnt = l.strip().split('\t')
            cnt = int(cnt)
            year, ctype, sub, field, code, version = a.strip('"').split('|')
            if 'PRCDR' in field:
                # PRCDR code may start with numberic
                continue
            if len(code) == 0:
                continue
            if version == '0':
                version = ICD_10
            elif version == '9':
                version = ICD_9
            else:
                version = ICD_UNKNOWN
                pass
            if code[0].isnumeric():
                version = ICD_9
            yield code, version, cnt
        pass
    pass


def lookup (code, book):
    while len(code) >= 3:
        v = book.get(code, None)
        if not v is None:
            return v
        code = code[:-1]
        pass
    return None

def load_icd9_stats (i10gem):
    for code, version, cnt in load_icd_stats():
        if version == ICD_9:
            yield code, cnt
        # unknown
        v = lookup(code, i10gem)
        if not v is None:
            yield v, cnt
        code = shorten(code)
        v = lookup(code, i10gem)
        if not v is None:
            yield v, cnt
            pass
        pass
    pass


icd9 = ICD9()

for k, node in enumerate(icd9.order):
    assert k == node.id
    node.weight = 0
    pass

for codes, cnt in load_icd9_stats(icd9.i10gem):
    if isinstance(codes, str):
        codes = [codes]
        pass
    codes = set(codes)
    nodes = []
    #weight = cnt / len(codes)
    for code in codes:
        node = icd9.icd9.get(code, None)
        if not node is None:
            nodes.append(node)
            pass
        pass
    if len(nodes) > 0:
        w = cnt / len(nodes)
        for node in nodes:
            node.weight += w
            pass
    pass

TOPK=100

if len(sys.argv) > 1:
    TOPK = int(sys.argv[1])
    pass


tree = Tree(icd9)
sel = tree.try_select(TOPK, 10000, 0)
sel.sort(key=lambda x: x[1])
for node in icd9.order:
    node.use = False
for i, p in sel:
    icd9.order[i].use = True
    pass

with open('icd9_codebook.pkl', 'wb') as f:
    pickle.dump(icd9, f)
    pass

