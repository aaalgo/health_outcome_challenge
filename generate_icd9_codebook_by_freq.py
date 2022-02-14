#!/usr/bin/env python3
import bz2
import sys
from collections import defaultdict
import pickle
from cms import Tree
from icd import *

topk = 100

if len(sys.argv) > 2:
    topk = int(sys.argv[2])
    pass

stat = []
with open(sys.argv[1], 'r') as f:
    for l in f:
        cnt, name, _ = l.split('\t')
        cnt = int(cnt)
        stat.append((cnt, name))
        pass
    pass

stat.sort(reverse=True)

stat = stat[:(topk-1)]

tree = Tree(icd9)

for node in icd9.order:
    node.use = False

for _, name in stat:
    icd9.icd9[name].use = True

icd9.root.use = True

with open('icd9_codebook.pkl', 'wb') as f:
    pickle.dump(icd9, f)
    pass

