#!/usr/bin/env python3
import os
import json
from collections import defaultdict
ROOT = os.path.abspath(os.path.dirname(__file__))

def shorten (code):
    if len(code) == 7:
        code = code[:6]
        while len(code) > 1 and code[-1] == 'X':
            code = code[:-1]
    return code

def patch_shorten (book):
    new_entries = defaultdict(set)
    for k, v in book.items():
        if len(k) == 7:
            k = shorten(k)
            if not k in book:
                new_entries[k] = new_entries[k] | set(v)
                pass
            pass
        pass
    for k, v in new_entries.items():
        book[k] = list(v)
        pass
    pass

def merge_dict_items (book):
    MAX_LENGTH = 7
    exist_lengths = [{} for _ in range(MAX_LENGTH+1)]
    new_entries = []

    for k, v in book.items():
        l = len(k)
        assert l <= MAX_LENGTH, k
        exist_lengths[l][k] = v
        pass
    for l in [7, 6, 5, 4]:
        common = defaultdict(list)
        for k, v in exist_lengths[l].items():
            pref = k[:-1]
            if pref in exist_lengths[l-1]:
                continue
            common[pref].append(v)
            pass
        for k, vs in common.items():
            # check if vs are all the same
            merged = set()
            sizes = []
            for v in vs:
                s = set(v)
                sizes.append(len(s))
                merged = merged | s
                pass
            sizes.sort()
            if sizes[0] == len(merged):
                v = list(merged)
                new_entries.append((k, v))
                exist_lengths[l-1][k] = v
                pass
            pass
        pass
    print("%d new entries found." % len(new_entries))
    for k, v in new_entries:
        book[k] = v
    pass



def load_txt (relpath, maxsplit=None):
    with open(os.path.join(ROOT, relpath), 'rb') as f:
        for l in f:
            #print(l)
            l = l.decode('utf-8')
            fs = [x for x in l.strip().split(' ') if len(x) > 0]
            if maxsplit:
                fs1 = fs[:maxsplit]
                rest = fs[maxsplit:]
                if len(rest) > 0:
                    fs1.append(' '.join(rest))
                fs = fs1
                pass
            yield fs
            pass
        pass
    pass

FLAG_ICD9_SG = 1
FLAG_ICD9_DX = 2
FLAG_ICD9_VIRTUAL =4

ICD_UNKNOWN = 0
ICD_9 = 9
ICD_10 = 10

def icd9_number (x):
    x = x[:3]
    if x[0] == 'E':     # E = 10..
        return 1000 + int(x[1:])
    if x[0] == 'V':     # V = 11..
        return 1100 + int(x[1:])
    return int(x)

class ICD9Node:
    def __init__ (self):
        self.flags = 0
        self.name = None
        self.desc = None
        self.embedding = None
        self.parent = None
        self.parent_id = None
        self.range = None
        self.depth = None
        self.weight = None
        self.acc_weight = None
        self.id = None
        self.use = None
        pass

def lookup_with_shorten (book, code):
    while len(code) >= 3:
        v = book.get(code, None)
        if not v is None:
            return v, code
        code = code[:-1]
        pass
    return None, None

ICD9_CHAPTERS = [
('001-139', 'Infectious And Parasitic Diseases'),
('140-239', 'Neoplasms'),
('240-279', 'Endocrine, Nutritional And Metabolic Diseases, And Immunity Disorders'),
('280-289', 'Diseases Of The Blood And Blood-Forming Organs'),
('290-319', 'Mental Disorders'),
('320-389', 'Diseases Of The Nervous System And Sense Organs'),
('390-459', 'Diseases Of The Circulatory System'),
('460-519', 'Diseases Of The Respiratory System'),
('520-579', 'Diseases Of The Digestive System'),
('580-629', 'Diseases Of The Genitourinary System'),
('630-679', 'Complications Of Pregnancy, Childbirth, And The Puerperium'),
('680-709', 'Diseases Of The Skin And Subcutaneous Tissue'),
('710-739', 'Diseases Of The Musculoskeletal System And Connective Tissue'),
('740-759', 'Congenital Anomalies'),
('760-779', 'Certain Conditions Originating In The Perinatal Period'),
('780-799', 'Symptoms, Signs, And Ill-Defined Conditions'),
('800-999', 'Injury And Poisoning'),
('V01-V91', 'Supplementary Classification Of Factors Influencing Health Status And Contact With Health Services'),
('E00-E99', 'Supplementary Classification Of External Causes Of Injury And Poisoning'),
('[MISC]', None)
]

class ICD9:
    def __init__ (self):
        self.icd9 = defaultdict(ICD9Node)
        cnt = 0
        for code, desc in load_txt('meta/CMS32_DESC_LONG_DX.txt', 1):
            node = self.icd9[code]
            node.flags += FLAG_ICD9_DX
            node.desc = desc
            cnt += 1
            pass
        print('%d ICD9 DX loaded.' % cnt)
        cnt = 0
        for code, desc in load_txt('meta/CMS32_DESC_LONG_SG.txt', 1):
            node = self.icd9[code]
            node.flags += FLAG_ICD9_SG
            node.desc = desc
            cnt += 1
            pass
        print('%d ICD9 SG loaded.' % cnt)
        print('%d total ICD9 loaded.' % len(self.icd9))
        ICD9_ROOT = '[]'
        assert not ICD9_ROOT in self.icd9
        icd9_root = self.icd9[ICD9_ROOT]
        icd9_root.flags = FLAG_ICD9_VIRTUAL
        icd9_root.depth = 0
        self.root = icd9_root

        for code, _ in ICD9_CHAPTERS:
            node = self.icd9[code]
            node.flags = FLAG_ICD9_VIRTUAL
            node.depth = 1
            node.parent = icd9_root
            pass
        icd9_misc = self.icd9['[MISC]']

        with open(os.path.join(ROOT, 'meta/codes.json'), 'r') as f:
            codes = json.load(f)
            pass
        notfound = 0
        addparent = 0
        for chain in codes:
            parent = icd9_root
            for i, one in enumerate(chain):
                code = one['code']
                assert one['depth'] == i + 1
                if code is None:
                    href = one['href']
                    code = '[%s]' % (href.split('=')[2])
                else:
                    code = code.replace('.', '')
                    pass
                if ('-' in code) or ('[' in code):
                    node = self.icd9[code]
                    if node.flags == 0:
                        # new
                        node.flags = FLAG_ICD9_VIRTUAL
                        node.depth = one['depth']
                    else:
                        assert node.depth == one['depth']
                    pass
                else:
                    node = self.icd9[code]
                    pass
                if node is None:
                    print("NOTFOUND", code)
                    notfound += 1
                    break
                if node.parent is None:
                    node.parent = parent
                    addparent += 1
                    pass
                parent = node
                pass
            pass # process one chain
        print("added %d parents." % addparent)
        print("notfound %d." % notfound)

        # establish missing parents
        ranges = []
        for code, node in self.icd9.items():
            if not '-' in code:
                continue
            a, b = code.split('-')
            a, b = icd9_number(a), icd9_number(b)
            ranges.append((node.depth, a, b, node))
            pass
        ranges.sort(reverse=True, key=lambda x: x[0])

        addparent1 = 0
        addparent2 = 0
        addparent3 = 0
        for code, node in self.icd9.items():
            if not node.parent is None:
                continue
            if (node.flags & (FLAG_ICD9_SG | FLAG_ICD9_DX)) == 0:
                assert node is icd9_root
                continue
            for i in range(1, len(code)-3):
                prefix = code[:-i]
                node.parent = self.icd9.get(prefix, None)
                if not node.parent is None:
                    addparent1 += 1
                    break
                pass
            if node.parent is None:
                xxx = icd9_number(code)
                for _, a, b, parent in ranges:
                    if xxx >= a and xxx <= b:
                        node.parent = parent
                        node.depth = parent.depth + 1
                        addparent2 += 1
                        break
            if node.parent is None:
                node.parent = icd9_misc
                node.depth = icd9_misc.depth + 1
                #print('MISC:', code)
                addparent3 += 1
            assert not node.parent is None, code
        print("2nd pass added %d + %d + %d parents." % (addparent1, addparent2, addparent3))

        self.i10gem = defaultdict(list)
        for icd10, icd9, flags in load_txt('meta/2018_I10gem.txt'):
            it = self.icd9.get(icd9, None)
            if it is None:
                continue
            self.i10gem[icd10].append(icd9)
            pass

        patch_shorten(self.i10gem)
        merge_dict_items(self.i10gem)

        if False:   # do not use embedding
            print('%d ICD 10 to 9 mappings loaded.' % len(self.i10gem))
            unused = 0
            used = 0
            for line in load_txt('meta/claims_codes_hs_300.txt'):
                if len(line) != 301:
                    assert line[0] == '51327'
                    continue
                code = line[0]
                if code[0] != 'I':
                    continue
                embedding = [float(x) for x in line[1:]]
                icd9 = code[4:].replace('.', '')
                node = self.icd9.get(icd9, None)
                if node is None:
                    #print('unused', icd9)
                    unused += 1
                    continue
                node.embedding = embedding
                used += 1
                pass
            print('%d embeddings unused.' % unused)
            print('%d embeddings used.' % used)

        order = []
        for k, v in self.icd9.items():
            v.name = k
            if v.parent is None:
                v.id = len(order)
                order.append(v)
                pass
        assert len(order) == 1
        for k, v in self.icd9.items():
            if not v.parent is None:
                v.id = len(order)
                order.append(v)
                pass
        for node in order:
            if node.parent is None:
                node.parent_id = -1
            else:
                node.parent_id = node.parent.id
                pass
            pass
        self.order = order
        pass

    def lookup (self, code, version = 9):
        exact = True
        code9 = []
        if version == 9 or code[0].isnumeric():
            code9 = [code]
        elif version == 10 or version == 0:
            code9, sh = lookup_with_shorten(self.i10gem, code)
            if not code9 is None:
                if (sh != code) or len(code9) > 1:
                    exact = False
                pass
            pass
        if version == 0 and code9 is None:
            code9 = [code]
            pass
        if code9 is None:
            code9 = []
        nodes = []
        for c in code9:
            node = self.icd9.get(c, None)
            if not node is None:
                nodes.append(node)
        return nodes, exact
    pass

if __name__ == '__main__':
    #from cms import Tree
    icd9 = ICD9()
    #tree = Tree(order)
    #tree.try_selection()
    pass
