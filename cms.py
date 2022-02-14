import os
import sys
from collections import namedtuple, defaultdict
import json
import pickle
from glob import glob
from datetime import datetime, timedelta
import subprocess as sp
import logging
import traceback

PARTICIPANT_NAME = 'aaalgo'

CMS_HOME = os.path.abspath(os.path.dirname(__file__))

CORE_LIB_PATHS = [os.path.join(CMS_HOME, 'build/lib.linux-x86_64-' + sys.version[:3])]

HAS_CORE = False
for CORE_LIB_PATH in CORE_LIB_PATHS:
    if os.path.exists(CORE_LIB_PATH):
        sys.path.append(CORE_LIB_PATH)
        HAS_CORE = True
        break

if HAS_CORE:
    from cms_core import *
    #os.nice(20)
    pass

logging.basicConfig(level=logging.INFO,
    format='%(levelname)s %(asctime)s %(message)s')

# META_VERSION = '0ceb39a6c4254ac7c9a0a79ed517621477a5389b' # meta 1
META_VERSION = '3081ff0822843629b56b4884032ec872b68d8834'   # meta 2

ROOT = os.path.abspath(os.path.dirname(__file__))
RAW_DATA_ROOT = os.environ.get("CMS_RAW_DATA_ROOT", "/shared/data/CMS/AI_Challenge_Stage2/data")

def string_to_day (text):
    return int(text)

def load_dt (dt):
    day = dt % 100 
    dt = dt // 100 
    month = dt % 100 
    year = dt // 100 
    return datetime(year, month, day)

def register_namedtuple (name, fields):
    cls = namedtuple(name, fields)
    setattr(sys.modules[__name__], name, cls)
    return cls


DATA_ROOT = 'data'

class LineFormat:
    def __init__ (self, line):
        fs = line.strip().split(' ')
        self.index = []
        off = 0
        for f in fs:
            l = len(f)
            self.index.append((off, l))
            off += l + 1
            pass
        fs = ''.join(self.split(line))
        for c in fs:
            assert c == '-'
        pass

    def split (self, line):
        fs = []
        for off, l in self.index:
            fs.append(line[off:(off + l)].strip())
            pass
        return fs

    def check_fts_line (self, line):
        min_length = self.index[-2][0]
        if len(line) < min_length:
            return False
        return True
    pass

Field = register_namedtuple('Field', ['no', 'long_name', 'short_name', 'type', 'line']) 

def load_columns (fts):
    # 返回fts文件中列出的column names
    # [field1, field2, ...]
    # 每一项是一个Field
    with open(fts, 'r') as f:
        index = None
        while True:
            l = next(f)
            if l[:2] == '--':
                index = LineFormat(l)
                break
            pass
        last = 0
        fields = []

        min_line_length = index.index[-1][0]    # offset of last field
        while True:
            try: 
                l = next(f)
            except StopIteration:
                break
            if not index.check_fts_line (l):
                strip = l.strip()
                if len(strip) > 0 and strip != '-- End of Document --' and strip != '- End of Document' and strip != 'Note: All DATE fields are written out in CCYYMMDD format.':
                    print("SKIP %s: %s" % (fts, strip))
                continue
            fs = index.split(l)
            try:
                # infer column type
                ctype = None
                c3 = fs[3]
                if c3[0] == '$' or c3 == 'CHAR' or fs[1] == 'DESY_SORT_KEY':
                    ctype = TYPE_STR
                elif c3.isnumeric() or c3 == 'NUM':
                    # is data
                    ctype = TYPE_INT
                    c4 = fs[4].strip()
                    if len(c4) == 0:
                        pass
                    elif c4 == 'YYMMDDN8.':
                        pass
                    elif float(c4) != int(float(c4)):
                        ctype= TYPE_FLOAT
                        pass
                    pass
                elif c3 == 'DATE':
                    # 老数据里的_DT列都存成int
                    ctype = TYPE_STR_TO_DAY
                else:
                    assert False
                    pass

                fs = fs[:3]
                fs.append(ctype)
                fs.append(l.strip())
                fs[0] = int(fs[0])
                num = fs[0]
                if num != last + 1:
                    assert False
            except:
                traceback.print_exc(1000, sys.stderr)
                sys.stderr.write('%s: %s\n' % (fts, l))
                sys.stderr.write(str(index.split(l)))
                break
            # fs[0]: colum number
            # fs[1]: long name
            # fs[2]: short name
            # fs[3]: type
            # fs[4]: the full line describing the column
            field = Field(*fs)
            fields.append(field)
            last = num
            pass
        pass
    return fields

def load_columns_bname (bname):
    fts_bname = bname
    if not bname[-1].isnumeric():   # 2010 car
        fts_bname = bname[:-1]
        pass
    return load_columns('data/%s.fts' % fts_bname)


CLAIM_TYPES = ['car', 'dme', 'hha', 'hosp', 'inp', 'out', 'snf']

def deep_glob (pattern):
    o = sp.check_output("find %s/ -name '%s'" % (DATA_ROOT, pattern), shell=True).decode('ascii')
    return list(filter(lambda x: x != '', o.strip().split('\n')))

class Records:
    def __init__ (self, cols):
        col_lookup = {}
        for i, col in enumerate(cols):
            col_lookup[col.long_name] = i
            pass
        self.cols = cols
        self.col_lookup = col_lookup
        self.claim_no_col = col_lookup.get('CLAIM_NO', None)
        pass

    def get (self, name, row):
        #print(name)
        #print(self.col_lookup[name])
        #print(len(row))
        return row[self.col_lookup[name]]
    pass

class SimpleRecords(Records):
    def __init__ (self, cols):
        super().__init__(cols)
        self.rows = []
        pass

    def add (self, row):
        self.rows.append(row)
        pass
    pass

class GroupedRecords(Records):
    def __init__ (self, cols):
        super().__init__(cols)
        self.rows = defaultdict(list)
        assert not self.claim_no_col is None
        pass

    def add (self, row):
        key = row[self.claim_no_col]
        self.rows[key].append(row)
        pass
    pass

DEFAULT_META_JSON = os.path.join(ROOT, 'meta2.json')

TYPE_STR = 1
TYPE_INT = 2
TYPE_FLOAT = 3
TYPE_STR_TO_DAY = 4

def assert_false (text):
    assert False
    return None

TYPE_CTORS = [assert_false, str, int, float, string_to_day]

class RawCaseLoader:

    def __init__ (self, meta_path=DEFAULT_META_JSON, include=None):
        with open(meta_path, 'r') as f:
            version, formats, lookup = json.load(f)
            assert version == META_VERSION, "meta.pkl is incompatible, please report"
            pass
        
        table_lookup = {}
        self.formats = []   # add this format to table
        self.table_ctors = []
        names = []
        self.names = names
        for ind, cols in formats:
            year, ctype, sub, key = ind
            name = None
            cols_new = [Field(*col) for col in cols]
            if ctype == 'den':
                name = 'den'
            elif sub is None:
                name = '%s' % ctype
            else:
                name = '%s_%s' % (ctype, sub)
                pass
            if name in table_lookup:
                tid = table_lookup[name]
            else:
                tid = len(self.table_ctors)
                table_lookup[name] = tid
                names.append(name)
                if sub is None or sub == 'claimsj' or sub == 'claimsk':
                    self.table_ctors.append((SimpleRecords, cols_new))
                else:
                    self.table_ctors.append((GroupedRecords, cols_new))
                pass
            use = True
            if not include is None:
                use = False
                for x in include:
                    if x in name:
                        use = True
                        break
            self.formats.append((tid, year, ctype, sub, key, cols_new, use))
            pass
        self.raw_case_ctor = register_namedtuple('RawCase', ['pid'] + names)
        pass

    def load (self, row):
        #print(row)
        pid = None
        tables = [m(c) for m, c in self.table_ctors]
        for one in row.split('|'):
            raw_fs = one.split(',')
            fid = int(raw_fs[0])
            raw_fs = raw_fs[1:]
            tid, year, ctype, sub, key, cols, use = self.formats[fid]
            if not use:
                continue
            assert len(cols) <= len(raw_fs), 'not %d <= %d: %d-%s-%s %s' % (len(cols), len(raw_fs), year, ctype, sub, one)

            fs = []
            for col, f in zip(cols, raw_fs):
                if len(f) == 0:
                    fs.append(None)
                else:
                    try:
                        fs.append(TYPE_CTORS[col.type](f))
                    except:
                        if (col.type is int) and (not '.' in f):
                            fs.append(int(float(f)))
                        else:
                            raise
                    pass
                pass
            if pid is None:
                pid = fs[key]
            else:
                assert pid == fs[key]
                pass
            tables[tid].add(fs)
            pass
        return self.raw_case_ctor(int(pid), *tables)

# RawCase format:
#   case.{pid, den, car, ... car_claimsj, ...}

# 需要把RawCase经过正规化，变成格式同一的Case
#
# case.{pid, den, car, dme, ...}
#

# 需要做的
#   1. 前后统一命名标准
#   2. 对于新格式中已经独立出来的表格的字段，从对应的表格中载入
#      对于老格式中,则需要从对应字段中提取
#   3. 剩下有没有独立的，都要从对应字段中提取
#   4. 表格排序，子表格排序

#   spec:   name, names

DEFAULT_MAPPING_JSON = os.path.join(ROOT, 'mapping2.json')

class CaseNormalizer:
    """Case normalizer.

    This class loads raw case data, which might contain multiple versions of
    data of the same claim type, into a unified format.  The output normalized
    case contains the follow fields:
            pid
            den
            car
            dme
            hha
            hosp
            inp
            out
            snf
    
    This loader operates upon a pre-defined ETL spec, which specifies for each
    ctype, which raw tables must be scanned, which output fields should be
    extracted and which input fields from the raw data should be used for these
    output fields.

    self.specs: [ctype, [input_table_spec], [output_field_spec], ctor]

    Each ctype has a corresponding row in self.specs, in unspecified order.
    Each input_table_spec specified must be scanned to generate output for this
    ctype.

    input_table_spec = [input_table_name]:
                lists all raw/input tables must be scanned.

        For example, for ctype dme, there are two versions: dme and dme_claimsj.

    output_field_spec = [field_name, subfields, [input_spec_v1, input_spec_v2, ...], ctor]

        Each entry defines an output field.  There are two kinds of fields:
            - Atomic field: field_name must be all capitalized, like CLAIM_NO.
              subfields is None.
            - Table field: the field is a sub-table, in such case name is in lower
              case,  like "lines".  Subfields lists the fields of the sub-table.

        The input_specs then specify how from each version these fields
        are to be loaded.  Version specs are ordered and can be indexed by vid.

    input_spec_v? = [input_field_name, input_subfield_names]

            input_field_name    input_subfield_names   comments
            ----------------    --------------------   --------
            str                 None                   Atomic field, get input_field_name from input_table
            str                 list of str            Sub-table. Input field name specifies the raw table
                                                       to load these fields from and input_field_names
                                                       specified the raw fields to load (new version).
            None                list of str            Sub-table. Load these columns from the raw table
                                                       directly (old version).
            None                None                   This version does not contain data. 
    """

    def __init__ (self, mapping_path = DEFAULT_MAPPING_JSON):
        with open(mapping_path, 'r') as f:
            self.specs = json.load(f)
        self.case_ctor = register_namedtuple('Case', ['pid'] + [spec[0] for spec in self.specs])
        for spec in self.specs:
            ctype, _, output_field_specs = spec
            spec.append(register_namedtuple('Rec_%s' % ctype, [s[0] for s in output_field_specs]))

            for field in output_field_specs:
                field_name, sub_field_names, _, _ = field
                if sub_field_names is None:
                    field.append(None)
                else:
                    field.append(register_namedtuple('Field_%s_%s' % (ctype, field_name), sub_field_names))
                    pass
                pass
            pass
        pass

    def load_column_groups (self, table, row, input_field_names, ctor):
        rows = []

        for i in range(1, 1000):
            fs = []
            good = False
            for fname in input_field_names:
                try:
                    v = table.get("%s%d" % (fname, i), row)
                    if not v is None:
                        good = True
                    fs.append(v)
                except:
                    break
                pass
            if len(fs) == 0:
                break
            assert len(fs) == len(input_field_names)
            if good:
                rows.append(ctor(*fs))
            pass
        return rows

    def load_sub_table (self, table, claim_no, input_field_names, ctor):
        rows = []
        assert isinstance(table, GroupedRecords)
        for row in table.rows.get(claim_no, []):
            fs = []
            for fname in input_field_names:
                fs.append(table.get(fname, row))
                pass
            rows.append(ctor(*fs))
            pass
        return rows

    def apply (self, raw):
        tup = [raw.pid]
        for _, input_table_specs, output_field_specs, ctor in self.specs:
            recs = []
            # vid: version_id
            for vid, input_table_name in enumerate(input_table_specs):
                input_table = raw.__getattribute__(input_table_name)
                assert isinstance(input_table, SimpleRecords)
                for row in input_table.rows:
                    fs = []
                    # process each output field
                    for _, subfields, input_field_spec_by_version, _, field_ctor in output_field_specs:
                        input_field_name, input_subfield_names = input_field_spec_by_version[vid]
                        if field_ctor is None:
                            # atomic output field
                            assert not input_field_name is None
                            assert input_subfield_names is None
                            fs.append(input_table.get(input_field_name, row))
                        else:
                            if input_field_name is None:
                                if input_subfield_names is None:
                                    fs.append([])
                                else:
                                    # load from input table directly
                                    fs.append(self.load_column_groups(input_table, row, input_subfield_names, field_ctor))
                            else:
                                # new version
                                assert type(input_subfield_names) is list
                                claim_no = input_table.get("CLAIM_NO", row)
                                fs.append(self.load_sub_table(raw.__getattribute__(input_field_name), claim_no, input_subfield_names, field_ctor))
                            pass
                        pass
                    recs.append(ctor(*fs))
                    pass
                pass
            tup.append(recs)
        return self.case_ctor(*tup)
    pass

DEFAULT_ICD9_CODEBOOK_PKL = os.path.join(ROOT, 'icd9_codebook.pkl')

class CaseLoader:
    def __init__ (self):
        self.loader = RawCaseLoader()
        self.normer = CaseNormalizer()
        if os.path.exists(DEFAULT_ICD9_CODEBOOK_PKL):
            with open(DEFAULT_ICD9_CODEBOOK_PKL, 'rb') as f:
                self.icd9 = pickle.load(f)
        pass

    def load (self, v):
        case = self.loader.load(v)
        return self.normer.apply(case)
    pass

loader = None
if os.path.exists(DEFAULT_META_JSON) and os.path.exists(DEFAULT_MAPPING_JSON):
    loader = CaseLoader()

DEFAULT_EXTRACTOR_PKL = os.path.join(ROOT, 'extractor.pkl')

MAX_DGNS = 25
MAX_PRCDR = 25

SUBTABLE_DUPLICITY = {
    'buyin': 0,
    'hmoind': 0,
    'dgns': MAX_DGNS,
    'linej': 0,
    'dgns_e': MAX_DGNS,
    'prcdr': MAX_PRCDR,
    'revenuej': 0,
    'instval': 0,
    'instoccr': 0,
    'instcond': 0
}

EXTRACTOR_SKIP = {
        'DSYSRTKY': 1,
        'CLAIMNO': 1,
        'PRCDR_DT': 1
}

class Claim:
    def __init__ (self, features, thru_dt, admsn_dt = None):
        self.features = features
        self.thru_dt = thru_dt
        self.admsn_dt = admsn_dt
        pass

# TODO: 2008, 2009 data ignored

class Extractor:
    def __init__ (self, mapping_path = DEFAULT_MAPPING_JSON, extractor_path = DEFAULT_EXTRACTOR_PKL):
        with open(mapping_path, 'r') as f:
            self.specs = json.load(f)
            pass

        with open(extractor_path, 'rb') as f:
            xtors = pickle.load(f)
            pass

        self.case_ctor = register_namedtuple('ExtractedCase', ['pid'] + [spec[0] for spec in self.specs])

        for spec in self.specs:
            ctype, _, fields  = spec
            # fields: [[field, subfields]]
            xtor = xtors[ctype]     # {field: [n, lookup]}
                                    # or {field: {subfield: [n, lookup]}}
            off = 0
            fts = []
            for field in fields:
                field_name, subfields, _, _ = field
                field.clear()
                # field = [field_name, use, offset, is_numeric, exhorstive, lookup, sub_xtor, sub_duplicity]
                #   field_name
                #   use: use this field
                #   offset: 
                #   is_numeric:
                #   exhostive:
                #   lookup:
                #   sub_xtor:
                #   sub_duplicity:

                # subfield = [subfield_name, use, snumeric, exhorstive, lookup]
                #
                if subfields is None:
                    if field_name in EXTRACTOR_SKIP:
                        # skip this field while extraction
                        field.extend([field_name, False, None, None, None, None, None, None])
                    elif field_name in xtor:
                        fv = xtor[field_name]
                        assert type(fv) is list     # use, exhorstive, lookup
                        U, E, lookup = fv
                        if not U:
                            field.extend([field_name, False, None, None, None, None, None, None])
                        else:
                            field.extend([field_name, True, off, False, E, lookup, None, None])
                            fts.append((field_name, True))
                            off += 1
                    else:
                        # col is numeric
                        field.extend([field_name, True, off, True, None, None, None, None])
                        fts.append((field_name, False))
                        off += 1
                        pass
                    pass
                else:
                    duplicity = SUBTABLE_DUPLICITY[field_name]
                    sub_xtor = []
                    sub_off = 0
                    sub_fts = []
                    fv = xtor.get(field_name, {})
                    assert type(fv) is dict
                    for subfield in subfields:
                        if subfield in EXTRACTOR_SKIP:
                            sub_xtor.append([subfield, False, None, None, None])
                        elif subfield in fv:
                            sfv = fv[subfield]
                            assert type(sfv) is list
                            U, E, lookup = sfv
                            if not U:
                                # subfield, use, numeric, exhorstive, lookup
                                sub_xtor.append([subfield, False, None, None, None])
                            else:
                                sub_xtor.append([subfield, True, False, E, lookup])
                                sub_fts.append((subfield, True))
                                sub_off += 1
                        else:
                            # col is numeric
                            sub_xtor.append([subfield, True, True, None, None])
                            sub_fts.append((subfield, False))
                            sub_off += 1
                            pass
                        pass
                    field.extend([field_name, True, off, None, None, None, sub_xtor, duplicity])
                    for i in range(duplicity):
                        for sf, is_cat in sub_fts:
                            fts.append(("%s%d" % (sf, i), is_cat))
                            pass
                        pass
                    assert len(sub_xtor) >= sub_off
                    # TODO: 这里应该是sub_off * duplicity
                    off += sub_off * duplicity
                    pass
                pass
            pass
            spec.clear()
            spec.extend([ctype, fields, fts])
        pass

    def apply (self, case):
        xxx = [case.pid]
        for ctype, fields, ft_names in self.specs:
            rows = getattr(case, ctype)
            fts = []
            for row in rows:
                thru_dt = getattr(row, "THRU_DT", None)
                admsn_dt = getattr(row, "ADMSN_DT", None)
                if not thru_dt is None:
                    if thru_dt //10000 < 2010:      # TODO!!!
                        continue
                    thru_dt = load_dt(thru_dt)
                if not admsn_dt is None:
                    admsn_dt = load_dt(admsn_dt)
                ft = []
                try:
                    for col, field in zip(row, fields):
                        field_name, use, off, is_numeric, exhorstive, lookup, sub_xtor, sub_duplicity = field
                        if not use:
                            continue
                        if sub_xtor is None:
                            if is_numeric:
                                if type(col) is str:
                                    col = float(col)
                                ft.append(col)
                            else:   # category
                                ft.append(lookup.get(col, len(lookup)))
                        else:
                            for sub in col[:sub_duplicity]:
                                assert len(sub) == len(sub_xtor)
                                for c, f in zip(sub, sub_xtor):
                                    sf, suse, snumeric, _, slookup = f

                                    if not suse:
                                        continue
                                    if snumeric:
                                        if type(c) is str:
                                            try:
                                                c = float(c)
                                            except:
                                                print(ctype, field_name, sf, suse)
                                                c = np.nan
                                        ft.append(c)
                                    else:
                                        ft.append(slookup.get(c, len(slookup)))
                                        pass
                                    pass
                            for _ in range(len(col), sub_duplicity):
                                for _, suse, _, _, _ in sub_xtor:
                                    if suse:
                                        ft.append(None)
                                pass
                        pass
                    assert len(ft) == len(ft_names)
                    fts.append(Claim(ft, thru_dt, admsn_dt))
                except Exception as e:
                    print(e)
                pass
            if ctype != 'den':
                fts.sort(key = lambda x: x.thru_dt)
            xxx.append(fts)
            pass
        return self.case_ctor(*xxx)
    pass

#class ICD:
#    def __init__ (self):
#        self.lookup = {}
#        with open(os.path.join(ROOT, 'CMS32_DESC_LONG_DX.txt'), 'rb') as f:
#            for l in f:
#                l = l.decode('iso-8859-1')
#                k, v = l.split(' ', 1)
#                v = v.strip()
#                self.lookup[k] = v
#                pass
#            pass
#        pass
#    
#    def explain (self, code):
#        return self.lookup.get(code, 'CODE %s' % code)
#    pass

def load_gs (gs_path):
    # train_gs.dat*
    # test_gs
    with open(gs_path, 'r') as f:
        for l in f:
            death365, pid, observe, cutoff = l.strip().split('\t')[:4]
            yield int(death365), int(pid), int(observe), int(cutoff)
        pass
    pass


def iter_lines (*patterns):
    for pattern in patterns:
        for path in glob(pattern):
            with open(path, 'rb') as f:
                for l in f:
                    yield l
    pass


def get_claim_icd9_nodes (claim, lookup=None):
    standalone = False
    if lookup is None:
        lookup = defaultdict(lambda:[False])
        standalone = True
        pass
    for code, version, source in claim.get_icd_codes():
        nodes, exact = loader.icd9.lookup(code, version)
        for node in nodes:
            item = lookup[node]
            item[0] = item[0] or exact
            pass
        pass
    if standalone:
        return [[k, v[0]] for k, v in lookup.items()]
    pass

def get_case_icd9_nodes (case):
    lookup = defaultdict(lambda:[False])
    for claim in case.claims():
        get_claim_icd9_nodes(claim, lookup)
        pass
    return [[k, v[0]] for k, v in lookup.items()]


