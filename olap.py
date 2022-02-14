import os
import traceback
import pickle
from glob import glob
import base64
import zlib
from tqdm import tqdm

class Den:
    def __init__ (self, den):
        self.STATE_CD = den.STATE_CD
        self.CNTY_CD = den.CNTY_CD
        self.SEX = den.SEX
        self.RACE = den.RACE
        self.AGE = den.AGE
        self.DEATH_DT = den.DEATH_DT
        self.RFRNC_YR = den.RFRNC_YR
        self.MS_CD = den.MS_CD
        self.A_MO_CNT = den.A_MO_CNT
        self.B_MO_CNT = den.B_MO_CNT
        self.HMO_MO = den.HMO_MO
        pass

class ClaimCD:
    def __init__ (self, claim):
        self.THRU_DT = claim.THRU_DT
        self.PMT_AMT = claim.PMT_AMT
        self.dgns = claim.dgns
        pass
    pass

class ClaimHH(ClaimCD):
    def __init__ (self, claim):
        super().__init__(claim)
        self.dgns_e = claim.dgns_e
        pass
    pass

class ClaimIOS(ClaimHH):
    def __init__ (self, claim):
        super().__init__(claim)
        self.prcdr = claim.prcdr
        pass
    pass

class ClaimIS(ClaimHH):
    def __init__ (self, claim):
        super().__init__(claim)
        self.ADMSN_DT = claim.ADMSN_DT
        self.prcdr = claim.prcdr
        pass
    pass

class Case:
    def __init__ (self, case):
        self.pid = case.pid
        self.den = [Den(row) for row in case.den]
        self.car = [ClaimCD(row) for row in case.car]
        self.dme = [ClaimCD(row) for row in case.dme]
        self.hha = [ClaimHH(row) for row in case.hha]
        self.hosp = [ClaimHH(row) for row in case.hosp]
        self.out = [ClaimIOS(row) for row in case.out]
        self.inp = [ClaimIS(row) for row in case.inp]
        self.snf = [ClaimIS(row) for row in case.snf]
        pass
    pass


def encode (case):
    return base64.b64encode(zlib.compress(pickle.dumps(case)))

def decode (case):
    return pickle.loads(zlib.decompress(base64.b64decode(case)))


def generate_lines (files):
    for f1 in files:
        with open(f1, 'r') as f:
            for l in f:
                yield l


def perf (tests='cms_merged/part-00000'):
    from cms import CaseLoader
    loader = CaseLoader()
    gen = generate_lines(glob(tests))
    r = []
    l1 = []
    l2 = []
    for _ in tqdm(range(1000)):
        l = next(gen)
        k, v = l.strip().split('\t')
        case = loader.load(v)
        v2 = encode(Case(case))
        #r.append(len(v2)/len(v))
        l1.append(len(l))
        l2.append(len(v2))
        pass
    print(sum(l2)/sum(l1))
    pass

