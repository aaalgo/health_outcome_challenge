import sys
from datetime import datetime
from datetime import timedelta
# Model 1.1: unexpected hosp. within 30 days for inp
# Model 1.2: unexpected hosp. within 30 days for any 
# Model 1.3: 
# Model 1.4: 
# Model 2.1: SNF
# Model 2.2: SNF
# Model 2.3:
# Model 2.4:
# Model 3.1:
# Model 3.2

def has_unplanned (rows):
    rows.sort(key=lambda x: x.THRU_DT)
    for i in range(1, len(rows)):
        pre = rows[i-1]
        cur = rows[i]
        a=str(pre.THRU_DT)
        if pre.THRU_DT < 20090000:
            pass
        else:
            if load_dt(cur.ADMSN_DT) - load_dt(pre.THRU_DT) < TH:
                return True
        pass
    return False

####
####adverse=['E878','E879',
####        # Surgical and medical procedures as the cause of abnormal reaction of patient or later complication
####         'E930','E931','E932','E933','E934','E935','E936','E937','E938','E939','E940','E941','E942','E943','E944','E945','E946','E947','E948','E949'     # Drugs, medicinal and biological substances causing adverse effects in therapeutic use are directly referring to an adverse event of a drug or treatment.
####        ]
####
####def has_dgns (rows):
####    rows.sort(key=lambda x: x.THRU_DT)
####    for row in rows:
####        for dgn in row.dgns:   
####            for aaa in adverse:
####                if aaa in dgn.ICD_DGNS_CD:
####                    return True
####            #if 'E' in dgn.ICD_DGNS_IND_SW:
####            #    return True
####    return False
####
####def load_dt (dt):
####    day = dt % 100
####    dt = dt // 100
####    month = dt % 100
####    year = dt // 100
####    try:
####        return datetime(year, month, day)
####    except:
####        print(dt)
####        raise
####    pass
####
####TH = timedelta(days=30)
####
####
####
##### LABEL
##### (DGNS_E, POA, 30DAY)
#####
####
####def row_has_dgns_e (row):
####    for dgn in row.dgns:
####        for a in adverse:
####            if a in dgn.ICD_DGNS_CD:
####                #print("DGNS_E", a, dgn.ICD_DGNS_CD)
####                return True
####    for dgn in getattr(row, "dgns_e", []):
####        for a in adverse:
####            if a in dgn.ICD_DGNS_E_CD:
####                #print("DGNS_E", a, dgn.ICD_DGNS_E_CD)
####                return True
####            pass
####        pass
####    return False
####
####def row_has_not_poa (row):
####    for dgn in row.dgns:
####        if dgn.CLM_POA_IND_SW == 'N':
####            return True
####    for dgn in getattr(row, "dgns_e", []):
####        if dgn.CLM_E_POA_IND_SW == 'N':
####            return True
####    return False
####
####
####def label_records (rows, check_poa = True, check_30day = True):
####    labels = [[False, False, False] for row in rows]
####    tups = list(zip(rows, labels))
####
####    for row, label in tups:
####        if row_has_dgns_e(row):
####            label[0] = True
####            pass
####
####    if check_poa:
####        for row, label in tups:
####            if row_has_not_poa(row):
####                label[1] = True
####        pass
####
####    if check_30day:
####        tups.sort(key=lambda x: x[0].THRU_DT)
####        for i in range(1, len(tups)):
####            pre = tups[i-1][0]
####            cur = tups[i][0]
####            label = tups[i][1]
####            if pre.THRU_DT < 20090000:
####                pass
####            else:
####                gap = load_dt(cur.ADMSN_DT) - load_dt(pre.THRU_DT)
####                if gap < TH:
####                    print('xxx', pre.DSYSRTKY, pre.THRU_DT, cur.ADMSN_DT, gap)
####                    label[2] = True
####    return labels
####
