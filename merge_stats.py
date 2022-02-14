#!/usr/bin/env python3
import pickle
import cms

STATE_CODE = {
1: "Alabama",
2: "Alaska",
3: "Arizona",
4: "Arkansas",
5: "California",
6: "Colorado",
7: "Connecticut",
8: "Delaware",
9: "District of Columbia",
10: "Florida",
11: "Georgia",
12: "Hawaii",
13: "Idaho",
14: "Illinois",
15: "Indiana",
16: "Iowa",
17: "Kansas",
18: "Kentucky",
19: "Louisiana",
20: "Maine",
21: "Maryland",
22: "Massachusetts",
23: "Michigan",
24: "Minnesota",
25: "Mississippi",
26: "Missouri",
27: "Montana",
28: "Nebraska",
29: "Nevada",
30: "New Hampshire",
31: "New Jersey",
32: "New Mexico",
33: "New York",
34: "North Carolina",
35: "North Dakota",
36: "Ohio",
37: "Oklahoma",
38: "Oregon",
39: "Pennsylvania",
40: "Puerto Rico",
41: "Rhode Island",
42: "South Carolina",
43: "South Dakota",
44: "Tennessee",
45: "Texas",
46: "Utah",
47: "Vermont",
48: "Virgin Islands",
49: "Virginia",
50: "Washington",
51: "West Virginia",
52: "Wisconsin",
53: "Wyoming",
}

lookup = {}

with open("stat_2009.txt", "r") as f:
    for l in f:
        k, v = l.strip().split('\t')
        k = k.strip('"')
        lookup[k] = int(round(float(v)))
        pass
    pass

for ctype in cms.CLAIM_TYPES:
    if ctype == 'car' or ctype == 'dme':
        continue
    total = 0
    for year in ['2009', '2010', '2011']:
        v = lookup['E_%s_%s' % (year, ctype)]
        total += int(v)
        pass
    lookup['E_%s' % ctype] = total
    if ctype == 'inp':
        total = 0
        for year in ['2009', '2010', '2011']:
            v = lookup['POA_%s_%s' % (year, ctype)]
            total += int(v)
            pass
        lookup['POA_%s' % ctype] = total
    pass



stats = {}

def fill_year_counts (year, key):
    counts = {
            'amount': lookup['pmt%s' % key],
            'claims': lookup['claim%s' % key],
            'patients': lookup['den%s' % key],
            'adverse_dgns_e': lookup['E%s' % key],
            'adverse_poa': lookup['POA%s' % key],
    }
    if not year in stats:
        stats[year] = {}
    stats[year]['counts'] = counts
    pass

def fill_year_state_counts (year, key):
    data = []
    for k, v in STATE_CODE.items():
        data.append({'name': v,
                     'value': lookup['claim%s_state_%d' % (key, k)]})
        pass
    stats[year]['counts']['claims_by_state'] = data
    pass

def fill_year_age_counts (year, key):
    data = []
    pat = 'den%s_age_' % key
    for k, v in lookup.items():
        if pat in k:
            age = int(k[len(pat):])
            count = int(v)
            data.append((age, int(v))) #{'name': age, 'value': int(v)})
        pass
    data.sort(key = lambda x: x[0])
    stats[year]['counts']['age'] = {
        'keys': [v[0] for v in data],
        'values': [v[1] for v in data]
        }
    pass

def fill_ctype_counts (year, key):
    data = []
    ad_data = []
    for ctype in cms.CLAIM_TYPES:
        if ctype == 'car' or ctype == 'dme':
            continue
        data.append({'name': ctype,
                     'value': lookup['%s%s' % (ctype, key)]})
        ad_data.append({'name': ctype,
                     'value': lookup['E%s_%s' % (key, ctype)]})
        pass
    ad_data.append({'name': 'inp_POA',
                 'value': lookup['POA%s_inp' % key]})
    stats[year]['counts']['claims_by_ctype'] = data
    stats[year]['counts']['ads_by_ctype'] = ad_data
    pass

'''
def fill_ctype_counts (year, key):
    data = []
    for ctype in cms.CLAIM_TYPES:
        data.append({'name': v,
                     'value': lookup['%s%s' % (ctype, key)]})
        pass
    stats[year]['counts']['claims_by_ctype'] = data
    pass
    '''

def fill_year (year, key):
    fill_year_counts(year, key)
    fill_year_state_counts(year, key)
    fill_year_age_counts(year, key)
    fill_ctype_counts(year, key)
    pass

fill_year('2011', '_2011')
fill_year('2010', '_2010')
fill_year('2009', '_2009')
fill_year('2009-2011', '')

print(stats)

with open('stats.pkl', 'wb') as f:
    pickle.dump(stats, f)

