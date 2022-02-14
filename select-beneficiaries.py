#!/usr/bin/env python3
import os
import sys
import logging
import pickle
from tqdm import tqdm
from glob import glob
from cms import *

# lexical_date
# CMS represents date with integer values like 20201225.
# The only legitimate overation upon such date values is comparison.
# One should not do addition or subtraction on such values.

# We use the following functions to add 30 days to forecast date,
# and then the returned value can be compared to various dates to
# detemine labels.

def add_days (dt, delta=29):
    day = dt % 100 
    dt = dt // 100 
    month = dt % 100 
    year = dt // 100 
    dt = datetime(year, month, day) + timedelta(days=delta)
    return (dt.year * 100 + dt.month) * 100 + dt.day

class Patient:
    def __init__ (self):
        self.pid = None
        self.first_dt = 99991231    # a very big date
                                    # will take min
        self.last_dt = 10000101     # a very small date
                                    # will take max
        self.death_dt = None
        self.ranges = []
        pass

    def select_and_label (self, fdate1, fdate30):
        # selection
        selected = True
        label = False
        revised = fdate1

        for admsn, dschrg in self.ranges:
            if admsn >= fdate1 and admsn <= fdate30:
                # if admission happens on forecast date
                # such admission is considered known and
                # not unplanned, so admsn >= fdate1
                label = True
                pass
            pass

        if not self.death_dt is None:
            # if patient is dead by forecast date,
            # then this patient should not selected for prediction
            if self.death_dt < fdate1:
                selected = False
                pass
            pass

        if not label:
            if self.last_dt < fdate30:
                # we don't yet have data on fdate30
                # the patient might be admitted between last_dt and fdate30
                selected = False
                pass
            pass

        if self.first_dt >= fdate1:
            selected = False
            pass

        if not selected:
            label = None
            revised = None
            pass

        return selected, label, revised

    pass

def merge_overlap_ranges (ranges):
    if len(ranges) == 0:
        return []
    # merge overlap ranges and returns the same format
    # ranges = [(b, e)]
    ranges.sort()
    aggr_ranges = []
    bx, ex = ranges[0]
    for b, e in ranges:
        if b > ex:
            aggr_ranges.append((bx, ex))
            bx, ex = b, e
        else:
            ex = max(ex, e)
            pass
        pass
    aggr_ranges.append((bx, ex))
    return aggr_ranges


def extract_field (i, textual_fields):
    f = textual_fields[i]
    if len(f) == 0:
        return None
    return int(f)

class RowParser:
    # parse an input row

    def __init__ (self, cols, is_den):
        self.is_den = is_den
        self.cols = cols
        cols_lookup = {}
        for i, col in enumerate(cols):
            cols_lookup[col.long_name] = i
            pass
        self.DESY_SORT_KEY = cols_lookup['DESY_SORT_KEY']
        self.maxsplit = self.DESY_SORT_KEY + 1
        if is_den:
            self.DATE_OF_DEATH = cols_lookup['DATE_OF_DEATH']
            self.REFERENCE_YEAR = cols_lookup['REFERENCE_YEAR']
            self.maxsplit = max(self.DESY_SORT_KEY,
                                self.DATE_OF_DEATH,
                                self.REFERENCE_YEAR) + 1
        else:
            self.CLM_THRU_DT = cols_lookup['CLM_THRU_DT']
            self.NCH_BENE_DSCHRG_DT = cols_lookup['NCH_BENE_DSCHRG_DT']
            self.CLM_ADMSN_DT = cols_lookup['CLM_ADMSN_DT']
            self.maxsplit = max(self.DESY_SORT_KEY,
                                 self.CLM_THRU_DT,
                                 self.NCH_BENE_DSCHRG_DT,
                                 self.CLM_ADMSN_DT) + 1
            pass
        pass

    def update_den (self, patient, fs):
        ref_year = extract_field(self.REFERENCE_YEAR, fs) + 2000;
        first_dt = ref_year * 10000 + 101
        last_dt = ref_year * 10000 + 1231
        death_dt = extract_field(self.DATE_OF_DEATH, fs)

        if patient.death_dt is None:
            patient.death_dt = death_dt
            pass

        if first_dt < patient.first_dt:
            patient.first_dt = first_dt
            pass

        if last_dt > patient.last_dt:
            patient.last_dt = last_dt
            if not death_dt is None:    # update death_dt when present
                patient.death_dt = death_dt
                pass
            pass
        pass

    def update_claim (self, patient, fs):
        admsn = extract_field(self.CLM_ADMSN_DT, fs)
        dschrg = extract_field(self.NCH_BENE_DSCHRG_DT, fs)
        if dschrg is None:
            dschrg = extract_field(self.CLM_THRU_DT, fs)
            pass
        patient.ranges.append((admsn, dschrg))
        pass

    def update (self, textual_row, patients):
        fs = textual_row.split(',', self.maxsplit)
        pid = extract_field(self.DESY_SORT_KEY, fs)
        patient = patients[pid]
        patient.pid = pid
        if self.is_den:
            self.update_den(patient, fs)
        else:
            self.update_claim(patient, fs)
        pass
    pass

def find_format (year1, ctype1, sub1):
    for tid, year, ctype, sub, key, cols, use in loader.loader.formats:
        if year == year1 and ctype == ctype1 and sub == sub1:
            return RowParser(cols, ctype1 == 'den')
    logging.error("Format for %d %s %s not found." % (year1, ctype1, sub1))
    assert False
    pass

def keep (ctype, sub):
    # return keep, is_snf_or_inp
    if ctype == 'den':
        return True
    if not ctype in ['inp', 'snf']:
        return False
    if not 'claims' in sub:
        return False
    return True

def scan_for_input_files (input_path):
    # find all relevant input CSV files
    # We only look for inp_claims* and snf_claims*
    #
    for year_dir in glob(os.path.join(input_path, 'AIHOC_Stage2_*_*_v1')):
        dir_bname = os.path.basename(year_dir)
        _, _, split, year, version = dir_bname.split('_')
        is_training = split == 'Training'
        if is_training:
            print("WARNING: dir %s is training data and will be used." % dir_bname)
        year = int(year)
        for csv_file in glob(os.path.join(year_dir, '*.csv')):
            bname = os.path.basename(csv_file)
            ctype, sub, _, _, year_csv = bname.split('_')
            assert year_csv == '%d.csv' % year, 'year in file name %s does not match that of dir name' % (bname, dir_bname)
            if ctype == 'den':
                assert sub == 'saf'
                sub = None
            #logging.info("loading %s %s %s" % (split, year, bname))
            if not keep(ctype, sub):
                continue
            yield year, ctype, sub, csv_file
        pass
    pass

def process (forecast_dates, input_path):
    # forecast_dates are sorted in ascending order
    logging.info("INPUT PATH: %s" % input_path)
    logging.info("OUTPUT PATH: %s" % output_path)

    forecast_dates = [(d, add_days(d)) for d in forecast_dates]
    for d1, d2 in forecast_dates:
        logging.info("FORECAST DATE: %d -> %d" % (d1, d2))

    # pid -> [(admsn, dschrg)]
    patients = defaultdict(Patient)

    input_files = list(scan_for_input_files(input_path))
    logging.info("loading %d input files." % len(input_files))
    for year, ctype, sub, csv_path in tqdm(input_files):
        parser = find_format(year, ctype, sub)
        with open(csv_path, 'r') as f:
            for l in f:
                parser.update(l, patients)
                pass
            pass
        pass

    patients = list(patients.values())

    if False:
        with open('patients.pkl', 'wb') as f:
            pickle.dump(patients, f)
            pass

    n = len(patients)
    logging.info("processing %d patients." % n)

    selection = []
    total_selected = 0
    total_positive = 0
    for patient in tqdm(patients):
        assert patient.first_dt < patient.last_dt
        #print('xxx', patient.first_dt, patient.last_dt)
        patient.ranges = list(sorted(patient.ranges)) #merge_overlap_ranges(patient.ranges)
        for fdate, fdate30 in forecast_dates:
            # split ranges
            selected, label, revised = patient.select_and_label(fdate, fdate30)
            if selected:
                total_selected += 1
                total_positive += int(label)
                pass
            selection.append((patient.pid, fdate, selected, label, revised))
            pass
        pass
    logging.info("total lines %d" % len(selection))
    logging.info("total selected %d (%g)" % (total_selected, total_selected/len(selection)))
    logging.info("total positive %d (%g)" % (total_positive, total_positive/total_selected))
    return selection


HELP="""Usage:
    python3 select-beneficiaries.py [forecast-dates [input-path] [output-path]] [-h]

Description:
    Generate beneficiary selection file.
    
    forecast-dates: forecast dates in YYYYMMDD format; if multiple dates are given they should be separated by commas. E.g. 20130601,20140630,20151231.
    input-path: path to search for LDS data files. Current directory will be used if ommited.
    output-path: path to output the selected beneficiaries file.  Current directory will be used if ommited.
    -h: display help information.
"""

def parse_cmdline ():
    if (len(sys.argv) < 2) or ('-h' in sys.argv) or len(sys.argv) > 4:
        print(HELP)
        sys.exit(0)
        pass
    cwd = os.getcwd()
    forecast_dates = [int(x) for x in sys.argv[1].split(',')]
    forecast_dates.sort()
    input_path = cwd
    output_path = cwd

    if len(sys.argv) > 2:
        input_path = sys.argv[2]
        pass

    if len(sys.argv) > 3:
        output_path = sys.argv[3]
        pass
    return forecast_dates, input_path, output_path

if __name__ == '__main__':

    forecast_dates, input_path, output_path = parse_cmdline()

    selection = process(forecast_dates, input_path)

    logging.info("writing %d lines to to output %s." % (len(selection), output_path))
    os.makedirs(output_path, exist_ok=True)
    with open(os.path.join(output_path, '%s-beneficiaries.csv' % PARTICIPANT_NAME), 'w') as f1, \
        open(os.path.join(output_path, '%s-target.csv' % PARTICIPANT_NAME), 'w') as f2:
        for pid, fdate, sel, label, revised in selection:
            f1.write('%d,%d,%d\n' % (pid, fdate, sel))
            if sel:
                f2.write('%d,%d,%d,%d\n' % (pid, fdate, label, revised))
            pass
    pass

