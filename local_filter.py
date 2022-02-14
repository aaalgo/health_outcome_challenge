#!/usr/bin/env python3
import os
import sys
from glob import glob
from tqdm import tqdm
import click
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'build/lib.linux-x86_64-' + sys.version[:3])))
import cms_core

@click.command(context_settings=dict(ignore_unknown_options=True,))
@click.option('--input', default=None)
@click.option('--output', default=None)
@click.option('--filter', default=None)
def run (input, output, filter):
    if not os.path.exists(output):
        os.makedirs(output, exist_ok=True)
        pass
    with open(filter, 'r') as f:
        fl = []
        with open(filter, 'r') as f:
            for l in f:
                pid = int(l.strip().split('\t')[0])
                fl.append(pid)
                pass
            pass
        pass
    print(len(fl), 'pids loaded.')
    filefilter = cms_core.FileFilter(fl)
    for path in tqdm(glob(os.path.join(input, '*'))):
        bname = os.path.basename(path)
        output_path = os.path.join(output, bname)
        filefilter.filter(path, output_path)
        pass
    pass

if __name__ == '__main__':
    run()
    pass
