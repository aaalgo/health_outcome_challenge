#!/usr/bin/env python3
import sys
import os
import pickle
import numpy as np
import click
sys.path.append(os.path.abspath('/home/wdong/shared/cms2'))
from process_split import FeatureCache, auc
from cms import logging

def run (model, gs, output, fast):
    # 命令行运行 .../process_split.py --split 3 处理split 3
    # 或者 .../process_split.py 处理所有的splits

    assert ("orig" in model) == (not fast)
    model = pickle.load(open(model, 'rb'))

    logging.info("populating cache.")
    cache = FeatureCache([gs], None, fast)

    logging.info("predicting")
    gs, dat = cache.load(gs)
    value=model.predict(dat)
    np.savetxt(output,value)

    print("AUC:", auc(gs, value))
    pass


@click.command(context_settings=dict(ignore_unknown_options=True,))
@click.option('--model', default=None)
@click.option('--gs', default=None)
@click.option('--output', default=None)
def run_cmdline (model, gs, output):
    run(model, gs, output, True)

if __name__ == '__main__':
    run_cmdline()

