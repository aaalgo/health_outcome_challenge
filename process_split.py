#!/usr/bin/env python3
import sys
import os
import pickle
import lightgbm as lgb
import glob
import random
import numpy as np
from tqdm import tqdm
import subprocess as sp
import multiprocessing
import sklearn.metrics as metrics
import click
sys.path.append(os.path.abspath('/home/wdong/shared/cms2'))
from cms import *
import preprocess

def prep (params):
    key, finaldata = params
    assert not finaldata is None
    assert finaldata.shape[0] != 0
    finaldata1=preprocess.faster_preprocess(finaldata)
    del finaldata
    finaldata = finaldata1
    finaldata=preprocess.faster_feature(finaldata)
    #finaldata=preprocess.feature(finaldata)
    return key, finaldata.astype(np.float32)

class FeatureCache:

    def __init__ (self, gs_paths, data_files=None, fast=True):
        if data_files is None:
            if not os.path.exists('data2') or not os.path.exists('data2/death'):
                print("Do this link on cluster behind barton and try again:")
                print("ln -s /shared/data/CMS/tmp2/stage2 data2")
                sys.exit(0)
                pass
            data_files=list(glob.glob('data2/death/**/part-*', recursive=True)) + \
                       list(glob.glob('data2/25th/**/part-*', recursive=True))

        # 把gs_paths里面用到的所有example都先加载到
        # 字典self.cache里
        # self.cache[pid]就是特征
        need_data=[]
        for path in gs_paths:
            for _, pid, _, cutoff in load_gs(path):
                need_data.append([pid, cutoff])
                pass
            pass

        core_loader = CoreLoader(loader)
        if fast:
            self.cache = core_loader.bulk_load_aggr_features(data_files, need_data)
        else:
            cache = core_loader.bulk_load_features(data_files, need_data)
            logging.info("preprocessing")
            jobs = list(cache.items())
            pool = multiprocessing.Pool()
            self.cache = {}
            for k, v in tqdm(pool.imap_unordered(prep, jobs), total=len(jobs)):
                self.cache[k] = v
                pass
            logging.info("%d items loaded." % len(self.cache))
        pass

    def load (self, gs_path):
        # keep: 如果数据不存在是否返回0/NaN
        gs=[]
        dat=[]
        for death365, pid, observe, cutoff in load_gs(gs_path):
            X = self.cache.get((pid, cutoff), None)
            assert not X is None
            gs.append(death365)
            dat.append(X)
            pass
        gs = np.array(gs)
        dat = np.vstack(dat)
        return gs, dat
    pass

def auc (gs, values):
    return metrics.roc_auc_score(gs, values)

def run (split, nsplits, fast):
    # 命令行运行 .../process_split.py --split 3 处理split 3
    # 或者 .../process_split.py 处理所有的splits

    if split is None:
        splits = list(range(nsplits))
    else:
        splits = [split]
        pass

    logging.info("populating cache.")

    gs_paths = []
    for split in splits:
        gs_paths.append('train_gs.dat.%d' % split)
        pass

    cache = FeatureCache(gs_paths, None, fast)

    for split in splits:

        logging.info("processing split %d" % split)
        gs, dat = cache.load('train_gs.dat.%d' % split) # False表示找不到的样本直接扔掉

        lgb_train = lgb.Dataset(dat, gs)
        params = {
            'boosting_type': 'gbdt',
            'objective': 'regression',
            'num_leaves': 150,
            'learning_rate': 0.05,
            'verbose': 1,
            'n_estimators': 400,
            'reg_alpha': 2.0,
        }

        gbm = lgb.train(params,
            lgb_train,
            num_boost_round=1000
        )

        del dat
        print('Saving model...')
        if fast:
            filename = ('finalized_model.sav.%d' % split)
        else:
            filename = ('finalized_model.sav_orig.%d' % split)
        pickle.dump(gbm, open(filename, 'wb'))
        pass
    logging.info('prediction finished.')
    pass

@click.command(context_settings=dict(ignore_unknown_options=True,))
@click.option('--split', type=int, default=None)
@click.option('--nsplits', type=int, default=5)
def run_cmdline (split, nsplits):
    run(split, nsplits, True)
    pass

if __name__ == '__main__':
    run_cmdline()

