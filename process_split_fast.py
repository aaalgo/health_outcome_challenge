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
import sklearn.metrics as metrics
import click
sys.path.append(os.path.abspath('/home/wdong/shared/cms2'))
from cms import *
import preprocess

def load_gs (gs_path):
    # train_gs.dat*
    # test_gs
    with open(gs_path, 'r') as f:
        for l in f:
            death365, pid, observe, cutoff, is_dead = l.strip().split('\t')
            yield int(death365), int(pid), int(observe), int(cutoff), int(is_dead)
        pass
    pass

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

    def __init__ (self, gs_paths, data_files):
        # 把gs_paths里面用到的所有example都先加载到
        # 字典self.cache里
        # self.cache[pid]就是特征
        need_data=[]
        for path in gs_paths:
            for _, pid, _, cutoff, _ in load_gs(path):
                need_data.append([pid, cutoff])
                pass
            pass

        core_loader = CoreLoader(loader)
        self.cache = core_loader.bulk_load_aggr_features(data_files, need_data)
        logging.info("%d items loaded." % len(self.cache))
        pass

    def load (self, gs_path, keep):
        # keep: 如果数据不存在是否返回0/NaN
        gs=[]
        dat=[]
        dim = None
        for death365, pid, observe, cutoff, is_dead in load_gs(gs_path):
            X = self.cache.get((pid, cutoff), None)
            if X is None:
                assert False    # 实际不应该出现这个情况
                if not keep:
                    continue
            elif dim is None:
                dim = X.shape
            else:
                assert dim == X.shape
                pass
            gs.append(death365)
            dat.append(X)
            pass
        na = np.zeros(dim)
        na_cnt = 0 
        for i, x in enumerate(dat):
            if x is None:
                dat[i] = na
                na_cnt += 1
                pass
            pass
        gs = np.array(gs)
        dat = np.vstack(dat)
        print(dat.shape, 'na', na_cnt)
        return gs, dat
    pass

def auc (gs, values):
    return metrics.roc_auc_score(gs, values)
    #fpr, tpr, thresholds = metrics.roc_curve(gs, values, pos_label=1)
    #return metrics.auc(fpr, tpr)

@click.command(context_settings=dict(ignore_unknown_options=True,))
@click.option('--split', type=int, default=None)
@click.option('--nsplits', type=int, default=5)
@click.option('--test/--no-test', default=True)
def run (split, nsplits, test):
    # 命令行运行 .../process_split.py --split 3 处理split 3
    # 或者 .../process_split.py 处理所有的splits

    if not os.path.exists('data2') or not os.path.exists('data2/death_train'):
        print("Do this link on cluster behind barton and try again:")
        print("ln -s /shared/data/CMS/tmp2/stage2 data2")
        return

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
    data_files=list(glob.glob('data2/death_train/*')) + \
               list(glob.glob('data2/fifth_train/*'))

    if test:
        data_files += list(glob.glob('data2/death_test/*')) + \
                      list(glob.glob('data2/fifth_test/*'))
        gs_paths.append('test_gs.dat')
        for split in splits:
            gs_paths.append('test_gs.dat.%d' % split)
            pass
        pass

    cache = FeatureCache(gs_paths, data_files)

    for split in splits:

        logging.info("processing split %d" % split)
        gs, dat = cache.load('train_gs.dat.%d' % split, False) # False表示找不到的样本直接扔掉
        logging.info("training")

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
        filename = ('finalized_model.sav.%d' % split)
        pickle.dump(gbm, open(filename, 'wb'))

        if not test:
            continue

        logging.info("predicting split tmp")
        gs, dat = cache.load('test_gs.dat.%d' % split, True) # True 表示找不到样本需要填0
        value=gbm.predict(dat)
        np.savetxt(('prediction.dat.%d' % split),value)

        print("AUC:", auc(gs, value))
        del gs
        del dat
        del value

        logging.info("predicting test")
        gs, dat = cache.load('test_gs.dat', True)
        value=gbm.predict(dat)
        np.savetxt(('prediction.dat.final.%d' % split),value)

        print("AUC:", auc(gs, value))
        del gs
        del dat
        del value

        del gbm
        pass
    logging.info('prediction finished.')

    pass

if __name__ == '__main__':
    run()
