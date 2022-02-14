#!/usr/bin/env python3
#coding=utf-8
import os
import sys
import numpy as np
import random
import pickle
from datetime import datetime
from glob import glob
from tqdm import tqdm
import sklearn.metrics as metrics
import lightgbm
import cms

os.nice(20)

def load_train_cases (paths, loader, gs_path, n = -1):

    gs = list(cms.load_gs(gs_path))
    if n > 0 and n < len(gs):
        gs = gs[:n]
        pass

    cases = loader.bulk_load_aggr_features(paths, [[pid, cutoff] for _, pid, _, cutoff in gs])

    X = []
    Y = []
    pids = []
    for label, pid, _, cutoff in gs:
        X.append(cases[(pid, cutoff)])
        Y.append(label)
        pids.append((pid, cutoff))
        pass
    print("%d cases loaded." % len(Y))
    return np.vstack(X), np.array(Y), pids


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(prog='PROG')
    parser.add_argument('--task', default='mortality' , help='')
    parser.add_argument('--root', default='mortality_small' , help='')
    #parser.add_argument('--n1', default=5000, type=int)
    #parser.add_argument('--n2', default=1000, type=int)
    args = parser.parse_args()

    assert args.task == 'mortality'

    train_paths = list(glob('%s/train/part-0*' % args.root))
    test_paths = list(glob('%s/test/part-0*' % args.root))

    loader = cms.CoreLoader(cms.loader)
    train_x, train_y, _ = load_train_cases(train_paths, loader, '%s/train_gs' % args.root)
    test_x, test_y, test_pids = load_train_cases(test_paths, loader, '%s/test_gs' % args.root)

    #eval_callback = EpochCallback(test_gs, test_files)
    print(train_x.shape, train_y.shape)
    print(test_x.shape, test_y.shape)

    params = {
        'boosting_type': 'gbdt',
        #'objective': 'regression',
        'num_leaves': 150,
        'learning_rate': 0.05,
        'verbose': 0,
        'n_estimators': 400,
        'reg_alpha': 2.0,
    }

    model = lightgbm.LGBMRegressor(**params)
    model.fit(train_x, train_y)

    with open('lgbm_model.pkl', 'wb') as f:
        pickle.dump(model, f)
        pass
        


    pred = model.predict(test_x)

    auc = metrics.roc_auc_score(test_y, pred)
    print('auc:', auc)

    order = [(v,i) for i, v in enumerate(pred)]
    order.sort()

    ranks = [None for _ in range(len(order))]
    for i, (_, j) in enumerate(order):
        ranks[j] = i
        pass

    N = len(order)
    
    # rank 0: 按预测分数排序

    evals = []
    for pid, label, pred, rank in zip(test_pids, test_y, pred, ranks):

        if label == 0:
            # rank应该低才好
            # 差的排前面，所以rank越大分数应该越小
            orank = N - rank
        elif label == 1:
            orank = rank
            pass

        evals.append((pid, label, pred, rank, orank))
        pass

    evals.sort(key=lambda x: x[-1])
    with open('eval.html', 'w') as f:
        f.write('<html><body><table border="1">\n')
        f.write('<tr><th>pid</th><th>cutoff</th><th>label</th><th>pred</th><th>rank</th><th>order</th></tr>\n')
        for (pid, cutoff), label, pred, rank, orank in evals[:2000]:
            f.write(f'<tr><td>{pid}</td><td>{cutoff}</td><td>{label}</td><td>{pred:.3f}</td><td>{rank}</td><td>{orank}</td></tr>\n')
        f.write('</table></body></html>\n')

    pass

