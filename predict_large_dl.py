#!/usr/bin/env python3
#coding=utf-8
import sys
import os
os.environ["CUDA_VISIBLE_DEVICES"] = "-1"
import numpy as np
import pickle
import random
from datetime import datetime
from glob import glob
from tqdm import tqdm
import sklearn.metrics as metrics
import tensorflow as tf
import keras
import cms
from dl_model import build_model, make_batch

tf.compat.v1.logging.set_verbosity(tf.compat.v1.logging.INFO)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(prog='PROG')
    parser.add_argument('--net', default='basic' , help='')
    parser.add_argument('--task', default='mortality' , help='')
    parser.add_argument('--model', default=None, help='')
    parser.add_argument('--n2', default=1000, type=int)
    args = parser.parse_args()


    for gpu in  tf.config.experimental.list_physical_devices('GPU'):
        tf.config.experimental.set_memory_growth(gpu, True)

    #eval_callback = EpochCallback(test_gs, test_files)
    test_gs = list(cms.load_gs('meta/mortality/test_gs'))

    val_data = []
    #test_gs = list(cms.load_gs(test_gs))
    #random.shuffle(test_gs)
    #test_gs = test_gs[:1000]
    test_db = cms.SampleLoader(args.task, list(glob('mortality/mortality_test/part-0*')), cms.loader, [])
    print("Training pool: %d" % test_db.size())
    out =[]

    model = build_model(args.net, test_db.codebook_size)
    model.load_weights(args.model)
    loss_object = tf.keras.losses.BinaryCrossentropy()

    eval_labels = []
    eval_preds = []
    for gs_label, pid, observe, cutoff in tqdm(test_gs[:args.n2]):
        X, Y = make_batch(test_db.get(pid, cutoff), gs_label)
        demo, claims, codes, mapping = X

        demo_v = tf.Variable(demo)
        claims_v = tf.Variable(claims)
        codes_v = tf.Variable(codes)
        X_v = [demo_v, claims_v, codes_v, mapping]

        with tf.GradientTape() as tape:
            tape.watch(demo_v)
            tape.watch(claims_v)
            tape.watch(codes_v)
            pred = model(X_v)
            loss = loss_object(Y, pred)
            pass

        grads = tape.gradient(loss, [demo_v, claims_v, codes_v])
        d_demo, d_claims, d_codes = grads
        grads = [d_demo.numpy(), d_claims.numpy(), None]

        l = float(loss.numpy())

        p = pred.numpy()[0, 1]
        out.append([pid, l, gs_label, p, cutoff, X, grads])

        eval_labels.append(gs_label)
        eval_preds.append(p)
        pass

    auc = metrics.roc_auc_score(eval_labels, eval_preds)
    print("AUC:", auc)


    out = sorted(out, key=lambda x: -x[1])
    top = out[:100]
    bottom = out[-100:]

    out = top + bottom

    lookup = {}
    for arr in out:
        pid = arr[0]
        if pid in lookup:
            print(pid)
        assert not pid in lookup
        lookup[arr[0]] = arr
        pass

    for path in tqdm(glob('mortality/mortality_test/part-*')):
        with open(path, 'rb') as f:
            for l in f:
                pid = int(l[:30].decode('ascii').split('\t')[0])
                arr = lookup.get(pid, None)
                if arr is None:
                    continue
                arr.append(l)
                pass
            pass
        pass

    with open('eval.pkl', 'wb') as f:
        pickle.dump(out, f)
    pass

