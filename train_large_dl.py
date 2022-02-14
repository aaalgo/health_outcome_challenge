#!/usr/bin/env python3
#coding=utf-8
import sys
import numpy as np
import random
from datetime import datetime
from glob import glob
import cms
from dl_model import train

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(prog='PROG')
    parser.add_argument('--task', default='mortality' , help='')
    parser.add_argument('--net', default='basic' , help='')
    parser.add_argument('--lr', default=0.01, type=float)
    parser.add_argument('--batch', default=1024, type=int)    # 44w
    parser.add_argument('--n1', default=500, type=int)    # 44w
    parser.add_argument('--val', default=None, type=int)    # 18w
    parser.add_argument('--epochs', default=100, type=int)
    args = parser.parse_args()

    default_name = args.net

    #eval_callback = EpochCallback(test_gs, test_files)
    val_gs = list(cms.load_gs('mortality/test_gs'))
    if not args.val is None:
        val_gs = val_gs[:args.val]

    black = [x[1] for x in val_gs]

    b1 = 128
    b2 = 384
    train_db = cms.LargeSampleLoader(args.task, list(glob('mortality/train_prep/part-0*')), cms.loader, black, args.batch, b1, b2)
    val_db = cms.SampleLoader(args.task, list(glob('mortality/test/part-0*')), cms.loader, [])
    
    train(args.net, train_db, val_db, val_gs, args.lr, args.batch, args.n1, args.epochs, 'dl_models', b1, b2)
    pass

