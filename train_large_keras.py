#!/usr/bin/env python3
#coding=utf-8
import sys
import numpy as np
import random
from datetime import datetime
from glob import glob
import cms
from dl_model import train, make_batch

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(prog='PROG')
    parser.add_argument('--task', default='mortality' , help='')
    parser.add_argument('--net', default='basic' , help='')
    parser.add_argument('--lr', default=0.0001, type=float)
    parser.add_argument('--n1', default=10000, type=int)
    parser.add_argument('--n2', default=2000, type=int)
    parser.add_argument('--epochs', default=100, type=int)
    args = parser.parse_args()

    default_name = args.net

    #eval_callback = EpochCallback(test_gs, test_files)
    val_gs = list(cms.load_gs('meta/mortality/test_gs'))
    val_gs = val_gs[:args.n2]

    black = [x[1] for x in val_gs]

    train_db = cms.LargeSampleLoader(args.task, list(glob('mortality/mortality_train/part-0*')), cms.loader, black)
    val_db = cms.SampleLoader(args.task, list(glob('mortality/mortality_test/part-0*')), cms.loader, [])

    
    train(args.net, train_db, val_db, val_gs, args.lr, args.n1, args.epochs)
    pass

