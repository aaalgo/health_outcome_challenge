#!/usr/bin/env python3
#coding=utf-8
import sys
import os
os.environ["CUDA_VISIBLE_DEVICES"] = "-1"
import numpy as np
import pickle
import random
from glob import glob
import cms
from dl_model import train

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(prog='PROG')
    parser.add_argument('--task', default='mortality' , help='')
    parser.add_argument('--net', default='basic' , help='')
    parser.add_argument('--split', default=0, type=int , help='')
    parser.add_argument('--epoch_size', default=None, type=int)
    parser.add_argument('--epochs', default=1500, type=int, help='')
    parser.add_argument('--lr', default=0.0001, type=float)
    parser.add_argument('--val_size', default=None, type=int)
    args = parser.parse_args()

    all_files = list(glob('/shared/data/CMS/tmp2/stage2/25th/part-*'))

    test_gs = list(cms.load_gs('test_gs.dat'))
    val_gs = list(cms.load_gs('test_gs.dat.%d' % args.split))

    black = [x[1] for x in test_gs + val_gs]
    print("black list size %d" % len(black))

    if not args.val_size is None:
        val_gs = val_gs[:args.val_size]

    db = cms.SampleLoader(args.task, all_files, cms.loader, black)
    print("Training pool: %d" % db.size())

    epoch = args.epoch_size
    if epoch is None:
        epoch = db.size()

    train(args.net, db, db, val_gs, args.lr, epoch, args.epochs, 'dl_model_%d' % args.split)
    pass

