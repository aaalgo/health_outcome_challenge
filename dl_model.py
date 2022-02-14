#!/usr/bin/env python3
#coding=utf-8
import sys
import os
#os.environ["CUDA_VISIBLE_DEVICES"] = "-1"
import numpy as np
from datetime import datetime
import importlib
from glob import glob
from tqdm import tqdm
import sklearn.metrics as metrics
import torch
import cms


def stream_val_batches (db, gs, b1, b2, batch):
    for i in range(0, len(gs), batch):
        labels = []
        demos = []
        claims_mask = []
        claims = []
        codes = []
        transfer = []

        for gs_label, pid, observe, cutoff in gs[i:(i+batch)]:
            a,b,c,d,e,f,_ = db.get(pid, cutoff, b1, b2)
            labels.append(gs_label)
            demos.append(b)
            claims_mask.append(c)
            claims.append(d)
            codes.append(e)
            transfer.append(f)
            pass

        X = [np.stack(demos),
             np.stack(claims_mask),
             np.stack(claims),
             np.stack(codes).astype(np.int64),
             np.stack(transfer)]
        #np.nan_to_num(demos, False)
        #np.nan_to_num(claims, False)
        np.nan_to_num(X[0], False)
        np.nan_to_num(X[2], False)
        Y = np.array(labels, dtype=np.int)
        yield X, Y
    pass

def make_train_batch (db, device):
    # sample_from_cpp is one sample from C++ library
    # convert to a minibatch of 1 sample
    # gs_label is a label from gs_train/gs_test
    labels, demos, claims_mask, claims, codes, transfer, _ = db.next()
    labels = labels.astype(np.int64)
    codes = codes.astype(np.int64)
    if False:
        print("labels", labels.shape)
        print("demos", demos.shape)
        print("claims_mask", claims_mask.shape)
        print("claims", claims.shape)
        print("codes", codes.shape)
        print("transfer", transfer.shape)
        sys.exit(0)
    # label: 0 or 1
    # demo: a vector    -- may contain NaN
    # claim: a matrix, each row is a feature vector of claim -- may contain NaN
    # codes: a vector, each entry is a code
    # transfer: transfer matrix that maps codes to claims
    np.nan_to_num(demos, False)
    np.nan_to_num(claims, False)

    return [torch.tensor(demos, device=device),
            torch.tensor(claims_mask, device=device),
            torch.tensor(claims, device=device),
            torch.tensor(codes, device=device),
            torch.tensor(transfer, device=device)], torch.tensor(labels, device=device)


class Metrics:
    def __init__ (self, prefix):
        self.prefix = prefix
        #self.loss = tf.keras.metrics.Mean()
        #self.auc = tf.keras.metrics.AUC()
        self.losses = []
        self.labels = []
        self.predicts = []
        pass

    def reset (self):
        #self.loss.reset_states()
        #self.auc.reset_states()
        self.losses = []
        self.labels = []
        self.predicts = []
        pass

    def update (self, loss, label, predict):
        self.losses.append(loss)
        self.labels.extend(label)
        self.predicts.extend(list(predict[:, 1]))
        pass

    def report (self):
        #loss = self.loss.result().numpy()
        #self.auc.update_state(self.labels, self.predicts)
        loss = np.mean(self.losses)
        auc = metrics.roc_auc_score(self.labels, self.predicts)
        print(self.prefix, "loss: %.4f" % loss, "auc: %.4f" % auc)
        pass
    pass

def train (net_name, train_db, val_db, val_gs, learning_rate, batch, epoch_size, epochs, output_dir, b1, b2):
    cpu = torch.device("cpu") # if cuda_condition else "cpu")
    device = torch.device("cuda:0") # if cuda_condition else "cpu")

    
    #demo = Input(shape=[train_db.demo_dimensions])
    # one example has a variable number of claims
    #claims = Input(shape=[None, train_db.claim_dimensions])
    #codes = Input(shape=[None])
    #transfer = Input(shape=[None, None])
    X, Y = make_train_batch(train_db, device)
    module = importlib.import_module('.'.join(['nets', net_name]))
    model = getattr(module, 'Model')(train_db.codebook_size, X[0].shape[-1], X[2].shape[-1]).to(device)

    optimizer = torch.optim.Adam(model.parameters(), lr=learning_rate, weight_decay=1e-5)

    train_metrics = Metrics("train")
    val_metrics = Metrics("val")
    outdir = '%s/%s/%s' % (output_dir, net_name, datetime.now().strftime("%Y%m%d-%H%M%S"))
    os.makedirs(outdir, exist_ok=True)
    #with ParallelGenerator(prefetch(), max_lookahead=1000) as stream:

    for epoch in range(epochs):
        print("epoch %d:" % epoch)

        train_metrics.reset()
        for _ in tqdm(list(range(epoch_size))):
            X, Y = make_train_batch(train_db, device)
            pred = model(X)
            loss = torch.nn.functional.nll_loss(pred, Y)

            optimizer.zero_grad()
            loss.backward()
            optimizer.step()
            train_metrics.update(loss.item(), list(Y.detach().cpu().numpy()), pred.detach().cpu().numpy())
            pass
        train_metrics.report()

        val_metrics.reset()
        for X, Y in tqdm(stream_val_batches(val_db, val_gs, b1, b2, batch), total=(len(val_gs)+batch-1)//batch):
            pred = model([torch.tensor(x, device=device) for x in X])
            loss  = torch.nn.functional.nll_loss(pred, torch.tensor(Y, device=device))
            val_metrics.update(loss.item(), list(Y), pred.detach().cpu().numpy())
            pass
        val_metrics.report()
        pass
    pass

