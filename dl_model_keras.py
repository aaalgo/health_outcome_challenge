#!/usr/bin/env python3
#coding=utf-8
import os
os.environ["CUDA_VISIBLE_DEVICES"] = "-1"
import numpy as np
from datetime import datetime
from glob import glob
import tensorflow as tf
import keras
from keras.optimizers import Adam, SGD
from keras.models import Model
from keras.callbacks import Callback
from keras.layers import Input, Lambda, Reshape
import cms

tf.compat.v1.logging.set_verbosity(tf.compat.v1.logging.INFO)

def make_batch (sample_from_cpp, gs_label=None):
    # sample_from_cpp is one sample from C++ library
    # convert to a minibatch of 1 sample
    # gs_label is a label from gs_train/gs_test
    label, demo, claims, codes, transfer, _ = sample_from_cpp
    # label: 0 or 1
    # demo: a vector    -- may contain NaN
    # claim: a matrix, each row is a feature vector of claim -- may contain NaN
    # codes: a vector, each entry is a code
    # transfer: transfer matrix that maps codes to claims
    assert claims.shape[0] > 0
    if not gs_label is None:
        label = gs_label    # make sure C++ label matches gs label
        pass
    sp_label = np.zeros((1,2))
    sp_label[0, label] = 1

    np.nan_to_num(demo, False)
    np.nan_to_num(claims, False)

    demo = demo[np.newaxis, :]
    claims = claims[np.newaxis, :, :]
    codes = codes[np.newaxis, :]
    transfer = transfer[np.newaxis, :, :]
    return [demo, claims, codes, transfer], sp_label

def build_model (model, lr, codebook, demo_dimensions, claim_dimensions):
    print("CODEBOOK:", codebook)
    print("DEMO DIM:", demo_dimensions)
    print("CLAIM DIM:", claim_dimensions)

    demo = Input(shape=[demo_dimensions])
    # one example has a variable number of claims
    claims = Input(shape=[None, claim_dimensions])
    codes = Input(shape=[None])
    transfer = Input(shape=[None, None])

    if 'basic' in model:
        from nets import basic
        probs, lookup = getattr(basic, 'create_%s_model' % model)(demo, claims, codes, transfer, codebook)
        pass
    else:
        assert False, "model not found"
        pass

    model = Model(inputs=[demo, claims, codes, transfer], outputs=[probs])


    opt = Adam(lr=lr)
    model.compile(optimizer=opt,
            loss='binary_crossentropy',
            metrics=[tf.keras.metrics.AUC()])
    return model, lookup

def train (net_name, train_db, val_db, val_gs, learning_rate, epoch_size, epochs, output_dir = 'dl_models'):
    val_data = []
    for gs_label, pid, observe, cutoff in val_gs:
        val_data.append(make_batch(val_db.get(pid, cutoff), gs_label))
        pass
    print("Test size: %d" % len(val_data))

    def train_generator ():
        while True:
            yield make_batch(train_db.next())
        pass

    def val_generator ():
        while True:
            for sample in val_data:
                yield sample

    model, _ = build_model(net_name, learning_rate, train_db.codebook_size, train_db.demo_dimensions, train_db.claim_dimensions)

    outdir = '%s/%s/%s' % (output_dir, net_name, datetime.now().strftime("%Y%m%d-%H%M%S"))
    os.makedirs(outdir, exist_ok=True)

    callbacks = []
    #callbacks.append(eval_callback) 
    callbacks.append(keras.callbacks.ModelCheckpoint(os.path.join(outdir, 'model.{epoch:04d}.h5'), save_weights_only=False))
    #callbacks.append(keras.callbacks.TensorBoard(log_dir=os.path.join(outdir, 'logs')))

    model.fit_generator(generator=train_generator(),
                               steps_per_epoch=epoch_size,
                               epochs=epochs,
                               validation_data=val_generator(),
                               validation_steps = len(val_data),
                               callbacks=callbacks
                              )
    pass

