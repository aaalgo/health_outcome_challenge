#!/usr/bin/perl
import os
import sys
import numpy as np
import math
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'build/lib.linux-x86_64-' + sys.version[:3])))
sys.path.append(os.path.join('/home/wdong/shared/cms2', 'build/lib.linux-x86_64-' + sys.version[:3]))
from cms_core import grad_feature

def preprocess(data):
#    print(data.shape)
    try:
        imax=data.shape[0]
        jmax=data.shape[1]
    except:
        data=data.reshape((1,data.shape[0]))
        imax=data.shape[0]
        jmax=data.shape[1]

    processed_data=np.zeros((imax,jmax*2))
    i=0
    while (i<imax):
        j=0
        while (j<jmax):
            if (math.isnan(data[i][j])):
                processed_data[i][j*2]=-3000
            else:
                processed_data[i][j*2]=data[i][j]
            if (math.isnan(data[i][j])):
                processed_data[i][j*2+1]=0
            else:
                processed_data[i][j*2+1]=1
            j=j+1
        i=i+1
    return processed_data
	
def faster_preprocess (data):
    if len(data.shape) == 1:
        data=data[np.newaxis, :, np.newaxis]
    else:
        data=data[:, :, np.newaxis]
        pass

    rows = data.shape[0]

    isnan = np.isnan(data)

    flags = np.ones_like(data)

    flags[isnan] = 0
    data[isnan] = -3000

    data = np.concatenate([data, flags], axis=2).reshape((rows, -1))
    return data

def feature(whole_train):
    try:
        j=whole_train.shape[1]
    except:
        whole_train=whole_train.reshape((1,whole_train.shape[0]))
    
    i=whole_train.shape[0]-1
    
    while(i<whole_train.shape[0]):
        length=8*len(whole_train[0,:].flatten())
        x_mean=np.nanmean(whole_train[:,:],axis=0)
        x_std=np.nanstd(whole_train[:,:],axis=0)+0.01
        x_norm = np.nan_to_num((whole_train[i,:] - x_mean) / x_std)
        whole_train_normed=(whole_train[:,:]-x_mean)/x_std
        matrix=np.ones((length*2+len(x_mean)*4))*(-5000)
        if (i>=7): 
            matrix[0:length]=whole_train[i-7:i+1,:].flatten()
        else:
            matrix[(length-(i+1)*len(whole_train[0].flatten())):length]=whole_train[0:i+1,:].flatten()

        if (i>=7): 
            matrix[length:length*2]=whole_train_normed[i-7:i+1,:].flatten()
        else:
            matrix[(length*2-(i+1)*len(whole_train[0].flatten())):(length*2)]=whole_train_normed[0:i+1,:].flatten()
        matrix[(length*2):(length*2+len(x_mean))]=x_std
        matrix[(length*2+len(x_mean)):(length*2+len(x_mean)*2)]=np.sum(whole_train[:,:][whole_train[:,:]==-3000],axis=0)/(-3000.0)/float(whole_train.shape[0])
        baseline=[]
        jjj=0
        while (jjj<whole_train.shape[1]):
            iii=0
            val=np.nan
            while (iii<whole_train.shape[0]):
                if (whole_train[iii][jjj]==-3000):
                    pass
                else:
                    if (math.isnan(val)):
                        val=whole_train[iii][jjj]
                        timediff=whole_train.shape[0]-iii
                iii=iii+1
            if (math.isnan(val)):
                baseline.append(np.nan)
                baseline.append(np.nan)
            else:
                baseline.append(val)
                baseline.append(timediff)
            jjj=jjj+1

        matrix[(length*2+len(x_mean)*2):(length*2+len(x_mean)*4)]=np.asarray(baseline)
        i=i+1
    return matrix


def faster_feature (whole_train):
    assert len(whole_train.shape) == 2

    i=whole_train.shape[0]-1
    input_dim = whole_train.shape[1]
    
    while(i<whole_train.shape[0]):
        length=8*input_dim

        x_mean=np.nanmean(whole_train,axis=0)
        assert len(x_mean) == input_dim
        x_std=np.nanstd(whole_train,axis=0)+0.01

        x_norm = np.nan_to_num((whole_train[i,:] - x_mean) / x_std)

        whole_train_normed=(whole_train - x_mean[np.newaxis, :])/x_std[np.newaxis, :]

        matrix=np.ones((length*2+len(x_mean)*4))*(-5000)
        if (i>=7): 
            matrix[0:length]=whole_train[i-7:i+1,:].flatten()
            matrix[length:length*2]=whole_train_normed[i-7:i+1,:].flatten()
        else:
            matrix[(length-(i+1)*input_dim):length]=whole_train[0:i+1,:].flatten()
            matrix[(length*2-(i+1)*len(whole_train[0].flatten())):(length*2)]=whole_train_normed[0:i+1,:].flatten()
            pass

        matrix[(length*2):(length*2+len(x_mean))]=x_std
        matrix[(length*2+len(x_mean)):(length*2+len(x_mean)*2)]=np.sum(whole_train[:,:]==-3000,axis=0)/float(whole_train.shape[0])

        matrix[(length*2+len(x_mean)*2):(length*2+len(x_mean)*4)]= grad_feature(whole_train, -3000)
        i=i+1
    return matrix
