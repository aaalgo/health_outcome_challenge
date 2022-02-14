#!/usr/bin/env python3
import tensorflow as tf
from keras.layers import Input, Flatten, Dense, Conv1D, MaxPooling1D, AveragePooling1D, Activation, GlobalAveragePooling1D, GlobalMaxPooling1D, LSTM, Lambda, Dropout, concatenate, Multiply, Add, LeakyReLU, BatchNormalization, LSTM, Embedding
from keras import backend as K

x = Input(shape=(32,))
y = Dense(16, activation='softmax')(x)
print(y)
print(type(y))
