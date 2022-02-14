import tensorflow as tf
from keras.models import Model as BaseModel
from keras.layers import Layer, Input, Flatten, Dense, Conv1D, MaxPooling1D, AveragePooling1D, Activation, GlobalAveragePooling1D, GlobalMaxPooling1D, Lambda, Dropout, Multiply, Add, LeakyReLU, LSTM, Embedding, Concatenate

class Model (BaseModel):
    def __init__ (self, codebook):
        super().__init__()
        self.embedding = Embedding(codebook, 32)
        self.concat1 = Concatenate(axis=2)
        self.conv_claim = Conv1D(32, 1, strides=1, activation='relu')
        self.lstm = LSTM(32, return_sequences=False, return_state=True)
        self.dense1 = Dense(32, activation='relu')
        self.dense2 = Dense(16, activation='relu')
        self.concat2 = Concatenate(axis=1)
        self.dense3 = Dense(32, activation='relu')
        self.dense4 = Dense(2, activation=tf.nn.softmax)
        pass

    def call (self, params):
        demo, claims, codes, transfer = params
        #embed = self.embedding(codes) # batch x n_code x dim
        # transfer: batch x n_claims x n_code
        # embed: batch x n_code x dim
        #code_feature = tf.linalg.matmul(transfer, embed)
        # code_feature: batch x n_claims x dim
        #net = self.concat1([claims, code_feature])
        net = claims
        net = self.conv_claim(net)
        _, state_h, state_c = self.lstm(net)
        net1 = self.dense1(state_h)
        net2 = self.dense2(demo)
        net = self.concat2([net1, net2])
        net = self.dense3(net)
        net = self.dense4(net)
        return net
    pass
