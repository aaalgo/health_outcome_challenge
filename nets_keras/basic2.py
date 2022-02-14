from model_helper import *

class Model (tf.keras.Model):

    def __init__ (self, codebook):
        super().__init__()
        self.codebook = codebook
        pass

    def call (self, params):
        demo, claims, codes, transfer = params
        with track_layers(self):
            claims = L(Conv1D, 64, 1, strides=1, activation='relu')(claims)
            embed = L(Embedding, self.codebook, 64)(codes) # batch x n_code x dim
            # transfer: batch x n_claims x n_code
            # embed: batch x n_code x dim
            codes = tf.linalg.matmul(transfer, embed)
            codes = L(Conv1D, 64, 1, strides=1, activation='relu')(codes)
            # code_feature: batch x n_claims x dim
            net = tf.concat([claims, codes], axis=2)
            net = L(Conv1D, 64, 1, strides=1, activation='relu')(net)
            _, net1, state_c = L(LSTM, 64, return_sequences=False, return_state=True)(net)
            net1 = L(Dense, 64, activation='relu')(net1)
            net2 = L(Dense, 64, activation='relu')(demo)
            net2 = L(Dense, 32, activation='relu')(net2)
            net = tf.concat([net1, net2], axis=1)
            net = L(Dense, 32, activation='relu') (net)
            net = L(Dense, 2, activation=tf.nn.softmax)(net)
            return net

