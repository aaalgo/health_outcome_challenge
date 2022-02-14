from model_helper import *

class Model (tf.keras.Model):

    def __init__ (self, codebook):
        super().__init__()
        self.codebook = codebook
        pass

    def call (self, params):
        demo, claims, codes, transfer = params
        assert claims.shape[1] > 0
        with track_layers(self):
            embed = L(Embedding, self.codebook, 64)(codes) # batch x n_code x dim
            # transfer: batch x n_claims x n_code
            # embed: batch x n_code x dim
            codes = tf.linalg.matmul(transfer, embed)
            codes = L(Conv1D, 64, 1, strides=1, activation='relu')(codes)
            # code_feature: batch x n_claims x dim
            net = codes
            net = L(Conv1D, 64, 1, strides=1, activation='relu')(net)
            net1, net2, _ = L(LSTM, 64, return_sequences=True, return_state=True)(net)
            net1 = L(GlobalAveragePooling1D)(net1)
            net1 = L(Dense, 64, activation='relu')(net1)
            net2 = L(Dense, 64, activation='relu')(net2)
            #net3 = L(Dense, 64, activation='relu')(demo)
            #net3 = L(Dense, 32, activation='relu')(net3)
            net = tf.concat([net1, net2], axis=1)
            net = L(Dense, 32, activation='relu') (net)
            net = L(Dense, 2, activation=tf.nn.softmax)(net)
            return net

