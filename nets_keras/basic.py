from model_helper import *

class Model (tf.keras.Model):

    def __init__ (self, codebook):
        super().__init__()
        self.codebook = codebook
        pass

    def call (self, params):
        demo, claims, codes, transfer = params
        with track_layers(self):
            embed = L(Embedding, self.codebook, 32)(codes) # batch x n_code x dim
            # transfer: batch x n_claims x n_code
            # embed: batch x n_code x dim
            code_feature = tf.linalg.matmul(transfer, embed)
            # code_feature: batch x n_claims x dim
            net = tf.concat([claims, code_feature], axis=2)
            net = L(Conv1D, 32, 1, strides=1, activation='relu')(net)
            _, state_h, state_c = L(LSTM, 32, return_sequences=False, return_state=True)(net)
            net1 = L(Dense, 32, activation='relu')(state_h)
            net2 = L(Dense, 16, activation='relu')(demo)
            net = tf.concat([net1, net2], axis=1)
            net = L(Dense, 32, activation='relu') (net)
            net = L(Dense, 2, activation=tf.nn.softmax)(net)
            return net

