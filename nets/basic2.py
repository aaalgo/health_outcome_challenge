import torch
import torch.nn as nn
import torch.nn.functional as F
from model_helper import *

class DummyAttention (nn.Module):
    def __init__ (self, in_channels):
        super().__init__()
        self.linear1 = nn.Linear(in_channels, 32)
        self.linear2 = nn.Linear(32, 1)
        pass

    def forward (self, net):
        # net: batch x claims x channels
        value = net
        net = self.linear1(net)
        net = F.relu(net)
        net = self.linear2(net)
        net = torch.flatten(net, 1)
        # batch x claims
        net = F.softmax(net, dim=1)
        net = torch.unsqueeze(net, dim=1)
        # batch x 1 x claims

        net = torch.matmul(net, value)
        # batch x 1 x channels
        net = torch.flatten(net, 1)
        return net
    pass

class Model (nn.Module):

    def __init__ (self, codebook):
        super().__init__()
        self.codebook = codebook
        pass

    def forward (self, params):
        demo, claims_mask, claims, codes, transfer = params
        with track_layers(self):
            ch = 32
            #print(codes.shape)
            embed = L(nn.Embedding, self.codebook, ch)(codes) # batch x n_code x dim
            embed = torch.flatten(embed, 2)
            # transfer: batch x n_claims x n_code
            # embed: batch x n_code x dim
            code_feature = torch.matmul(transfer, embed)
            # code_feature: batch x n_claims x dim
            net = torch.cat([claims, code_feature], dim=2)

            in_ch = net.shape[-1]

            HEADS = 8
            att = []
            for i in range(HEADS):
                att.append(L(DummyAttention, in_ch)(net))
                pass

            # [batch x channels]

            net = torch.cat(att, dim=1)

            ch = 32

            # batch x channels x time
            net = L(nn.Linear, in_ch * HEADS, ch)(net)
            net1 = F.relu(net)
            ch1 = ch
            # batch, seqlen, dim
            in_ch, ch2 = demo.shape[-1], 16
            net2 = L(nn.Linear, in_ch, ch2)(demo)
            net2 = F.relu(net2)
            net = torch.cat([net1, net2], dim=1)
            in_ch, ch = ch1 + ch2, 32
            net = L(nn.Linear, in_ch, ch) (net)
            net = F.relu(net)
            in_ch, ch = ch, 2
            net = L(nn.Linear, in_ch, ch)(net)
            net = F.log_softmax(net, dim=1)
            return net

