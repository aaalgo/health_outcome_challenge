import torch
import torch.nn as nn
import torch.nn.functional as F

def masked_softmax(vec, mask):
    masked_vec = vec * mask
    max_vec = torch.max(masked_vec, dim=1, keepdim=True)[0]
    exps = torch.exp(masked_vec-max_vec)
    masked_exps = exps * mask
    masked_sums = masked_exps.sum(1, keepdim=True)
    return masked_exps/(masked_sums + 1e-6)

def linear_stack (in_channel, channels):
    modules = []
    for ch in channels:
        modules.append(nn.Linear(in_channel, ch))
        in_channel = ch
        pass
    return modules, in_channel

def mlp (in_ch, channels, keep_top=True):
    seq = []
    for ch in channels:
        seq.append(nn.Linear(in_ch, ch))
        seq.append(nn.ReLU())
        in_ch = ch
        pass
    if not keep_top:
        seq.pop()
    return nn.Sequential(*seq), in_ch

def identity (x):
    return x

class Quadratic:
    def __init__ (self, ch_in, compression = 0):
        self.compression = identity
        if compression > 0:
            self.compression = nn.Linear(ch_in, compression)
            ch_in = compression
            pass
        self.channels = ch_in * ch_in
        pass

    def forward (self, net):
        net = self.compression(net)
        net = torch.einsum('...i,...j->...ij', net, net)
        net = torch.flatten(net, 1)
        return net
    pass

class DummyAttention (nn.Module):
    def __init__ (self, in_channels):
        super().__init__()
        self.mlp, _ = mlp(in_channels, [32, 1], False)
        pass

    def forward (self, net, mask):
        # net: batch x claims x channels
        value = net
        net = self.mlp(net)
        net = torch.flatten(net, 1)
        # batch x claims
        net = masked_softmax(net, mask)
        net = torch.unsqueeze(net, dim=1)
        # net = batch x 1 x claims
        # value = batch x claims x channels
        net = torch.matmul(net, value)
        # batch x 1 x channels
        net = torch.flatten(net, 1)
        # batch x channels
        return net 
    pass

class Model (nn.Module):

    def __init__ (self, codebook, demo_dim, claims_dim):
        print("DEMO_DIM", demo_dim)
        print("CLAIMS_DIM", claims_dim)
        super().__init__()
        embed_dim = 128
        self.embed = nn.Embedding(codebook, embed_dim)
        self.claims_mlp1, ch_in = mlp(claims_dim + embed_dim, [32])
        lstm_dim = 128
        self.lstm = nn.LSTM(input_size=ch_in, hidden_size=lstm_dim)
        HEADS = 2
        self.attentions = nn.ModuleList([DummyAttention(lstm_dim) for _ in range(HEADS)])
        self.claims_mlp2, ch1 = mlp(lstm_dim * HEADS, [64])
        self.demo_mlp, ch2 = mlp(demo_dim, [8])
        self.final_mlp, _ = mlp(ch1+ch2, [32, 2], False)
        pass

    def forward (self, params):
        demo, claims_mask, claims, codes, transfer = params

        embed = self.embed(codes) # batch x n_code x dim
        embed = torch.flatten(embed, 2)
        # transfer: batch x n_claims x n_code
        # embed: batch x n_code x dim
        code_feature = torch.matmul(transfer, embed)
        # code_feature: batch x n_claims x dim
        net = torch.cat([claims, code_feature], dim=2)
        net = self.claims_mlp1(net)
        # batch, seqlen, dim
        net = torch.transpose(net, 0,1)
        # seqlen, batch, dim
        #net = torch.flip(net, [1])
        net, _ = self.lstm(net)
        # net: seq_len, batch, dim 
        net = torch.transpose(net, 0, 1)
        att = [x(net, claims_mask) for x in self.attentions]
        net = torch.cat(att, dim=1)
        net1 = self.claims_mlp2(net)
        net2 = self.demo_mlp(demo)
        net = torch.cat([net1, net2], dim=1)
        net = self.final_mlp(net)
        net = F.log_softmax(net, dim=1)
        return net

